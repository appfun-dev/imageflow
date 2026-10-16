package controller

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// ImageStreamMapping controller (ticket T4, push registration path).
//
// Lifecycle: CI creates an ImageStreamMapping task -> this controller records
// startedAt, performs the idempotent catalog registration (see
// registration.go: ImageArtifact dedup + bounded stream status association +
// Image Change surface) and writes the terminal Complete condition
// (Succeeded/Failed) with completedAt through the status subresource. TTL GC
// (task_gc_controller.go) removes terminal tasks later. Every state transition
// is audited with kube Events.
//
// Idempotence contract (ADR-004, user story 16): re-reconciling a terminal
// task has zero effects, and re-registering the current (stream, tag, digest)
// is a no-op success without a duplicate Image Change. Change detection is
// keyed exclusively off persisted state (ImageStream.status.tags[].items), so
// retries and controller restarts converge without in-memory events.
//
// created-by annotation: the T3 mutating webhook injects
// imageflow.appfun.dev/created-by on admission. This controller only ever
// writes the status subresource of task objects — the main endpoint (and with
// it metadata/annotations) is never touched, so the annotation is preserved.

// created-by annotation contract: the T3 mutating webhook injects
// imageflow.appfun.dev/created-by on admission. This controller only ever
// writes the status subresource of task objects — the main endpoint (and with
// it metadata/annotations) is never touched, so the annotation is preserved
// (unit-tested in mapping_controller_test.go).

// Event recorder component names (Event.source.component).
const (
	recorderNameMapping = "imageflow-mapping"
	recorderNameTaskGC  = "imageflow-task-gc"
)

// ImageFlow kind names used for scheme-recognition guards.
const (
	kindImageStreamMapping = "ImageStreamMapping"
	kindImageStreamImport  = "ImageStreamImport"
)

// Failure taxonomy for the Complete condition message prefix. The condition
// reason vocabulary is frozen to Succeeded/Failed (T2); the diagnosable code
// lives in the message prefix and in the MappingFailed Event.
const (
	failCodeInvalidImageReference = "InvalidImageReference"
	failCodeStreamNotFound        = "StreamNotFound"
)

func init() {
	Registrars = append(Registrars, &mappingRegistrar{})
}

// MappingControllerNames carries the controller names T4 builds. controller-runtime
// validates controller names against a process-global set, so a second manager
// built in the same process (integration suites) must use distinct names.
type MappingControllerNames struct {
	// Mapping is the ImageStreamMapping reconciler controller name.
	Mapping string
	// GCMapping is the TTL GC controller name for ImageStreamMapping tasks.
	GCMapping string
	// GCImport is the TTL GC controller name for ImageStreamImport tasks.
	GCImport string
}

// DefaultMappingControllerNames returns the production controller names.
func DefaultMappingControllerNames() MappingControllerNames {
	return MappingControllerNames{
		Mapping:   "mapping",
		GCMapping: "task-gc-mapping",
		GCImport:  "task-gc-import",
	}
}

// SetupMappingControllers registers all T4 controllers with mgr: the
// ImageStreamMapping reconciler plus TTL GC for both task kinds (GC is uniform
// across Mapping and Import, ticket T4). Registrars use it for production
// wiring; integration suites call it directly with test-unique names to stay
// isolated from other registration units.
func SetupMappingControllers(mgr ctrl.Manager, opts Options, names MappingControllerNames) error {
	if err := setupMappingController(mgr, opts, names.Mapping); err != nil {
		return err
	}
	return SetupTaskGCControllers(mgr, opts, names)
}

type mappingRegistrar struct{}

func (mappingRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	return setupMappingController(mgr, opts, DefaultMappingControllerNames().Mapping)
}

func setupMappingController(mgr ctrl.Manager, opts Options, name string) error {
	if missing := missingImageFlowKinds(mgr, kindImageStreamMapping); len(missing) > 0 {
		logSkippedRegistration(name, missing)
		return nil
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&imageflowv1alpha1.ImageStreamMapping{}).
		Named(name).
		Complete(NewMappingReconciler(
			mgr.GetClient(),
			mgr.GetEventRecorderFor(recorderNameMapping),
			opts.Clock,
			opts.Config,
			sharedMappingMetrics(),
		))
}

// missingImageFlowKinds lists the ImageFlow kinds the manager scheme does not
// recognize. Building a controller For() an unregistered type would abort the
// whole manager; registration units skip instead — loudly, because in
// production (cmd/main.go registers api/v1alpha1 in init()) a skip means a
// wiring regression.
func missingImageFlowKinds(mgr ctrl.Manager, kinds ...string) []string {
	var missing []string
	for _, kind := range kinds {
		if !mgr.GetScheme().Recognizes(imageflowv1alpha1.GroupVersion.WithKind(kind)) {
			missing = append(missing, kind)
		}
	}
	return missing
}

func logSkippedRegistration(controllerName string, missingKinds []string) {
	ctrl.Log.WithName("setup").Info(
		"ImageFlow controller registration SKIPPED: the manager scheme does not recognize the required kinds; "+
			"production wiring registers api/v1alpha1 in cmd/main.go init(), so a skip in production is a wiring regression",
		"controller", controllerName,
		"groupVersion", imageflowv1alpha1.GroupVersion.String(),
		"missingKinds", missingKinds,
	)
}

// MappingReconciler reconciles ImageStreamMapping task objects.
type MappingReconciler struct {
	client   client.Client
	recorder record.EventRecorder
	clock    clock.Clock
	config   config.Config
	metrics  *mappingMetrics
}

// NewMappingReconciler builds the reconciler. A nil metrics set is replaced
// with an unregistered one (tests); production passes the shared collectors
// registered against controller-runtime's global metrics registry.
func NewMappingReconciler(
	cl client.Client,
	recorder record.EventRecorder,
	clk clock.Clock,
	cfg config.Config,
	metrics *mappingMetrics,
) *MappingReconciler {
	if metrics == nil {
		metrics = newMappingMetrics()
	}
	return &MappingReconciler{client: cl, recorder: recorder, clock: clk, config: cfg, metrics: metrics}
}

// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreammappings,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreammappings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreams,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imageartifacts,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile processes one ImageStreamMapping task. Terminal tasks reconcile to
// zero effects; non-terminal tasks run the (idempotent) registration and end in
// Complete=True with reason Succeeded or Failed. Transient errors requeue with
// backoff — that is the retry half of the Image Change reliability contract.
func (r *MappingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var mapping imageflowv1alpha1.ImageStreamMapping
	if err := r.client.Get(ctx, req.NamespacedName, &mapping); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil // deleted (e.g. by TTL GC) before we got to it
		}
		return ctrl.Result{}, fmt.Errorf("get mapping: %w", err)
	}
	if taskIsTerminal(&mapping.Status.TaskObjectStatus) {
		return ctrl.Result{}, nil
	}

	if err := r.ensureStartedAt(ctx, req.NamespacedName); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("record startedAt: %w", err)
	}

	_, digest, err := SplitDigestReference(mapping.Spec.Image)
	if err != nil {
		message := fmt.Sprintf("%s: spec.image %q must be a digest reference <repo>@sha256:<64hex>; "+
			"a registration without a digest cannot produce an Image Change (ADR-004)",
			failCodeInvalidImageReference, mapping.Spec.Image)
		return r.failTask(ctx, req.NamespacedName, "", message)
	}

	res, err := RegisterImage(ctx, r.client, r.recorder, r.clock, r.config, Registration{
		Namespace:      mapping.Namespace,
		Stream:         mapping.Spec.Stream,
		Tag:            mapping.Spec.Tag,
		ImageReference: mapping.Spec.Image,
		Digest:         digest,
		Source:         RegistrationSourceMapping,
		SourceTask:     mapping.Name,
	})
	if err != nil {
		if errors.Is(err, ErrStreamNotFound) {
			message := fmt.Sprintf("%s: %v; create the stream first or fix spec.stream",
				failCodeStreamNotFound, err)
			return r.failTask(ctx, req.NamespacedName, digest, message)
		}
		// Transient: requeue with backoff and retry; persisted-state dedup makes
		// the retry safe (no duplicate Image Change).
		return ctrl.Result{}, fmt.Errorf("register image: %w", err)
	}

	message := mappingSuccessMessage(&mapping, digest, res)
	completed, err := r.completeTask(ctx, req.NamespacedName, taskCompletion{
		reason:      imageflowv1alpha1.ReasonSucceeded,
		message:     message,
		digest:      digest,
		artifactRef: res.ArtifactName,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("write completion status: %w", err)
	}

	r.metrics.registrations.WithLabelValues(string(res.Outcome)).Inc()
	r.observeTaskDuration(completed, taskResultSuccess)
	r.recorder.Eventf(completed, corev1.EventTypeNormal, EventReasonMappingRegistered, "%s", message)
	logger.Info("mapping registration complete",
		"stream", mapping.Spec.Stream, "tag", mapping.Spec.Tag,
		"digest", digest, "outcome", res.Outcome, "artifact", res.ArtifactName)
	return ctrl.Result{}, nil
}

func mappingSuccessMessage(mapping *imageflowv1alpha1.ImageStreamMapping, digest string, res RegistrationResult) string {
	if res.Outcome == RegistrationNoop {
		return fmt.Sprintf("No-op success: %s:%s already references %s — idempotent re-registration, "+
			"no duplicate Image Change", mapping.Spec.Stream, mapping.Spec.Tag, digest)
	}
	previous := "none"
	if res.OldDigest != "" {
		previous = res.OldDigest
	}
	return fmt.Sprintf("Registered %s onto %s:%s; image change recorded (previous: %s)",
		mapping.Spec.Image, mapping.Spec.Stream, mapping.Spec.Tag, previous)
}

// failTask writes the terminal failure status, records metrics and emits the
// audit Event. A failed task is terminal (no requeue): it stays around for the
// failure TTL (default 24h) so the diagnosable reason can be inspected.
func (r *MappingReconciler) failTask(
	ctx context.Context,
	key types.NamespacedName,
	digest string,
	message string,
) (ctrl.Result, error) {
	completed, err := r.completeTask(ctx, key, taskCompletion{
		reason:  imageflowv1alpha1.ReasonFailed,
		message: message,
		digest:  digest,
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("write failure status: %w", err)
	}
	r.metrics.registrations.WithLabelValues(regOutcomeFailed).Inc()
	r.observeTaskDuration(completed, taskResultFailure)
	r.recorder.Eventf(completed, corev1.EventTypeWarning, EventReasonMappingFailed, "%s", message)
	return ctrl.Result{}, nil
}

// taskCompletion is the terminal status payload (Complete=True, reason from the
// frozen Succeeded/Failed vocabulary, diagnosable message).
type taskCompletion struct {
	reason      string
	message     string
	digest      string
	artifactRef string
}

// completeTask writes the terminal status through the status subresource with
// retry-on-conflict, pinning every timestamp to the injected clock (seam S3).
func (r *MappingReconciler) completeTask(
	ctx context.Context,
	key types.NamespacedName,
	c taskCompletion,
) (*imageflowv1alpha1.ImageStreamMapping, error) {
	var fresh imageflowv1alpha1.ImageStreamMapping
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.client.Get(ctx, key, &fresh); err != nil {
			return err
		}
		now := r.clock.Now()
		st := &fresh.Status
		if st.StartedAt == nil {
			startedAt := metav1.NewTime(now)
			st.StartedAt = &startedAt
		}
		completedAt := metav1.NewTime(now)
		st.CompletedAt = &completedAt
		if c.digest != "" {
			st.Digest = c.digest
		}
		if c.artifactRef != "" {
			st.ArtifactRef = c.artifactRef
		}
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               imageflowv1alpha1.ConditionTypeComplete,
			Status:             metav1.ConditionTrue,
			Reason:             c.reason,
			Message:            c.message,
			ObservedGeneration: fresh.Generation,
		})
		// SetStatusCondition stamps LastTransitionTime from the wall clock; pin
		// it to the injected clock so TTL GC and tests are deterministic.
		if cond := meta.FindStatusCondition(st.Conditions, imageflowv1alpha1.ConditionTypeComplete); cond != nil {
			cond.LastTransitionTime = metav1.NewTime(now)
		}
		return r.client.Status().Update(ctx, &fresh)
	})
	if err != nil {
		return nil, err
	}
	return &fresh, nil
}

// ensureStartedAt persists startedAt (once) before any effect is performed, so
// task duration survives a mid-registration crash.
func (r *MappingReconciler) ensureStartedAt(ctx context.Context, key types.NamespacedName) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh imageflowv1alpha1.ImageStreamMapping
		if err := r.client.Get(ctx, key, &fresh); err != nil {
			return err
		}
		if fresh.Status.StartedAt != nil || taskIsTerminal(&fresh.Status.TaskObjectStatus) {
			return nil
		}
		now := metav1.NewTime(r.clock.Now())
		fresh.Status.StartedAt = &now
		return r.client.Status().Update(ctx, &fresh)
	})
}

func (r *MappingReconciler) observeTaskDuration(completed *imageflowv1alpha1.ImageStreamMapping, result string) {
	st := &completed.Status
	if st.StartedAt == nil || st.CompletedAt == nil {
		return
	}
	r.metrics.taskDuration.WithLabelValues(result).
		Observe(st.CompletedAt.Sub(st.StartedAt.Time).Seconds())
}

// taskIsTerminal reports whether a task object reached its terminal state
// (Complete=True). TTL GC keys off this plus completedAt; reconcilers use it to
// make re-reconciliation of finished tasks effect-free.
func taskIsTerminal(st *imageflowv1alpha1.TaskObjectStatus) bool {
	cond := meta.FindStatusCondition(st.Conditions, imageflowv1alpha1.ConditionTypeComplete)
	return cond != nil && cond.Status == metav1.ConditionTrue
}
