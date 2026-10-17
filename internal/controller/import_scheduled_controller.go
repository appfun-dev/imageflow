package controller

// Scheduled import controller (ticket T7, ADR-004 §3, spec decision 2).
//
// Streams whose spec.tags[] declare importPolicy.scheduled=true together with
// an external source (from) are polled periodically: this controller creates
// one ImageStreamImport task per due tag, and the ImageStreamImport controller
// (import_controller.go) executes it through the S2 registry seam and the
// shared T4 registration machinery. External tag drift (same tag, new digest)
// therefore becomes a real Image Change without any user action, attributed to
// the pull path (EventReasonTagDriftDetected + the shared ImageChanged event).
//
// Separation of concerns: this unit is a CADENCE controller. It never talks to
// a registry, never writes catalog state and never writes task status — it only
// creates task objects (plus audit Events and metrics). That keeps the registry
// seam, the credential handling and the failure taxonomy in exactly one place.
//
// Scheduling model (seam S3, deterministic under an injected clock):
//
//   - due-ness is recomputed from PERSISTED state on every reconcile: the
//     anchor is the newest scheduled task of that (stream, tag), measured by
//     completedAt when terminal and by creationTimestamp while in flight. No
//     in-memory schedule survives — or is needed for — a controller restart.
//   - effective interval = spec.tags[].importPolicy.intervalSeconds when set,
//     else --scheduled-default-interval, else --import-default-interval, else
//     the built-in 15m (ADR-004 default of 900s).
//   - a tag with a non-terminal scheduled task never stacks a second one.
//   - the reconcile returns RequeueAfter(the earliest remaining wait) so the
//     cadence runs on wall-clock time in production and in kind e2e (short
//     interval flags); envtest drives it by advancing the injected clock and
//     touching the stream, exactly like the T4 TTL GC specs.
//   - anchoring caveat, documented on purpose: when every scheduled task of a
//     tag has been TTL-collected — possible only when intervalSeconds exceeds
//     the task TTLs — the next reconcile imports immediately and re-anchors.
//     Imports can then happen more often than configured, never less often,
//     and drift detection stays correct.
//
// Credential caveat (v1 API shape, handed to T12): ImageStream.spec.tags[]
// carries no pull-secret reference, so scheduled imports always run
// ANONYMOUSLY. On-demand ImageStreamImport tasks can reference a pull secret;
// a per-tag secret reference would be an API change (v1.1 candidate).

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// defaultScheduledImportInterval is the built-in fallback of the effective
// interval chain (ADR-004: interval default 900s) and only applies when a
// zero-valued config reaches the reconciler.
const defaultScheduledImportInterval = 15 * time.Minute

// Labels and label values this controller puts on the tasks it creates. The
// source label is the selector used to find "my" tasks again; the value is a
// constant, so it is always a valid label value. Tag/stream identity is NOT
// encoded in a label (tag names may exceed the 63-character label limit and may
// carry characters label values forbid) — it is read from the task spec, which
// is authoritative anyway.
const (
	// LabelImportSource marks who created an ImageStreamImport task.
	LabelImportSource = "imageflow.appfun.dev/import-source"
	// LabelValueScheduledImport marks tasks created by this controller.
	LabelValueScheduledImport = "scheduled"
	// LabelValueOnDemandImport marks tasks created by users/CI.
	LabelValueOnDemandImport = "on-demand"
)

// scheduledTaskKind is the Kind of the owner reference this controller sets on
// the tasks it creates (cascade GC with the stream).
const scheduledTaskOwnerKind = "ImageStream"

// ScheduledImportReconciler polls ImageStreams for due scheduled tag imports.
type ScheduledImportReconciler struct {
	client   client.Client
	recorder record.EventRecorder
	clock    clock.Clock
	config   config.Config
	metrics  *importMetrics
}

// NewScheduledImportReconciler builds the cadence controller. A nil clock falls
// back to the real clock; a nil metrics set is replaced with an unregistered
// one (tests).
func NewScheduledImportReconciler(
	cl client.Client,
	recorder record.EventRecorder,
	clk clock.Clock,
	cfg config.Config,
	metrics *importMetrics,
) *ScheduledImportReconciler {
	if clk == nil {
		clk = clock.Real{}
	}
	if metrics == nil {
		metrics = newImportMetrics()
	}
	return &ScheduledImportReconciler{
		client: cl, recorder: recorder, clock: clk, config: cfg, metrics: metrics,
	}
}

// Reconcile creates the due scheduled import tasks of one ImageStream and
// reports when the next decision is due. Errors of one tag do not block the
// other tags of the same stream; a creation failure is reported (Event +
// metric) and requeued through the returned error.
func (r *ScheduledImportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var stream imageflowv1alpha1.ImageStream
	if err := r.client.Get(ctx, req.NamespacedName, &stream); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil // stream deleted; owned tasks cascade with it
		}
		return ctrl.Result{}, fmt.Errorf("get stream: %w", err)
	}

	pending := scheduledTags(stream.Spec.Tags)
	if len(pending) == 0 {
		return ctrl.Result{}, nil
	}

	existing, err := r.listScheduledTasks(ctx, stream.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}

	now := r.clock.Now()
	var earliest time.Duration
	var firstFailure error
	for _, tag := range pending {
		interval := EffectiveImportInterval(tag, r.config)
		history := tasksForTag(existing, stream.Name, tag.Name)
		due, wait, inFlight := importScheduleDecision(history, now, interval)
		if !due {
			earliest = minDuration(earliest, wait)
			if inFlight {
				logger.V(1).Info("scheduled import still in flight; not stacking a task",
					"stream", stream.Name, "tag", tag.Name)
			}
			continue
		}

		created, createErr := r.createScheduledTask(ctx, &stream, tag, interval)
		if createErr != nil {
			r.metrics.scheduledCreations.WithLabelValues(scheduledCreateError).Inc()
			r.recorder.Eventf(&stream, corev1.EventTypeWarning, EventReasonScheduledImportError,
				"Could not create the scheduled ImageStreamImport for tag %q (source %s): %v",
				tag.Name, tag.From.Name, createErr)
			if firstFailure == nil {
				firstFailure = fmt.Errorf("create scheduled import task for %s:%s: %w",
					stream.Name, tag.Name, createErr)
			}
			earliest = minDuration(earliest, interval)
			continue
		}

		r.metrics.scheduledCreations.WithLabelValues(scheduledCreateCreated).Inc()
		r.recorder.Eventf(&stream, corev1.EventTypeNormal, EventReasonScheduledImportCreated,
			"Created scheduled ImageStreamImport %s for tag %q (source %s, interval %s)",
			created.Name, tag.Name, tag.From.Name, interval)
		logger.V(1).Info("scheduled import task created",
			"stream", stream.Name, "tag", tag.Name, "task", created.Name, "interval", interval)
		earliest = minDuration(earliest, interval)
	}

	if firstFailure != nil {
		return ctrl.Result{}, firstFailure
	}
	if earliest > 0 {
		return ctrl.Result{RequeueAfter: earliest}, nil
	}
	return ctrl.Result{}, nil
}

// createScheduledTask creates one ImageStreamImport task for a due tag. The
// task is owned by the stream (controlling ownerRef, so it cascades on stream
// deletion) and labelled as scheduled, which is how this controller recognises
// its own tasks on the next pass.
func (r *ScheduledImportReconciler) createScheduledTask(
	ctx context.Context,
	stream *imageflowv1alpha1.ImageStream,
	tag imageflowv1alpha1.TagReference,
	interval time.Duration,
) (*imageflowv1alpha1.ImageStreamImport, error) {
	task := &imageflowv1alpha1.ImageStreamImport{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: scheduledTaskPrefix(stream.Name, tag.Name),
			Namespace:    stream.Namespace,
			Labels:       map[string]string{LabelImportSource: LabelValueScheduledImport},
			Annotations: map[string]string{
				AnnotationScheduledInterval: interval.String(),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         imageflowv1alpha1.GroupVersion.String(),
				Kind:               scheduledTaskOwnerKind,
				Name:               stream.Name,
				UID:                stream.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(false),
			}},
		},
		Spec: imageflowv1alpha1.ImageStreamImportSpec{
			Stream: stream.Name,
			Tag:    tag.Name,
			From:   *tag.From.DeepCopy(),
			// No PullSecretRef: the v1 ImageStream API carries no per-tag secret
			// reference, so scheduled imports are anonymous (see file godoc).
		},
	}
	if err := r.client.Create(ctx, task); err != nil {
		return nil, err
	}
	return task, nil
}

// AnnotationScheduledInterval records the effective interval a task was created
// with. It is audit metadata only — the schedule is never read back from it.
const AnnotationScheduledInterval = "imageflow.appfun.dev/scheduled-interval"

// listScheduledTasks returns the scheduled tasks of one namespace. The label
// selector keeps user-created (on-demand) tasks out of the schedule; the
// per-tag match happens on the authoritative task spec.
func (r *ScheduledImportReconciler) listScheduledTasks(
	ctx context.Context,
	namespace string,
) ([]imageflowv1alpha1.ImageStreamImport, error) {
	var list imageflowv1alpha1.ImageStreamImportList
	selector := client.MatchingLabelsSelector{
		Selector: labels.SelectorFromSet(labels.Set{LabelImportSource: LabelValueScheduledImport}),
	}
	if err := r.client.List(ctx, &list, client.InNamespace(namespace), selector); err != nil {
		return nil, fmt.Errorf("list scheduled import tasks: %w", err)
	}
	return list.Items, nil
}

// scheduledTags filters the stream tags this controller polls: scheduled must
// be true and an external source must be present (the CRD CEL rule already
// rejects scheduled without from; the check here keeps the controller total).
func scheduledTags(tags []imageflowv1alpha1.TagReference) []imageflowv1alpha1.TagReference {
	pending := make([]imageflowv1alpha1.TagReference, 0, len(tags))
	for _, tag := range tags {
		if tag.From == nil || tag.ImportPolicy.Scheduled == nil || !*tag.ImportPolicy.Scheduled {
			continue
		}
		pending = append(pending, tag)
	}
	return pending
}

// EffectiveImportInterval resolves the polling period of one tag:
// spec.tags[].importPolicy.intervalSeconds when positive, else
// --scheduled-default-interval, else --import-default-interval, else the
// built-in 15m default. Exported because the e2e and integration suites assert
// the cadence they configured.
func EffectiveImportInterval(tag imageflowv1alpha1.TagReference, cfg config.Config) time.Duration {
	if tag.ImportPolicy.IntervalSeconds != nil && *tag.ImportPolicy.IntervalSeconds > 0 {
		return time.Duration(*tag.ImportPolicy.IntervalSeconds) * time.Second
	}
	if cfg.ScheduledDefaultInterval > 0 {
		return cfg.ScheduledDefaultInterval
	}
	if cfg.ImportDefaultInterval > 0 {
		return cfg.ImportDefaultInterval
	}
	return defaultScheduledImportInterval
}

// tasksForTag selects the scheduled tasks of one (stream, tag) pair.
func tasksForTag(
	tasks []imageflowv1alpha1.ImageStreamImport,
	stream, tag string,
) []imageflowv1alpha1.ImageStreamImport {
	matching := make([]imageflowv1alpha1.ImageStreamImport, 0, len(tasks))
	for _, task := range tasks {
		if task.Spec.Stream == stream && task.Spec.Tag == tag {
			matching = append(matching, task)
		}
	}
	return matching
}

// importScheduleDecision is the pure scheduling decision of one tag: is an
// import due now, and if not, how long should the controller wait?
//
// The anchor is the newest scheduled task of the tag, measured by completedAt
// when the task is terminal and by creationTimestamp while it is in flight (the
// import controller stamps completedAt from the injected clock, so the
// arithmetic stays deterministic under seam S3). No tasks at all means the
// policy was just declared: import immediately.
func importScheduleDecision(
	history []imageflowv1alpha1.ImageStreamImport,
	now time.Time,
	interval time.Duration,
) (due bool, wait time.Duration, inFlight bool) {
	if interval <= 0 {
		interval = defaultScheduledImportInterval
	}
	if len(history) == 0 {
		return true, 0, false
	}

	newest := history[0]
	for _, task := range history[1:] {
		if taskAnchorTime(task).After(taskAnchorTime(newest)) {
			newest = task
		}
	}
	if !taskIsTerminal(&newest.Status.TaskObjectStatus) {
		// A previous scheduled import has not finished: never stack tasks. The
		// watch on owned tasks re-triggers this stream when it completes.
		return false, interval, true
	}

	elapsed := now.Sub(taskAnchorTime(newest))
	if elapsed >= interval {
		return true, 0, false
	}
	return false, interval - elapsed, false
}

// taskAnchorTime is the timestamp the schedule measures from: the terminal
// completion when present, the creation time otherwise.
func taskAnchorTime(task imageflowv1alpha1.ImageStreamImport) time.Time {
	if task.Status.CompletedAt != nil {
		return task.Status.CompletedAt.Time
	}
	return task.CreationTimestamp.Time
}

// scheduledTaskPrefix builds a DNS-1123-safe GenerateName prefix for a
// scheduled task. Tag names may carry characters (or a length) that object
// names forbid, so the tag part is normalized and, when the normalization is
// lossy or the budget is exhausted, disambiguated by the same hash-suffix
// scheme the mirror name encoder uses.
func scheduledTaskPrefix(stream, tag string) string {
	const suffix = "-import-"
	encoded := EncodeImageStreamTagName(stream, tag)
	if len(encoded)+len(suffix) > mirrorNameMaxLen {
		encoded = encoded[:mirrorNameMaxLen-len(suffix)]
	}
	return encoded + suffix
}

func minDuration(current, candidate time.Duration) time.Duration {
	if current <= 0 {
		return candidate
	}
	if candidate <= 0 {
		return current
	}
	if candidate < current {
		return candidate
	}
	return current
}
