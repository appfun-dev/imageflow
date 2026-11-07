package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// Task TTL garbage collection (ticket T4; ADR-002 R6/R7, ADR-004 §2).
//
// GC is uniform across both short-lived task kinds — ImageStreamMapping and
// ImageStreamImport — and keys purely off the shared TaskObjectStatus shape:
// terminal Complete=True plus completedAt. Successes are collected after
// --mapping-ttl-success (default 1h), failures after --mapping-ttl-failure
// (default 24h, failure stays around for diagnosis). All timestamps come from
// the injected clock (seam S3), so tests advance time deterministically.
//
// T7 (Import pull path) reuses this mechanism untouched: the task-gc-import
// controller below already collects completed ImageStreamImport tasks.

// Default TTLs, used defensively when a zero-valued config.Config reaches the
// reconciler (production binds flags with these defaults and Validate()
// rejects non-positive values).
const (
	defaultMappingTTLSuccess = time.Hour
	defaultMappingTTLFailure = 24 * time.Hour
)

func init() {
	Registrars = append(Registrars, &taskGCRegistrar{})
}

type taskGCRegistrar struct{}

func (taskGCRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	return SetupTaskGCControllers(mgr, opts, DefaultMappingControllerNames())
}

// SetupTaskGCControllers registers the TTL GC controllers for both task kinds.
// Each kind gets its own controller (distinct process-global name); a kind
// missing from the manager scheme is skipped loudly instead of aborting the
// manager build.
func SetupTaskGCControllers(mgr ctrl.Manager, opts Options, names MappingControllerNames) error {
	if err := setupTaskGCController(mgr, opts, names.GCMapping, gcKindMapping, kindImageStreamMapping); err != nil {
		return err
	}
	return setupTaskGCController(mgr, opts, names.GCImport, gcKindImport, kindImageStreamImport)
}

func setupTaskGCController(mgr ctrl.Manager, opts Options, name, gcKind, apiKind string) error {
	if missing := missingImageFlowKinds(mgr, apiKind); len(missing) > 0 {
		logSkippedRegistration(name, missing)
		return nil
	}
	reconciler := &taskGCReconciler{
		client:   mgr.GetClient(),
		recorder: mgr.GetEventRecorderFor(recorderNameTaskGC),
		clock:    opts.Clock,
		config:   opts.Config,
		metrics:  sharedMappingMetrics(),
		kind:     gcKind,
	}
	var task client.Object
	switch gcKind {
	case gcKindMapping:
		task = &imageflowv1alpha1.ImageStreamMapping{}
	case gcKindImport:
		task = &imageflowv1alpha1.ImageStreamImport{}
	default:
		return fmt.Errorf("unknown task kind label %q", gcKind)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(task).
		Named(name).
		Complete(reconciler)
}

// taskGCReconciler deletes task objects whose terminal state is older than the
// configured TTL. It never writes status; the owning controllers complete
// tasks, GC only removes them (plus one audit Event and one metric).
type taskGCReconciler struct {
	client   client.Client
	recorder record.EventRecorder
	clock    clock.Clock
	config   config.Config
	metrics  *mappingMetrics
	kind     string // gcKindMapping | gcKindImport (metric label + audit message)
}

// Reconcile collects one task object if its TTL has elapsed, otherwise it
// requeues for the remaining time. Non-terminal tasks are left alone: the
// terminal status write itself re-triggers this controller via the watch.
func (r *taskGCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj, status, err := r.fetchTask(ctx, req)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get %s task: %w", r.kind, err)
	}
	return gcCompletedTask(ctx, gcTaskDeps{
		client:   r.client,
		recorder: r.recorder,
		clock:    r.clock,
		config:   r.config,
		metrics:  r.metrics,
	}, r.kind, obj, status)
}

func (r *taskGCReconciler) fetchTask(
	ctx context.Context,
	req ctrl.Request,
) (client.Object, *imageflowv1alpha1.TaskObjectStatus, error) {
	switch r.kind {
	case gcKindMapping:
		task := &imageflowv1alpha1.ImageStreamMapping{}
		if err := r.client.Get(ctx, req.NamespacedName, task); err != nil {
			return nil, nil, err
		}
		return task, &task.Status.TaskObjectStatus, nil
	case gcKindImport:
		task := &imageflowv1alpha1.ImageStreamImport{}
		if err := r.client.Get(ctx, req.NamespacedName, task); err != nil {
			return nil, nil, err
		}
		return task, &task.Status.TaskObjectStatus, nil
	default:
		return nil, nil, fmt.Errorf("unknown task kind label %q", r.kind)
	}
}

// gcTaskDeps bundles the collaborators of the shared GC decision so both task
// kinds run through identical logic.
type gcTaskDeps struct {
	client   client.Client
	recorder record.EventRecorder
	clock    clock.Clock
	config   config.Config
	metrics  *mappingMetrics
}

// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreamimports,verbs=get;list;watch;delete

// gcCompletedTask is the uniform TTL GC decision for both task kinds. The
// decision surface is exactly the persisted TaskObjectStatus: terminal
// Complete=True plus completedAt; reason Succeeded selects the success TTL,
// anything else the (longer) failure TTL so botched registrations survive for
// diagnosis. Not due yet -> requeue after the remaining time; due -> delete
// with an audit Event.
func gcCompletedTask(
	ctx context.Context,
	deps gcTaskDeps,
	kind string,
	obj client.Object,
	st *imageflowv1alpha1.TaskObjectStatus,
) (ctrl.Result, error) {
	if !taskIsTerminal(st) || st.CompletedAt == nil {
		return ctrl.Result{}, nil // not collectable: no terminal state to count TTL from
	}

	ttl := taskTTL(deps.config, st)
	remaining := ttl - deps.clock.Since(st.CompletedAt.Time)
	if remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	if err := deps.client.Delete(ctx, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("delete %s task %s/%s: %w", kind, obj.GetNamespace(), obj.GetName(), err)
	}

	reason := imageflowv1alpha1.ReasonFailed
	if taskSucceeded(st) {
		reason = imageflowv1alpha1.ReasonSucceeded
	}
	deps.recorder.Eventf(obj, corev1.EventTypeNormal, EventReasonTaskGarbageCollected,
		"Deleted completed %s task %s/%s: %s with TTL %s elapsed (completedAt %s)",
		kind, obj.GetNamespace(), obj.GetName(), reason, ttl,
		st.CompletedAt.UTC().Format(time.RFC3339))
	deps.metrics.gcDeletions.WithLabelValues(kind).Inc()
	log.FromContext(ctx).Info("task garbage collected",
		"kind", kind, "namespace", obj.GetNamespace(), "name", obj.GetName(),
		"reason", reason, "ttl", ttl)
	return ctrl.Result{}, nil
}

// taskTTL selects the TTL for a terminal task: success TTL for
// Complete=True/reason=Succeeded, failure TTL otherwise (unknown reasons are
// treated conservatively as failures). Non-positive configured values fall
// back to the documented defaults.
func taskTTL(cfg config.Config, st *imageflowv1alpha1.TaskObjectStatus) time.Duration {
	if taskSucceeded(st) {
		if cfg.MappingTTLSuccess > 0 {
			return cfg.MappingTTLSuccess
		}
		return defaultMappingTTLSuccess
	}
	if cfg.MappingTTLFailure > 0 {
		return cfg.MappingTTLFailure
	}
	return defaultMappingTTLFailure
}

func taskSucceeded(st *imageflowv1alpha1.TaskObjectStatus) bool {
	cond := meta.FindStatusCondition(st.Conditions, imageflowv1alpha1.ConditionTypeComplete)
	return cond != nil && cond.Status == metav1.ConditionTrue && cond.Reason == imageflowv1alpha1.ReasonSucceeded
}
