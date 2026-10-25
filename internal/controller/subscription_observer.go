package controller

// Workload Observer (ticket T11, ADR-005 §3/§5, ADR-006, ADR-008 §2/§3):
// the observation half of the delivery contract — it turns "the field was
// written" into "the release actually progressed", per target, without ever
// deploying or rewriting anything.
//
// # Ownership split with the T9 delivery engine
//
// The engine seeds rolloutStatus=Progressing when a value-changing apply
// lands, owns the terminal Conflict state and initializes empty rollout/
// health to Unknown. This observer owns EVERY later transition
// (Progressing→Succeeded/Failed/Unknown and back) from native workload
// status, recomputes the aggregate with the engine's exported
// AggregateRolloutStatus (never a private re-derivation), heals in-flight
// recentDeliveries records to their terminal rollout result WITHOUT touching
// the engine's Superseded logic, floats delivery summaries up to the
// ImageStreamTag/ImageTag mirror status (bounded events[] + consumers), and
// writes the honest dual-namespace rollout-outcome Events.
//
// # Honesty rules (never fabricate success)
//
//   - built-in first-batch kinds (Deployment/StatefulSet/DaemonSet located by
//     container name) are judged from NATIVE status only (ADR-006 fields);
//   - every other class — third-party CRDs, imageFields locators, targets
//     with a statusPolicyRef — stays honestly Unknown with an explanation,
//     because the v1 CUE status policy evaluator is deferred to v1.1
//     (ADR-005 §4/§5). The subscription carries an explicit
//     StatusPolicyEvaluated=False Condition for that contract;
//   - healthStatus is ALWAYS Unknown in v1 (no evaluator): imageApplied=true
//     or rollout=Succeeded never implies health (ADR-005 §3);
//   - a workload whose pod template no longer references the desired image
//     is Unknown, not Succeeded/Failed: native status would describe a
//     different image.
//
// # Hot-loop safety
//
// The engine's workload watches are generation-only, so observer status
// writes cannot re-trigger the engine's apply path. This controller watches
// workload STATUS (its own observation surface — it never writes workloads)
// and subscription status.targets/recentDeliveries changes; its own writes
// re-trigger exactly one convergence pass that the DeepEqual gate quiesces.
// Terminal Events re-Get the target uncached (T4 duplicate-Event lesson).
// While any observed rollout is Progressing the controller re-polls
// (waitingStateRequeue) so determination never depends on a missed watch.

import (
	"context"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// Production controller and recorder names. Integration tests build their
// instances under distinct names: controller-runtime validates controller
// names process-globally.
const (
	observerControllerName           = "subscription-observer"
	recorderNameSubscriptionObserver = "imageflow-subscription-observer"
)

// Observer-owned per-target reasons (T11 extends the frozen T9 TargetReason*
// vocabulary — engine reasons are never redefined). They explain the ROLLOUT
// stage; the delivery engine preserves them across steady-state passes (see
// the observer-reason preservation in subscription_delivery_status.go).
const (
	TargetReasonRolloutProgressing = "RolloutProgressing"
	TargetReasonRolloutSucceeded   = "RolloutSucceeded"
	TargetReasonRolloutFailed      = "RolloutFailed"
	TargetReasonRolloutUnknown     = "RolloutUnknown"
)

// Event reasons for observed rollout outcomes (dual-namespace audit surface,
// ADR-003 §4) and the engine-side latest-wins supersede announcement.
const (
	EventReasonRolloutProgressing = "RolloutProgressing"
	EventReasonRolloutSucceeded   = "RolloutSucceeded"
	EventReasonRolloutFailed      = "RolloutFailed"
	EventReasonRolloutUnknown     = "RolloutUnknown"
	EventReasonDeliverySuperseded = "DeliverySuperseded"
)

// StatusPolicyEvaluated Condition vocabulary: the explicit v1 contract that
// health is never fabricated (ADR-005 §4/§5). Status is always False in v1 —
// no evaluator runs — and the reason distinguishes "nobody asked for one"
// from "one was asked for but the evaluator is deferred to v1.1".
const (
	ConditionTypeStatusPolicyEvaluated  = "StatusPolicyEvaluated"
	ReasonStatusPolicyNotConfigured     = "NotConfigured"
	ReasonStatusPolicyEvaluatorDeferred = "EvaluatorDeferred"
)

// auditImageNoneRecorded renders "no previous image" in audit surfaces
// (distinct from the engine/mapping lanes' phrasing on purpose).
const auditImageNoneRecorded = "none recorded"

// isObserverOwnedReason reports whether a per-target reason was written by
// this observer. The delivery engine preserves these across its steady-state
// Applied passes so the observer's rollout explanation survives as the
// per-target WHY until the next delivery or failure rewrites it.
func isObserverOwnedReason(reason string) bool {
	switch reason {
	case TargetReasonRolloutProgressing, TargetReasonRolloutSucceeded,
		TargetReasonRolloutFailed, TargetReasonRolloutUnknown:
		return true
	}
	return false
}

// observerReasonFor maps a rollout status to its observer-owned reason.
func observerReasonFor(status imageflowv1alpha1.RolloutStatus) string {
	switch status {
	case imageflowv1alpha1.RolloutProgressing:
		return TargetReasonRolloutProgressing
	case imageflowv1alpha1.RolloutSucceeded:
		return TargetReasonRolloutSucceeded
	case imageflowv1alpha1.RolloutFailed:
		return TargetReasonRolloutFailed
	case imageflowv1alpha1.RolloutUnknown:
		return TargetReasonRolloutUnknown
	}
	return ""
}

func observerEventReason(status imageflowv1alpha1.RolloutStatus) string {
	switch status {
	case imageflowv1alpha1.RolloutProgressing:
		return EventReasonRolloutProgressing
	case imageflowv1alpha1.RolloutSucceeded:
		return EventReasonRolloutSucceeded
	case imageflowv1alpha1.RolloutFailed:
		return EventReasonRolloutFailed
	case imageflowv1alpha1.RolloutUnknown:
		return EventReasonRolloutUnknown
	}
	return EventReasonRolloutUnknown
}

func observerEventType(status imageflowv1alpha1.RolloutStatus) string {
	if status == imageflowv1alpha1.RolloutFailed {
		return corev1.EventTypeWarning
	}
	return corev1.EventTypeNormal
}

func observerResultLabel(status imageflowv1alpha1.RolloutStatus) string {
	switch status {
	case imageflowv1alpha1.RolloutProgressing:
		return observerResultProgressing
	case imageflowv1alpha1.RolloutSucceeded:
		return observerResultSucceeded
	case imageflowv1alpha1.RolloutFailed:
		return observerResultFailed
	case imageflowv1alpha1.RolloutUnknown:
		return observerResultUnknown
	}
	return observerResultUnknown
}

func observerHealthLabel(status imageflowv1alpha1.HealthStatus) string {
	switch status {
	case imageflowv1alpha1.HealthHealthy:
		return observerHealthHealthy
	case imageflowv1alpha1.HealthUnhealthy:
		return observerHealthUnhealthy
	case imageflowv1alpha1.HealthUnknown:
		return observerHealthUnknown
	}
	return observerHealthUnknown
}

type subscriptionObserverRegistrar struct{}

func init() {
	Registrars = append(Registrars, subscriptionObserverRegistrar{})
}

// Register implements Registrar.
func (subscriptionObserverRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	return RegisterSubscriptionObserver(mgr, observerControllerName, opts)
}

// RegisterSubscriptionObserver wires the workload observer onto mgr under the
// given controller name. The name is a parameter because controller-runtime
// validates controller names against a process-global registry: production
// uses observerControllerName, test managers pass suffixed names.
func RegisterSubscriptionObserver(mgr ctrl.Manager, name string, opts Options) error {
	if missing := missingImageFlowKinds(mgr, kindImageSubscription, mirrorKindImageStreamTag, mirrorKindImageTag); len(missing) > 0 {
		logSkippedRegistration(name, missing)
		return nil
	}
	timeSource := opts.Clock
	if timeSource == nil {
		timeSource = clock.Real{}
	}
	r := NewSubscriptionObserverReconciler(
		mgr.GetClient(),
		mgr.GetAPIReader(),
		mgr.GetEventRecorderFor(recorderNameSubscriptionObserver),
		timeSource,
		opts.Config,
		sharedObserverMetrics(),
	)
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&imageflowv1alpha1.ImageSubscription{}, builder.WithPredicates(observerSubscriptionPredicate{})).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.subsForWorkload), builder.WithPredicates(observerWorkloadStatusPredicate{})).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestsFromMapFunc(r.subsForWorkload), builder.WithPredicates(observerWorkloadStatusPredicate{})).
		Watches(&appsv1.DaemonSet{}, handler.EnqueueRequestsFromMapFunc(r.subsForWorkload), builder.WithPredicates(observerWorkloadStatusPredicate{})).
		Complete(r)
}

// SubscriptionObserverReconciler observes native workload status and owns the
// post-delivery outcome transitions of ImageSubscription status, the
// rollout-outcome Events and the mirror delivery float-up. It performs ZERO
// writes to workloads.
type SubscriptionObserverReconciler struct {
	client    client.Client
	apiReader client.Reader // uncached: fresh workload status + terminal-Event re-Gets
	recorder  record.EventRecorder
	clock     clock.Clock
	config    config.Config
	metrics   *observerMetrics
}

// NewSubscriptionObserverReconciler builds the reconciler. A nil apiReader
// falls back to the cached client; nil metrics build an unregistered collector
// set for tests.
func NewSubscriptionObserverReconciler(cl client.Client, apiReader client.Reader, recorder record.EventRecorder, clk clock.Clock, cfg config.Config, metrics *observerMetrics) *SubscriptionObserverReconciler {
	if apiReader == nil {
		apiReader = cl
	}
	if metrics == nil {
		metrics = newObserverMetrics()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &SubscriptionObserverReconciler{
		client:    cl,
		apiReader: apiReader,
		recorder:  recorder,
		clock:     clk,
		config:    cfg,
		metrics:   metrics,
	}
}

func (r *SubscriptionObserverReconciler) reader() client.Reader {
	if r.apiReader != nil {
		return r.apiReader
	}
	return r.client
}

// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;daemonsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagesubscriptions,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagesubscriptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreamtags;imagetags,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagestreamtags/status;imagetags/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile runs one observation pass: determine every observable target's
// rollout from fresh native status, heal in-flight delivery records, write
// the honest StatusPolicyEvaluated Condition, recompute the aggregate with
// the engine's exported AggregateRolloutStatus, then — only when a status
// write actually landed — emit the dual-namespace Events, count the outcome
// metrics and float the persisted delivery window up to the mirrors.
func (r *SubscriptionObserverReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var sub imageflowv1alpha1.ImageSubscription
	if err := r.client.Get(ctx, req.NamespacedName, &sub); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if sub.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}
	// T8 authorization gate (same frozen contract as the delivery engine):
	// no verdict yet ⇒ requeue with zero writes; denied ⇒ zero writes.
	forbidden := meta.FindStatusCondition(sub.Status.Conditions, imageflowv1alpha1.ConditionTypeForbidden)
	if forbidden == nil {
		return ctrl.Result{RequeueAfter: authzPendingRequeue}, nil
	}
	if forbidden.Status == metav1.ConditionTrue {
		return ctrl.Result{}, nil
	}

	pass := r.observe(ctx, &sub)
	transition, err := r.syncObserverStatus(ctx, req.NamespacedName, pass)
	joined := errors.Join(append(pass.errs, err)...)
	if err != nil {
		return ctrl.Result{}, joined
	}
	if transition.wrote {
		r.emitObserverEffects(ctx, &sub, transition)
		r.floatUpDeliveries(ctx, req.NamespacedName)
	}
	return ctrl.Result{RequeueAfter: pass.requeue}, joined
}

// observerPass is one full observation sweep of a subscription.
type observerPass struct {
	outcomes    []observerOutcome
	requeue     time.Duration
	errs        []error
	policyRefs  int // spec targets declaring a statusPolicyRef
	targetCount int
}

// observerOutcome is the planned observation effect for one target.
type observerOutcome struct {
	ref       imageflowv1alpha1.TargetReference
	kindLabel string // bounded metric vocabulary (observerKind*)
	// rollout is the determined-or-persisted rollout status after this pass
	// (drives record healing and the poll requeue).
	rollout imageflowv1alpha1.RolloutStatus
	// transition is non-nil when the persisted entry's rolloutStatus must
	// move; it is applied only when the fresh state still matches `from`
	// (stale plans are dropped and re-planned on the next trigger).
	transition *rolloutTransition
	// reasonFix is non-nil when the rollout status stays as persisted but the
	// entry still carries the engine's steady-state Applied reason while the
	// rollout stage is Unknown: the observer then writes its honest
	// explanation once (reason/message only — no state change, no timestamp).
	reasonFix *rolloutTransition
	errs      []error
}

type rolloutTransition struct {
	from    imageflowv1alpha1.RolloutStatus
	to      imageflowv1alpha1.RolloutStatus
	reason  string
	message string
}

func (r *SubscriptionObserverReconciler) observe(ctx context.Context, sub *imageflowv1alpha1.ImageSubscription) observerPass {
	pass := observerPass{}
	seen := map[imageflowv1alpha1.TargetReference]struct{}{}
	for i := range sub.Spec.Targets {
		target := sub.Spec.Targets[i]
		if _, dup := seen[target.TargetRef]; dup {
			continue
		}
		seen[target.TargetRef] = struct{}{}
		pass.targetCount++
		if target.StatusPolicyRef != nil {
			pass.policyRefs++
		}
		out := r.observeTarget(ctx, sub, target)
		if out == nil {
			continue
		}
		pass.outcomes = append(pass.outcomes, *out)
		pass.errs = append(pass.errs, out.errs...)
		if out.rollout == imageflowv1alpha1.RolloutProgressing && pass.requeue == 0 {
			// Poll fallback: watches can be missed; an in-flight rollout is
			// re-judged on a bounded cadence until it reaches a terminal
			// result (native progressDeadlineSeconds bounds the Failed case).
			pass.requeue = waitingStateRequeue
		}
	}
	return pass
}

// observeTarget plans the observation effects for one spec target against
// the subscription's persisted status snapshot. nil means "nothing to
// observe": the engine has not written an entry yet, the entry is T8-owned
// (Forbidden) or terminal (Conflict/Superseded — engine domain).
func (r *SubscriptionObserverReconciler) observeTarget(ctx context.Context, sub *imageflowv1alpha1.ImageSubscription, target imageflowv1alpha1.SubscriptionTarget) *observerOutcome {
	ref := target.TargetRef
	idx := findTargetEntry(sub.Status.Targets, ref)
	if idx < 0 {
		return nil // engine has not written this target yet — its domain
	}
	entry := &sub.Status.Targets[idx]
	if entry.Reason == TargetReasonForbidden {
		return nil // T8 owns this entry's lifecycle end to end
	}
	if entry.DesiredImage == "" {
		return nil // no delivery was ever attempted for this target
	}
	if entry.RolloutStatus == imageflowv1alpha1.RolloutConflict || entry.Reason == TargetReasonConflict {
		return nil // terminal Conflict is engine-owned (ADR-008 first-wins)
	}
	if entry.RolloutStatus == imageflowv1alpha1.RolloutSuperseded {
		return nil // defensive: entry-level Superseded is terminal
	}

	out := &observerOutcome{ref: ref, kindLabel: observerKindOther, rollout: entry.RolloutStatus}
	gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)
	kindLabel, observable := observableBuiltinKind(gvk, target)
	if !observable {
		// Honest Unknown (ADR-005 §5, ADR-006): v1 has no evaluator and no
		// native signal for this class. Any persisted non-terminal rollout
		// state is flipped ONCE with the contract explanation; Unknown stays.
		switch entry.RolloutStatus {
		case imageflowv1alpha1.RolloutProgressing,
			imageflowv1alpha1.RolloutSucceeded,
			imageflowv1alpha1.RolloutFailed:
			out.transition = &rolloutTransition{
				from:    entry.RolloutStatus,
				to:      imageflowv1alpha1.RolloutUnknown,
				reason:  TargetReasonRolloutUnknown,
				message: honestUnknownMessage(gvk, target),
			}
			out.rollout = imageflowv1alpha1.RolloutUnknown
		case imageflowv1alpha1.RolloutUnknown, "":
			// "" is initialized by the engine; Unknown is already honest.
		}
		if entry.RolloutStatus == imageflowv1alpha1.RolloutUnknown && entry.Reason == TargetReasonApplied {
			out.reasonFix = &rolloutTransition{
				from:    imageflowv1alpha1.RolloutUnknown,
				to:      imageflowv1alpha1.RolloutUnknown,
				reason:  TargetReasonRolloutUnknown,
				message: honestUnknownMessage(gvk, target),
			}
		}
		return out
	}

	out.kindLabel = kindLabel
	facts, found, err := r.getWorkloadFacts(ctx, gvk, ref)
	if err != nil {
		out.errs = append(out.errs, err)
		return out
	}
	if !found {
		// Target absent / kind not installed: the engine owns that WHY
		// (TargetNotFound / KindNotInstalled); the observer stays silent and
		// keeps the persisted state (poll requeue covers reappearance).
		return out
	}
	det := r.determineRollout(ctx, facts, target, *entry)
	out.rollout = det.status
	switch {
	case det.status == entry.RolloutStatus:
		if det.status == imageflowv1alpha1.RolloutUnknown && entry.Reason == TargetReasonApplied {
			// Rollout stays Unknown but the entry still carries the engine's
			// steady-state Applied reason: write the honest per-target
			// explanation once (reason/message only, no state transition).
			out.reasonFix = &rolloutTransition{
				from:    det.status,
				to:      det.status,
				reason:  det.reason,
				message: det.message,
			}
		}
	case entry.RolloutStatus == "" && det.status == imageflowv1alpha1.RolloutUnknown:
		// The engine initializes empty rolloutStatus to Unknown on its next
		// pass with its own reason surface; the observer does not race it.
	default:
		out.transition = &rolloutTransition{
			from:    entry.RolloutStatus,
			to:      det.status,
			reason:  det.reason,
			message: det.message,
		}
	}
	return out
}

// observerTransition carries the effects of one status write.
type observerTransition struct {
	wrote          bool
	events         []observerPlannedEvent
	rolloutSamples []observerMetricSample
	healthSamples  []string
}

type observerPlannedEvent struct {
	eventType string
	reason    string
	message   string
	ref       imageflowv1alpha1.TargetReference
}

type observerMetricSample struct {
	kind   string
	result string
}

// syncObserverStatus writes the observed state with the T9 engine's
// discipline: retry-on-conflict over a FRESH read, planned transitions
// applied only when the fresh state still matches the plan (stale plans are
// dropped — the racing write re-triggers this controller), aggregate
// recomputed with the engine's exported AggregateRolloutStatus, and a
// semantic DeepEqual gate so steady-state passes never write.
func (r *SubscriptionObserverReconciler) syncObserverStatus(ctx context.Context, key types.NamespacedName, pass observerPass) (observerTransition, error) {
	var transition observerTransition
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		transition = observerTransition{}
		var fresh imageflowv1alpha1.ImageSubscription
		if err := r.client.Get(ctx, key, &fresh); err != nil {
			return err
		}
		now := r.clock.Now()
		computed := *fresh.Status.DeepCopy()
		r.applyObserverOutcomes(&fresh, &computed, pass.outcomes, now, &transition)
		healInFlightRecords(&computed, pass.outcomes)
		setStatusPolicyCondition(&computed, pass.policyRefs, fresh.Generation, now)
		computed.RolloutStatus = AggregateRolloutStatus(computed.Targets)
		computed.ObservedGeneration = fresh.Generation
		if equality.Semantic.DeepEqual(fresh.Status, computed) {
			transition = observerTransition{}
			return nil
		}
		fresh.Status = computed
		if err := r.client.Status().Update(ctx, &fresh); err != nil {
			return err
		}
		transition.wrote = true
		return nil
	})
	if err != nil {
		return observerTransition{}, err
	}
	return transition, nil
}

// applyObserverOutcomes writes the planned transitions (and reason-only
// fixes) into the computed status, verifying each plan against the fresh
// state, and collects the Events/metrics samples. Events and metrics are
// emitted by the caller ONLY after the write landed.
func (r *SubscriptionObserverReconciler) applyObserverOutcomes(fresh *imageflowv1alpha1.ImageSubscription, computed *imageflowv1alpha1.ImageSubscriptionStatus, outcomes []observerOutcome, now time.Time, transition *observerTransition) {
	for i := range outcomes {
		out := &outcomes[i]
		if out.transition == nil && out.reasonFix == nil {
			continue
		}
		idx := findTargetEntry(computed.Targets, out.ref)
		if idx < 0 {
			continue
		}
		entry := &computed.Targets[idx]
		if out.transition != nil {
			if entry.RolloutStatus != out.transition.from {
				// Stale plan: a concurrent write (e.g. the engine seeding a
				// Progressing re-target) moved the entry. Drop it — that
				// write re-triggers this controller and the next pass
				// re-determines from fresh state.
				continue
			}
			entry.RolloutStatus = out.transition.to
			entry.LastTransitionTime = ptrTime(now)
			entry.Reason = out.transition.reason
			entry.Message = authz.TruncateMessage(out.transition.message, 1024)
		} else {
			fix := out.reasonFix
			if entry.RolloutStatus != fix.from || entry.Reason != TargetReasonApplied {
				continue // stale plan
			}
			// Reason/message only: no state change ⇒ LastTransitionTime and
			// health stay untouched. No Event either — nothing happened to
			// the rollout; this is the standing explanation surface.
			entry.Reason = fix.reason
			entry.Message = authz.TruncateMessage(fix.message, 1024)
			continue
		}
		rec := attributedRecord(computed.RecentDeliveries, *entry)
		transition.events = append(transition.events, observerPlannedEvent{
			eventType: observerEventType(out.transition.to),
			reason:    observerEventReason(out.transition.to),
			message:   authz.TruncateMessage(observerAuditMessage(fresh, *entry, rec, out.transition.to), 1024),
			ref:       out.ref,
		})
		transition.rolloutSamples = append(transition.rolloutSamples, observerMetricSample{
			kind:   out.kindLabel,
			result: observerResultLabel(out.transition.to),
		})
		// Health sample per rollout transition: v1 only ever reports unknown
		// honestly (no evaluator, ADR-005 §4) — rollout success never implies
		// health.
		transition.healthSamples = append(transition.healthSamples, observerHealthLabel(entry.HealthStatus))
	}
}

// healInFlightRecords moves in-flight recentDeliveries records ("" or
// Progressing — the engine's seed) to the entry's TERMINAL rollout result
// (Succeeded/Failed) once the observation knows it, with the matching
// observer reason. It never touches Superseded/Conflict/terminal records and
// never demotes a record to Unknown/Progressing: an unresolved record stays
// in-flight, which is exactly what the engine's latest-wins Superseded
// marking (subscription_delivery_status.go) expects.
func healInFlightRecords(computed *imageflowv1alpha1.ImageSubscriptionStatus, outcomes []observerOutcome) {
	for j := range computed.RecentDeliveries {
		rec := &computed.RecentDeliveries[j]
		if rec.RolloutResult != "" && rec.RolloutResult != imageflowv1alpha1.RolloutProgressing {
			continue // terminal (Succeeded/Failed/Superseded/Conflict/Unknown-frozen)
		}
		idx := findTargetEntry(computed.Targets, rec.TargetRef)
		if idx < 0 {
			continue
		}
		entry := &computed.Targets[idx]
		// Attribution: the record describes the image actually on the object
		// (ObservedImage), or — while the engine's status write is still
		// converging — the desired image of an applied entry.
		if rec.NewImage != entry.ObservedImage && (!entry.ImageApplied || rec.NewImage != entry.DesiredImage) {
			continue
		}
		out := findOutcome(outcomes, rec.TargetRef)
		if out == nil {
			continue
		}
		switch out.rollout {
		case imageflowv1alpha1.RolloutSucceeded, imageflowv1alpha1.RolloutFailed:
			if rec.RolloutResult != out.rollout {
				rec.RolloutResult = out.rollout
				rec.Reason = observerReasonFor(out.rollout)
			}
		default:
			// Progressing/Unknown stay in-flight (honest: no terminal result
			// was observed). The engine's Superseded marking and the bounded
			// window remain the only writers of these records from here.
		}
	}
}

// setStatusPolicyCondition maintains the explicit v1 health contract
// Condition (ADR-005 §4/§5): Status=False always (no evaluator runs in v1),
// reason NotConfigured when no target declares a statusPolicyRef and
// EvaluatorDeferred when at least one does. Success is never fabricated; the
// Condition is the durable explanation. LastTransitionTime is pinned to the
// injected clock on status flips (T8 pointer-aliasing lesson: snapshot the
// previous condition BY VALUE before SetStatusCondition mutates in place).
func setStatusPolicyCondition(computed *imageflowv1alpha1.ImageSubscriptionStatus, policyRefs int, generation int64, now time.Time) {
	reason := ReasonStatusPolicyNotConfigured
	message := "No target declares a statusPolicyRef and v1 ships no CUE status policy evaluator (deferred to v1.1, ADR-005 §4/D-016): healthStatus stays honestly Unknown for every target and success is never fabricated. Rollout of built-in first-batch kinds is observed from native workload status (ADR-006); all other classes stay honestly Unknown."
	if policyRefs > 0 {
		reason = ReasonStatusPolicyEvaluatorDeferred
		message = fmt.Sprintf("%d target(s) declare a statusPolicyRef: v1 froze the Status Policy storage and I/O contract but defers the CUE evaluator to v1.1 (ADR-005 §4/D-016), so healthStatus stays honestly Unknown and success is never fabricated. Rollout of built-in first-batch kinds is still observed from native workload status (ADR-006).", policyRefs)
	}
	desired := metav1.Condition{
		Type:               ConditionTypeStatusPolicyEvaluated,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	}
	previous := findConditionValue(computed.Conditions, ConditionTypeStatusPolicyEvaluated)
	meta.SetStatusCondition(&computed.Conditions, desired)
	if !previous.found || previous.status != desired.Status {
		if cond := meta.FindStatusCondition(computed.Conditions, ConditionTypeStatusPolicyEvaluated); cond != nil {
			cond.LastTransitionTime = metav1.NewTime(now)
		}
	}
}

// emitObserverEffects writes the dual-namespace rollout-outcome Events (full
// audit: source, old/new image, target, per-stage results) and counts the
// outcome metrics. Called ONLY after a status write landed. The target-side
// copy re-Gets the workload uncached first (T4 duplicate/stale-Event lesson).
func (r *SubscriptionObserverReconciler) emitObserverEffects(ctx context.Context, sub *imageflowv1alpha1.ImageSubscription, transition observerTransition) {
	for _, ev := range transition.events {
		r.recorder.Eventf(sub, ev.eventType, ev.reason, "%s", ev.message)
		if obj := r.freshTargetObject(ctx, ev.ref); obj != nil {
			r.recorder.Eventf(obj, ev.eventType, ev.reason, "%s", ev.message)
		} else {
			log.FromContext(ctx).V(1).Info("observer rollout-outcome event skipped on the target side: uncached re-get failed",
				"target", authz.FormatTarget(ev.ref))
		}
	}
	for _, s := range transition.rolloutSamples {
		r.metrics.rolloutOutcomes.WithLabelValues(s.kind, s.result).Inc()
	}
	for _, h := range transition.healthSamples {
		r.metrics.healthOutcomes.WithLabelValues(h).Inc()
	}
}

func (r *SubscriptionObserverReconciler) freshTargetObject(ctx context.Context, ref imageflowv1alpha1.TargetReference) client.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind))
	key := types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}
	if err := r.reader().Get(ctx, key, obj); err != nil {
		if !apierrors.IsNotFound(err) {
			log.FromContext(ctx).V(1).Info("observer uncached target re-get failed", "target", authz.FormatTarget(ref), "error", err)
		}
		return nil
	}
	return obj
}

// observerAuditMessage renders the full per-delivery audit required on
// rollout-outcome Events (ticket AC6): source, old/new image, target and all
// four stage results — with the standing note that health is never derived
// from rollout success (ADR-005 §3).
func observerAuditMessage(sub *imageflowv1alpha1.ImageSubscription, entry imageflowv1alpha1.TargetDeliveryStatus, rec *imageflowv1alpha1.DeliveryResult, to imageflowv1alpha1.RolloutStatus) string {
	oldImage := auditImageNoneRecorded
	newImage := entry.DesiredImage
	if rec != nil {
		if rec.OldImage != "" {
			oldImage = rec.OldImage
		}
		newImage = rec.NewImage
	}
	resolved := "unresolved"
	if _, digest, err := SplitDigestReference(entry.DesiredImage); err == nil {
		resolved = "digest " + digest
	}
	health := entry.HealthStatus
	if health == "" {
		health = imageflowv1alpha1.HealthUnknown
	}
	return fmt.Sprintf(
		"Rollout observation for %s: source %s %s/%s; old image %s; new image %s; stages — imageResolved: %s, imageApplied: %t, rollout: %s, health: %s (v1 has no status policy evaluator, ADR-005 §4: health is never fabricated and never derived from rollout success).",
		authz.FormatTarget(entry.TargetRef),
		sub.Spec.Source.Kind, sub.Spec.Source.Namespace, sub.Spec.Source.Name,
		oldImage, newImage, resolved, entry.ImageApplied, to, health)
}

// attributedRecord finds the recentDeliveries record describing the image
// currently observed on the entry (newest first), for Event audit rendering.
func attributedRecord(records []imageflowv1alpha1.DeliveryResult, entry imageflowv1alpha1.TargetDeliveryStatus) *imageflowv1alpha1.DeliveryResult {
	for i := range records {
		rec := &records[i]
		if rec.TargetRef != entry.TargetRef {
			continue
		}
		if rec.NewImage == entry.ObservedImage || (entry.ImageApplied && rec.NewImage == entry.DesiredImage) {
			return rec
		}
	}
	return nil
}

func findOutcome(outcomes []observerOutcome, ref imageflowv1alpha1.TargetReference) *observerOutcome {
	for i := range outcomes {
		if outcomes[i].ref == ref {
			return &outcomes[i]
		}
	}
	return nil
}

func ptrTime(t time.Time) *metav1.Time {
	nt := metav1.NewTime(t)
	return &nt
}

// observerSubscriptionPredicate fires on generation changes and on any
// status change of the subscription (the engine's writes are the observer's
// primary trigger; the observer's own writes re-trigger exactly one
// convergence pass that the DeepEqual gate quiesces).
type observerSubscriptionPredicate struct {
	predicate.Funcs
}

func (observerSubscriptionPredicate) CreateFunc(e event.CreateEvent) bool { return true }
func (observerSubscriptionPredicate) DeleteFunc(e event.DeleteEvent) bool { return false }
func (observerSubscriptionPredicate) UpdateFunc(e event.UpdateEvent) bool {
	oldSub, ok1 := e.ObjectOld.(*imageflowv1alpha1.ImageSubscription)
	newSub, ok2 := e.ObjectNew.(*imageflowv1alpha1.ImageSubscription)
	if !ok1 || !ok2 {
		return true
	}
	if oldSub.Generation != newSub.Generation {
		return true
	}
	return !equality.Semantic.DeepEqual(oldSub.Status, newSub.Status)
}

// observerWorkloadStatusPredicate is the STATUS-driven counterpart of the
// engine's generation-only workloadChangePredicate: the observation surface
// is workload status, so status churn is exactly what must fire here (and
// the observer never writes workloads, so no self-loop exists).
type observerWorkloadStatusPredicate struct {
	predicate.Funcs
}

func (observerWorkloadStatusPredicate) CreateFunc(e event.CreateEvent) bool { return true }
func (observerWorkloadStatusPredicate) DeleteFunc(e event.DeleteEvent) bool { return false }
func (observerWorkloadStatusPredicate) UpdateFunc(e event.UpdateEvent) bool {
	if e.ObjectOld == nil || e.ObjectNew == nil {
		return true
	}
	if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
		return true
	}
	switch old := e.ObjectOld.(type) {
	case *appsv1.Deployment:
		newObj, ok := e.ObjectNew.(*appsv1.Deployment)
		return ok && !equality.Semantic.DeepEqual(old.Status, newObj.Status)
	case *appsv1.StatefulSet:
		newObj, ok := e.ObjectNew.(*appsv1.StatefulSet)
		return ok && !equality.Semantic.DeepEqual(old.Status, newObj.Status)
	case *appsv1.DaemonSet:
		newObj, ok := e.ObjectNew.(*appsv1.DaemonSet)
		return ok && !equality.Semantic.DeepEqual(old.Status, newObj.Status)
	}
	return true
}

// subsForWorkload maps a workload event to every subscription targeting it
// (observer-side copy of the engine's mapping; the reconcilers are separate
// types with separate caches of nothing).
func (r *SubscriptionObserverReconciler) subsForWorkload(ctx context.Context, obj client.Object) []ctrl.Request {
	var kind string
	switch obj.(type) {
	case *appsv1.Deployment:
		kind = kindDeployment
	case *appsv1.StatefulSet:
		kind = kindStatefulSet
	case *appsv1.DaemonSet:
		kind = kindDaemonSet
	default:
		return nil
	}
	apiVersion := appsv1.SchemeGroupVersion.String()
	subs, err := r.listSubscriptions(ctx)
	if err != nil {
		return nil
	}
	var requests []ctrl.Request
	for i := range subs {
		sub := &subs[i]
		for j := range sub.Spec.Targets {
			ref := sub.Spec.Targets[j].TargetRef
			if ref.APIVersion == apiVersion && ref.Kind == kind &&
				ref.Namespace == obj.GetNamespace() && ref.Name == obj.GetName() {
				requests = append(requests, ctrl.Request{NamespacedName: types.NamespacedName{
					Namespace: sub.Namespace,
					Name:      sub.Name,
				}})
				break
			}
		}
	}
	return requests
}

func (r *SubscriptionObserverReconciler) listSubscriptions(ctx context.Context) ([]imageflowv1alpha1.ImageSubscription, error) {
	var subs imageflowv1alpha1.ImageSubscriptionList
	if err := r.client.List(ctx, &subs); err != nil {
		log.FromContext(ctx).Error(err, "workload observer could not list ImageSubscriptions for workload mapping")
		return nil, err
	}
	return subs.Items, nil
}

// compile-time interface guards
var (
	_ runtime.Object = &unstructured.Unstructured{}
	_ client.Object  = &imageflowv1alpha1.ImageSubscription{}
)
