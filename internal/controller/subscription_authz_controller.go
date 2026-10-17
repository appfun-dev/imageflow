package controller

import (
	"context"
	"fmt"
	"time"

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
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/webhook"
)

// Subscription authorization controller (ticket T8, ADR-003): the
// controller-side half of the subscription authorization gate. It re-verifies
// the created-by identity of every ImageSubscription against every delivery
// target with SubjectAccessReview (through the cached, rate-limited gate in
// internal/authz) and persists the verdict on the subscription surface:
//
//   - Forbidden condition (True with reason TargetsUnauthorized /
//     MissingIdentity, False with reason Authorized) — the API-object contract
//     the T9 delivery engine consumes: a target whose subscription reports
//     Forbidden=True, or whose per-target status entry carries
//     Reason="Forbidden", must receive ZERO writes;
//   - per-target status.targets[] entries with Reason="Forbidden" for every
//     denied target (authorized targets are left to the delivery engine);
//   - transition Events (TargetsForbidden / TargetsAuthorized /
//     MissingCreatorIdentity) — never silent (spec: 不静默降级).
//
// Verification timings (ADR-003): create admission and targets-change
// admission are enforced by the validating webhook (internal/webhook, T8
// authorizer); this controller owns the periodic re-verification — every
// reconcile re-checks through the gate and requeues at --sar-recheck-period,
// while the gate's cache TTL (same period) bounds SAR call volume to
// ≈ subscriptions × targets × recheck frequency. Permission revocation or
// re-grant therefore flips the persisted state within one recheck period.
//
// Status-surface merge contract with the T9 delivery engine:
//   - this controller only ever writes the Forbidden condition and, inside
//     status.targets[] entries, the Reason/Message fields of entries whose
//     Reason it owns ("Forbidden"); every other per-target field
//     (desiredImage, rollout/health state and their ObservedGeneration/
//     LastTransitionTime bookkeeping, retry state) belongs to the delivery
//     engine and is preserved;
//   - when a target becomes authorized again, a Forbidden entry is cleared;
//     an entry left without any delivery-engine data is dropped entirely;
//   - status.observedGeneration and status.rolloutStatus (worst-state
//     aggregation) are delivery-engine-owned and never touched here.
//
// Fail-closed rules (never fail open, never silently degrade):
//   - missing/malformed created-by annotation → Forbidden=True
//     (MissingIdentity): without a trusted creator identity nothing may be
//     written;
//   - unresolved gate verdicts (rate limited, unmapped target kind, SAR API
//     failure) → reconcile error with backoff requeue; the persisted surface
//     is left untouched until the picture is complete again.

// subscriptionAuthzControllerName is the production controller name (Named()).
// Integration tests must build their instance under a different name:
// controller-runtime validates controller names process-globally.
const subscriptionAuthzControllerName = "subscription-authz"

// recorderNameSubscriptionAuthz is the Event source component name.
const recorderNameSubscriptionAuthz = "imageflow-subscription-authz"

// kindImageSubscription is the ImageFlow kind name used by the
// scheme-recognition guard.
const kindImageSubscription = "ImageSubscription"

// TargetReasonForbidden is the per-target status.targets[].reason value
// written for denied targets. It is part of the frozen T8→T9 contract: the
// delivery engine must not write to a target whose entry carries this reason
// (or whose subscription reports the Forbidden condition True).
const TargetReasonForbidden = "Forbidden"

// Condition reasons of the Forbidden condition (stable, machine-readable;
// additional reason vocabulary on top of the frozen T2 condition types).
const (
	// reasonTargetsUnauthorized is used with Forbidden=True when at least one
	// target failed the SubjectAccessReview check.
	reasonTargetsUnauthorized = "TargetsUnauthorized"
	// reasonMissingIdentity is used with Forbidden=True when the created-by
	// annotation is absent or malformed (fail closed).
	reasonMissingIdentity = "MissingIdentity"
	// reasonAuthorized is used with Forbidden=False when every target passed.
	reasonAuthorized = "Authorized"
)

// Event reasons of the authorization surface (Event.reason vocabulary).
const (
	// EventReasonTargetsForbidden announces a transition into (or a change
	// of) the forbidden state: Warning, lists the denied targets.
	EventReasonTargetsForbidden = "TargetsForbidden"
	// EventReasonTargetsAuthorized announces the recovery transition back to
	// fully authorized: Normal.
	EventReasonTargetsAuthorized = "TargetsAuthorized"
	// EventReasonMissingIdentity announces the fail-closed state of a
	// subscription without a trusted created-by identity: Warning.
	EventReasonMissingIdentity = "MissingCreatorIdentity"
)

func init() {
	Registrars = append(Registrars, subscriptionAuthzRegistrar{})
}

type subscriptionAuthzRegistrar struct{}

func (subscriptionAuthzRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	return RegisterSubscriptionAuthz(mgr, subscriptionAuthzControllerName, opts)
}

// RBAC of the whole manager, regenerated into config/rbac/role.yaml by
// `make manifests`. The gate creates SubjectAccessReviews (the only
// authorization.k8s.io access ImageFlow needs) and writes the subscription
// status subresource; workload writes belong to the delivery engine (T9) and
// are deliberately absent here — this controller performs ZERO writes to
// target objects by construction.
//
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagesubscriptions,verbs=get;list;watch
// +kubebuilder:rbac:groups=imageflow.appfun.dev,resources=imagesubscriptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create

// RegisterSubscriptionAuthz wires the subscription authorization controller
// onto mgr under the given controller name (parameterized because
// controller-runtime enforces process-global controller-name uniqueness; the
// integration suite runs its instance under a distinct name). The manager's
// RESTMapper resolves target kinds for the SAR resource attributes and the
// manager's client creates the SubjectAccessReviews.
func RegisterSubscriptionAuthz(mgr ctrl.Manager, name string, opts Options) error {
	// Scheme-recognition guard (platform pattern): builder.Complete resolves
	// watched GVKs against the manager scheme at registration time, so a
	// manager without the imageflow kinds (shared manager_test.go environment)
	// must skip loudly instead of aborting the whole process.
	if missing := missingImageFlowKinds(mgr, kindImageSubscription); len(missing) > 0 {
		logSkippedRegistration(name, missing)
		return nil
	}

	timeSource := opts.Clock
	if timeSource == nil {
		timeSource = clock.Real{}
	}
	gate := authz.NewGate(mgr.GetClient(), mgr.GetRESTMapper(), authz.Options{
		Clock: timeSource,
		// The cache TTL equals the recheck period so a cached verdict can
		// never outlive one periodic re-verification (<=0 → gate default).
		TTL: opts.Config.SARRecheckPeriod,
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&imageflowv1alpha1.ImageSubscription{}).
		Complete(NewSubscriptionAuthzReconciler(
			mgr.GetClient(),
			mgr.GetEventRecorderFor(recorderNameSubscriptionAuthz),
			timeSource,
			opts.Config,
			gate,
		))
}

// SubscriptionAuthzReconciler re-verifies subscription authorization and
// persists the Forbidden surface. It performs no writes to workload targets —
// that guarantee is structural: the reconciler's only write surfaces are the
// subscription status subresource and Events.
type SubscriptionAuthzReconciler struct {
	client   client.Client
	recorder record.EventRecorder
	clock    clock.Clock
	config   config.Config
	gate     *authz.Gate
}

// NewSubscriptionAuthzReconciler builds the reconciler. gate must be non-nil
// (the registrar and tests construct it explicitly so the SAR client, REST
// mapper, clock seam and cache bounds stay injectable).
func NewSubscriptionAuthzReconciler(
	cl client.Client,
	recorder record.EventRecorder,
	clk clock.Clock,
	cfg config.Config,
	gate *authz.Gate,
) *SubscriptionAuthzReconciler {
	if clk == nil {
		clk = clock.Real{}
	}
	return &SubscriptionAuthzReconciler{
		client:   cl,
		recorder: recorder,
		clock:    clk,
		config:   cfg,
		gate:     gate,
	}
}

// Reconcile runs one authorization pass over a single subscription. Every
// pass ends with RequeueAfter = --sar-recheck-period: that periodic requeue
// is the timing that flips the persisted state after RBAC changes (which
// generate no watch events on the subscription itself).
func (r *SubscriptionAuthzReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	requeue := ctrl.Result{RequeueAfter: r.recheckPeriod()}

	var sub imageflowv1alpha1.ImageSubscription
	if err := r.client.Get(ctx, req.NamespacedName, &sub); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get subscription: %w", err)
	}
	if r.gate == nil {
		return ctrl.Result{}, fmt.Errorf("authorization gate is not configured")
	}

	outcome := r.evaluate(ctx, &sub)
	if len(outcome.unresolved) > 0 {
		// Incomplete verdict picture (rate limited / unmapped kind / SAR API
		// failure): keep the persisted surface as-is and retry with backoff —
		// never fail open, never write a half-truth.
		first := outcome.unresolved[0]
		return ctrl.Result{}, fmt.Errorf(
			"authorization unresolved for %d of %d targets of %s (first: %s: %s)",
			len(outcome.unresolved), outcome.targetCount, req.NamespacedName,
			authz.FormatTarget(first.Target), first.Reason)
	}

	transition, err := r.syncAuthorizationStatus(ctx, req.NamespacedName, outcome)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("sync authorization status: %w", err)
	}
	r.emitTransitionEvents(&sub, transition)
	logger.V(1).Info("subscription authorization re-verified",
		"targets", outcome.targetCount,
		"denied", len(outcome.denied),
		"conditionChanged", transition.wrote)
	return requeue, nil
}

// recheckPeriod is the effective periodic re-verification interval
// (ADR-003 default when the flag/config is unset, e.g. zero-value test
// configs).
func (r *SubscriptionAuthzReconciler) recheckPeriod() time.Duration {
	if period := r.config.SARRecheckPeriod; period > 0 {
		return period
	}
	return authz.DefaultRecheckPeriod
}

// authorizationOutcome is the verdict picture of one reconcile pass.
type authorizationOutcome struct {
	// creator is the raw created-by annotation value (user-facing identity
	// display in condition messages).
	creator string
	// identityErr is set when the created-by annotation is absent or
	// malformed (fail closed: Forbidden=True/MissingIdentity).
	identityErr error
	// denied lists definitively unauthorized targets.
	denied []authz.TargetVerdict
	// unresolved lists targets without a definitive verdict; any entry
	// aborts the pass with a backoff requeue.
	unresolved []authz.TargetVerdict
	// targetCount is the number of spec targets checked.
	targetCount int
}

// evaluate resolves the creator identity and runs the gate over every target.
func (r *SubscriptionAuthzReconciler) evaluate(
	ctx context.Context,
	sub *imageflowv1alpha1.ImageSubscription,
) authorizationOutcome {
	outcome := authorizationOutcome{
		creator:     sub.GetAnnotations()[webhook.CreatedByAnnotation],
		targetCount: len(sub.Spec.Targets),
	}
	subject, err := webhook.SARSubject(outcome.creator)
	if err != nil {
		outcome.identityErr = err
		return outcome
	}
	refs := make([]imageflowv1alpha1.TargetReference, 0, len(sub.Spec.Targets))
	for i := range sub.Spec.Targets {
		refs = append(refs, sub.Spec.Targets[i].TargetRef)
	}
	for _, verdict := range r.gate.CheckTargets(ctx, subject, refs) {
		switch verdict.Verdict {
		case authz.VerdictDenied:
			outcome.denied = append(outcome.denied, verdict)
		case authz.VerdictUnknown:
			outcome.unresolved = append(outcome.unresolved, verdict)
		}
	}
	return outcome
}

// desiredCondition renders the Forbidden condition for one outcome.
func (o authorizationOutcome) desiredCondition(generation int64) metav1.Condition {
	base := metav1.Condition{
		Type:               imageflowv1alpha1.ConditionTypeForbidden,
		ObservedGeneration: generation,
	}
	switch {
	case o.identityErr != nil:
		base.Status = metav1.ConditionTrue
		base.Reason = reasonMissingIdentity
		base.Message = authz.TruncateMessage(fmt.Sprintf(
			"No trusted creator identity: %v. ImageFlow performs zero writes to any target "+
				"until the %s annotation records a verifiable creator (ADR-003; the mutating "+
				"webhook injects it at admission).",
			o.identityErr, webhook.CreatedByAnnotation), 1024)
	case len(o.denied) > 0:
		base.Status = metav1.ConditionTrue
		base.Reason = reasonTargetsUnauthorized
		base.Message = authz.TruncateMessage(fmt.Sprintf(
			"Creator %q lacks update permission on %d of %d target(s): %s. Zero writes are "+
				"performed for these targets (ADR-003); grant standard RBAC update in the "+
				"target namespace(s) and the next re-check flips this state.",
			o.creator, len(o.denied), o.targetCount, authz.FormatDeniedTargets(o.denied)), 2048)
	default:
		base.Status = metav1.ConditionFalse
		base.Reason = reasonAuthorized
		base.Message = fmt.Sprintf(
			"Creator %q is authorized to update all %d target(s) (SubjectAccessReview gate, ADR-003).",
			o.creator, o.targetCount)
	}
	return base
}

// authzStatusTransition records what one status sync changed, for event
// emission. Everything is derived from the persisted surface, so events
// survive controller restarts without in-memory state.
type authzStatusTransition struct {
	wrote            bool
	nowForbidden     bool
	becameAuthorized bool
	identity         bool
	message          string
}

// syncAuthorizationStatus persists the Forbidden condition and the per-target
// Forbidden entries with retry-on-conflict, skipping the write entirely when
// the surface already agrees (no write amplification on the periodic cadence:
// a steady-state re-check must not touch the object).
func (r *SubscriptionAuthzReconciler) syncAuthorizationStatus(
	ctx context.Context,
	key types.NamespacedName,
	outcome authorizationOutcome,
) (authzStatusTransition, error) {
	var transition authzStatusTransition
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var sub imageflowv1alpha1.ImageSubscription
		if err := r.client.Get(ctx, key, &sub); err != nil {
			return err
		}
		now := r.clock.Now()
		desired := outcome.desiredCondition(sub.Generation)
		// Snapshot the previous condition by VALUE: FindStatusCondition
		// returns a pointer into the slice that SetStatusCondition mutates
		// in place, so keeping the pointer would alias the new state.
		var previous metav1.Condition
		hadPrevious := false
		if c := meta.FindStatusCondition(sub.Status.Conditions,
			imageflowv1alpha1.ConditionTypeForbidden); c != nil {
			previous, hadPrevious = *c, true
		}

		targetsChanged := applyForbiddenTargetEntries(&sub.Status.Targets, outcome.denied)
		conditionChanged := forbiddenConditionChanged(hadPrevious, previous, desired)
		if !conditionChanged && !targetsChanged {
			transition = authzStatusTransition{}
			return nil
		}

		meta.SetStatusCondition(&sub.Status.Conditions, desired)
		if !hadPrevious || previous.Status != desired.Status {
			// Pin the transition timestamp to the injected clock (seam S3)
			// exactly like the T4 completion writer; reason/message-only
			// changes keep the original transition time (convention).
			if c := meta.FindStatusCondition(sub.Status.Conditions, desired.Type); c != nil {
				c.LastTransitionTime = metav1.NewTime(now)
			}
		}
		if err := r.client.Status().Update(ctx, &sub); err != nil {
			return err
		}

		wasForbidden := hadPrevious && previous.Status == metav1.ConditionTrue
		nowForbidden := desired.Status == metav1.ConditionTrue
		transition = authzStatusTransition{
			wrote:            true,
			nowForbidden:     nowForbidden,
			becameAuthorized: !nowForbidden && wasForbidden,
			identity:         nowForbidden && desired.Reason == reasonMissingIdentity,
			message:          desired.Message,
		}
		return nil
	})
	if err != nil {
		return authzStatusTransition{}, err
	}
	return transition, nil
}

// forbiddenConditionChanged reports whether the persisted condition differs
// from the desired one in any user-visible field.
func forbiddenConditionChanged(hadPrevious bool, previous, desired metav1.Condition) bool {
	if !hadPrevious {
		return true
	}
	return previous.Status != desired.Status ||
		previous.Reason != desired.Reason ||
		previous.Message != desired.Message ||
		previous.ObservedGeneration != desired.ObservedGeneration
}

// applyForbiddenTargetEntries upserts Reason="Forbidden" entries for denied
// targets and clears entries of re-authorized targets, preserving every
// delivery-engine-owned field (merge contract in the file header). It reports
// whether the entry list changed.
func applyForbiddenTargetEntries(
	entries *[]imageflowv1alpha1.TargetDeliveryStatus,
	denied []authz.TargetVerdict,
) bool {
	changed := false
	stillDenied := make(map[imageflowv1alpha1.TargetReference]bool, len(denied))
	for _, verdict := range denied {
		stillDenied[verdict.Target] = true
		message := authz.TruncateMessage(verdict.Reason, authz.MaxTargetReasonLength)
		if idx := findTargetEntry(*entries, verdict.Target); idx >= 0 {
			entry := &(*entries)[idx]
			if entry.Reason == TargetReasonForbidden && entry.Message == message {
				continue
			}
			entry.Reason = TargetReasonForbidden
			entry.Message = message
			changed = true
			continue
		}
		*entries = append(*entries, imageflowv1alpha1.TargetDeliveryStatus{
			TargetRef: verdict.Target,
			Reason:    TargetReasonForbidden,
			Message:   message,
		})
		changed = true
	}

	kept := make([]imageflowv1alpha1.TargetDeliveryStatus, 0, len(*entries))
	for _, entry := range *entries {
		if entry.Reason == TargetReasonForbidden && !stillDenied[entry.TargetRef] {
			changed = true
			entry.Reason = ""
			entry.Message = ""
			if isForbiddenHusk(entry) {
				// Nothing but the target reference remains (the delivery
				// engine never touched this entry): drop it entirely so it
				// owns the entry lifecycle from here on.
				continue
			}
		}
		kept = append(kept, entry)
	}
	if changed {
		*entries = kept
	}
	return changed
}

// findTargetEntry locates the status entry of a target by reference equality.
func findTargetEntry(
	entries []imageflowv1alpha1.TargetDeliveryStatus,
	ref imageflowv1alpha1.TargetReference,
) int {
	for i := range entries {
		if entries[i].TargetRef == ref {
			return i
		}
	}
	return -1
}

// isForbiddenHusk reports whether a cleared entry carries nothing beyond its
// target reference: no delivery-engine data (images, rollout/health, retry
// state) and no delivery-engine bookkeeping. Such husks were created purely
// by this controller and are dropped on recovery.
func isForbiddenHusk(entry imageflowv1alpha1.TargetDeliveryStatus) bool {
	return entry.DesiredImage == "" && entry.ObservedImage == "" && !entry.ImageApplied &&
		entry.RolloutStatus == "" && entry.HealthStatus == "" &&
		entry.ObservedGeneration == 0 && entry.RetryCount == 0 &&
		entry.LastTransitionTime == nil && entry.NextRetryTime == nil
}

// emitTransitionEvents writes the audit Events for one status transition
// (Warning into/within the forbidden state, Normal on recovery). Steady-state
// re-checks wrote nothing and emit nothing.
func (r *SubscriptionAuthzReconciler) emitTransitionEvents(
	sub *imageflowv1alpha1.ImageSubscription,
	t authzStatusTransition,
) {
	if !t.wrote {
		return
	}
	switch {
	case t.identity:
		// Forbidden=True with reason MissingIdentity (fail closed).
		r.recorder.Event(sub, corev1.EventTypeWarning, EventReasonMissingIdentity, t.message)
	case t.nowForbidden:
		// Both the False->True flip and True->True content changes (a newly
		// denied target) are Warning-worthy; steady state wrote nothing.
		r.recorder.Event(sub, corev1.EventTypeWarning, EventReasonTargetsForbidden, t.message)
	case t.becameAuthorized:
		r.recorder.Event(sub, corev1.EventTypeNormal, EventReasonTargetsAuthorized, t.message)
	}
}
