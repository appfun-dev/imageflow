package controller

// Delivery status merge (ticket T9): the single status-subresource writer of
// the delivery engine. Everything user-visible (per-target entries, the
// bounded recentDeliveries window, the T9-owned conditions, the worst-state
// aggregate) is recomputed from persisted state on every pass and written
// only when it differs — steady-state reconciles perform ZERO status writes
// and emit ZERO events.
//
// Merge discipline (frozen T8 contract, unit-test pinned on both sides):
//   - entries whose persisted Reason is "Forbidden" belong to T8 and are
//     preserved byte-for-byte (Reason/Message AND every delivery field);
//   - for every other entry this engine owns desiredImage, observedImage,
//     imageApplied, rolloutStatus, healthStatus, retryCount, nextRetryTime,
//     observedGeneration and lastTransitionTime; Reason/Message of entries it
//     owns are written freely, EXCEPT the T11 observer-owned rollout
//     explanations (Reason Rollout*): a steady-state Applied opinion with no
//     delivery consequence preserves them (preserveObserverReason) so the
//     per-target "why" of the persisted rollout state survives;
//   - persisted rollout/health values are PRESERVED unless this pass has a
//     delivery consequence (Progressing seed on a value-changing apply,
//     terminal Conflict) or the field is empty (initialization to Unknown —
//     T11 owns every observation-driven transition);
//   - status.observedGeneration, status.rolloutStatus (worst-state
//     aggregate) and recentDeliveries are written here and never by T8;
//   - stale entries of targets removed from the spec are pruned, except
//     Forbidden ones (T8 lifecycle).
//
// Event/record dedup survives cache lag and restarts because every plan item
// is gated on the FRESH read inside the retry-on-conflict closure: reason
// transitions compare against the persisted reason, delivery records against
// the persisted convergence state, and value-changing applies are already
// idempotent through the fresh pre-apply read (equal-and-owned skips the
// apply entirely).

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
	"github.com/appfun-dev/imageflow/internal/config"
)

// plannedEvent is one Event to emit after the status write succeeded.
type plannedEvent struct {
	eventType string
	reason    string
	message   string
	// targetSide duplicates the Event onto the workload object in the target
	// namespace (ADR-003 §4 dual-namespace audit of every delivery).
	targetSide   bool
	targetObject runtime.Object
}

// deliveryTransition is everything one status sync pass wants to say after
// the write succeeded.
type deliveryTransition struct {
	wrote          bool
	deliveredCount int
	events         []plannedEvent
	latencies      []time.Duration
	failureReasons []string
	truncated      int
}

// syncDeliveryStatus recomputes the T9-owned status surface from a fresh read
// and writes it only on change. The closure plans events/records against the
// fresh state so concurrent writers (T8 flips, future T11 observations) can
// never duplicate or drop an effect.
func (r *SubscriptionDeliveryReconciler) syncDeliveryStatus(
	ctx context.Context,
	key types.NamespacedName,
	source sourceState,
	pass deliveryPass,
) (deliveryTransition, error) {
	var transition deliveryTransition
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		transition = deliveryTransition{}
		var fresh imageflowv1alpha1.ImageSubscription
		if err := r.client.Get(ctx, key, &fresh); err != nil {
			return err
		}
		now := r.clock.Now()
		computed := *fresh.Status.DeepCopy()

		mergedTargets, records := r.mergeTargetEntries(&fresh, pass.outcomes, source, now, &transition)
		computed.Targets = mergedTargets
		computed.RecentDeliveries = mergeRecentDeliveries(
			fresh.Status.RecentDeliveries, records, deliveryLimit(r.config), pass.outcomes, &transition)
		setDeliveryConditions(&computed, fresh.Generation, pass.outcomes, now)
		computed.RolloutStatus = AggregateRolloutStatus(computed.Targets)
		computed.ObservedGeneration = fresh.Generation

		if equality.Semantic.DeepEqual(fresh.Status, computed) {
			// Idempotency gate: no write amplification, and (because every
			// plan item is transition-gated) no events were planned.
			transition.wrote = false
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
		return deliveryTransition{}, err
	}
	return transition, nil
}

// mergeTargetEntries rebuilds status.targets in spec order and collects the
// delivery records the pass produced.
func (r *SubscriptionDeliveryReconciler) mergeTargetEntries(
	fresh *imageflowv1alpha1.ImageSubscription,
	outcomes []deliveryOutcome,
	source sourceState,
	now time.Time,
	transition *deliveryTransition,
) ([]imageflowv1alpha1.TargetDeliveryStatus, []imageflowv1alpha1.DeliveryResult) {
	merged := make([]imageflowv1alpha1.TargetDeliveryStatus, 0, len(outcomes))
	var records []imageflowv1alpha1.DeliveryResult
	consumed := make(map[int]bool, len(outcomes))

	for i := range outcomes {
		out := &outcomes[i]
		idx := findTargetEntry(fresh.Status.Targets, out.ref)
		if out.preserveEntry {
			// T8-owned Forbidden entry: preserve untouched (frozen contract).
			if idx >= 0 {
				consumed[idx] = true
				merged = append(merged, fresh.Status.Targets[idx])
			}
			continue
		}
		var base imageflowv1alpha1.TargetDeliveryStatus
		if idx >= 0 {
			consumed[idx] = true
			base = fresh.Status.Targets[idx]
		} else {
			base = imageflowv1alpha1.TargetDeliveryStatus{TargetRef: out.ref}
		}
		entry, plan := mergeEntry(fresh.Generation, base, out, source, now)
		merged = append(merged, entry)
		if plan.record != nil {
			records = append(records, *plan.record)
			transition.deliveredCount++
		}
		transition.events = append(transition.events, plan.events...)
		if plan.latency != nil {
			transition.latencies = append(transition.latencies, *plan.latency)
		}
		if plan.failureLabel != "" {
			transition.failureReasons = append(transition.failureReasons, plan.failureLabel)
		}
	}

	// Stale entries of targets no longer in the spec: Forbidden ones belong
	// to T8's lifecycle; every other stale entry is pruned (T9 owns the
	// delivery entry lifecycle).
	for i := range fresh.Status.Targets {
		if consumed[i] {
			continue
		}
		if fresh.Status.Targets[i].Reason == TargetReasonForbidden {
			merged = append(merged, fresh.Status.Targets[i])
		}
	}
	return merged, records
}

// entryPlan carries the record/event/metric decisions of one merged entry.
type entryPlan struct {
	record       *imageflowv1alpha1.DeliveryResult
	events       []plannedEvent
	latency      *time.Duration
	failureLabel string
}

// mergeEntry applies one outcome onto the persisted entry base, honoring the
// field-ownership split (file header), and plans the transition-gated effects.
func mergeEntry(
	generation int64,
	base imageflowv1alpha1.TargetDeliveryStatus,
	out *deliveryOutcome,
	source sourceState,
	now time.Time,
) (imageflowv1alpha1.TargetDeliveryStatus, entryPlan) {
	var plan entryPlan
	merged := *base.DeepCopy()
	merged.TargetRef = out.ref

	if out.setDesired {
		merged.DesiredImage = out.desiredImage
	}
	if out.setObserved {
		merged.ObservedImage = out.observedImage
	}
	if out.setImageApplied {
		merged.ImageApplied = out.imageApplied
	}

	// Rollout/health: preserve persisted observations (T11's lane) except for
	// delivery consequences; initialize empty fields to Unknown (never
	// defaulted to success, ADR-005 §5).
	stateChanged := false
	if out.rolloutSet != nil {
		if merged.RolloutStatus != *out.rolloutSet {
			merged.RolloutStatus = *out.rolloutSet
			stateChanged = true
		}
	} else if merged.RolloutStatus == "" {
		merged.RolloutStatus = imageflowv1alpha1.RolloutUnknown
		stateChanged = true
	}
	if merged.HealthStatus == "" {
		merged.HealthStatus = imageflowv1alpha1.HealthUnknown
		stateChanged = true
	}
	if stateChanged {
		transitionTime := metav1.NewTime(now)
		merged.LastTransitionTime = &transitionTime
	}

	reasonChanged := base.Reason != out.reason
	// T11 preservation (see file header): when this pass has no delivery
	// consequence and its opinion is the steady-state Applied one, an
	// observer-owned rollout explanation stays the per-target Reason/Message
	// until the next delivery, failure or observer transition rewrites it.
	if preserveObserverReason(base, out) {
		reasonChanged = false
	} else {
		merged.Reason = out.reason
		merged.Message = out.message
	}
	merged.ObservedGeneration = generation

	switch {
	case out.resetRetry:
		merged.RetryCount = 0
		merged.NextRetryTime = nil
	case out.retryDelta > 0:
		merged.RetryCount = base.RetryCount + out.retryDelta
		nextRetry := metav1.NewTime(now.Add(retryBackoffFor(merged.RetryCount)))
		merged.NextRetryTime = &nextRetry
	}

	planEffects(&plan, base, out, source, now, reasonChanged)
	return merged, plan
}

// preserveObserverReason reports whether the persisted entry carries a T11
// observer-owned rollout explanation (RolloutProgressing/RolloutSucceeded/
// RolloutFailed/RolloutUnknown) that must survive this pass: the pass forced
// no rollout value itself (out.rolloutSet == nil) and its opinion is the
// steady-state Applied one. Delivery consequences (Progressing seed,
// Conflict, any failure reason) still overwrite it — the engine's WHY wins
// while a delivery is failing or landing.
func preserveObserverReason(base imageflowv1alpha1.TargetDeliveryStatus, out *deliveryOutcome) bool {
	return out.rolloutSet == nil && out.reason == TargetReasonApplied && isObserverOwnedReason(base.Reason)
}

// planEffects gates delivery records, Events and latency observations on the
// persisted base state (fresh-read dedup, file header).
func planEffects(
	plan *entryPlan,
	base imageflowv1alpha1.TargetDeliveryStatus,
	out *deliveryOutcome,
	source sourceState,
	now time.Time,
	reasonChanged bool,
) {
	// An observer-owned rollout explanation (T11) counts as converged too:
	// desired+applied match and the reason surface simply carries the
	// observer's WHY — emitting a confirmation record here would spam the
	// bounded window on every steady-state pass.
	freshConverged := base.DesiredImage == source.ref &&
		base.ImageApplied &&
		(base.Reason == TargetReasonApplied || isObserverOwnedReason(base.Reason))

	switch {
	case out.delivered:
		// THE delivery: a value-changing apply landed this pass. Always
		// recorded (fresh-read evidence makes duplicates impossible: the next
		// pass reads equal-and-owned and skips the apply).
		plan.record = newDeliveryRecord(source, out, base.DesiredImage, now)
		plan.events = append(plan.events, imageAppliedEvent(out, source, true))
		if source.created != nil {
			latency := now.Sub(source.created.Time)
			if latency < 0 {
				latency = 0
			}
			plan.latency = &latency
		}
	case out.converged && !freshConverged:
		// Ownership takeover (orphan/user field already at the digest) or
		// crash recovery before the status write: the subscription now
		// guarantees the field — record it once (gate: persisted entry not
		// converged yet).
		plan.record = newDeliveryRecord(source, out, base.DesiredImage, now)
		plan.events = append(plan.events, imageAppliedEvent(out, source, false))
	case out.conflictNow && base.Reason != TargetReasonConflict:
		// Terminal Conflict transition (ADR-008): recorded with the Conflict
		// outcome; zero writes were performed.
		record := newDeliveryRecord(source, out, base.DesiredImage, now)
		record.ImageApplied = out.imageApplied
		record.RolloutResult = imageflowv1alpha1.RolloutConflict
		record.Reason = TargetReasonConflict
		plan.record = record
	}

	if reasonChanged {
		if label, isFailure := deliveryFailureLabel(out.reason); isFailure {
			eventReason := out.reason
			switch out.reason {
			case TargetReasonUnsupported:
				eventReason = EventReasonUnsupportedTarget
			case TargetReasonConflict:
				eventReason = EventReasonDeliveryConflict
			}
			plan.events = append(plan.events, plannedEvent{
				eventType: corev1.EventTypeWarning,
				reason:    eventReason,
				message:   authz.TruncateMessage(out.message, 1024),
			})
			plan.failureLabel = label
		}
	}
}

// newDeliveryRecord builds the §12.9-shaped recentDeliveries entry.
func newDeliveryRecord(
	source sourceState,
	out *deliveryOutcome,
	oldImage string,
	now time.Time,
) *imageflowv1alpha1.DeliveryResult {
	return &imageflowv1alpha1.DeliveryResult{
		Source:        source.spec,
		OldImage:      oldImage,
		NewImage:      source.ref,
		TargetRef:     out.ref,
		ImageApplied:  true,
		RolloutResult: imageflowv1alpha1.RolloutProgressing,
		HealthResult:  imageflowv1alpha1.HealthUnknown,
		Reason:        TargetReasonApplied,
		ObservedAt:    metav1.NewTime(now),
	}
}

// imageAppliedEvent renders the dual-namespace ImageApplied Event of one
// delivery (valueChanged) or ownership confirmation.
func imageAppliedEvent(out *deliveryOutcome, source sourceState, valueChanged bool) plannedEvent {
	previous := "<none>"
	if out.previousValue != "" {
		previous = out.previousValue
	}
	message := fmt.Sprintf(
		"Image change delivered: %s of %s now reference %s (previous field value: %s); "+
			"source %s, rollout observation is independent of this field write (ADR-005 §3)",
		out.locator, authz.FormatTarget(out.ref), source.ref, previous, source.name)
	if !valueChanged {
		message = fmt.Sprintf(
			"Delivery confirmed: %s of %s already reference %s; this subscription claimed the "+
				"field ownership via SSA (exclusivity registry, ADR-008); source %s",
			out.locator, authz.FormatTarget(out.ref), source.ref, source.name)
	}
	return plannedEvent{
		eventType:    corev1.EventTypeNormal,
		reason:       EventReasonImageApplied,
		message:      authz.TruncateMessage(message, 1024),
		targetSide:   true,
		targetObject: out.targetObject,
	}
}

// mergeRecentDeliveries prepends new records (newest first), marks same-target
// in-flight records Superseded (latest-wins, ADR-008) and clamps the window to
// the bounded limit; dropped entries are counted for the truncation Event and
// metric (ADR-008: overflow falls to kube Events and the audit log).
func mergeRecentDeliveries(
	existing []imageflowv1alpha1.DeliveryResult,
	records []imageflowv1alpha1.DeliveryResult,
	limit int,
	outcomes []deliveryOutcome,
	transition *deliveryTransition,
) []imageflowv1alpha1.DeliveryResult {
	if len(records) == 0 {
		return existing
	}
	merged := make([]imageflowv1alpha1.DeliveryResult, 0, len(existing)+len(records))
	merged = append(merged, existing...)
	for _, record := range records {
		for i := range merged {
			entry := &merged[i]
			if entry.TargetRef == record.TargetRef && entry.NewImage != record.NewImage &&
				(entry.RolloutResult == "" || entry.RolloutResult == imageflowv1alpha1.RolloutProgressing) {
				// The in-flight delivery of this target is superseded by the
				// new digest: terminal, never retried, never rolled back.
				inFlight := entry.RolloutResult
				if inFlight == "" {
					inFlight = imageflowv1alpha1.RolloutProgressing
				}
				entry.RolloutResult = imageflowv1alpha1.RolloutSuperseded
				// T11 (AC5/AC6): the supersede itself is audited with a
				// dual-namespace Event carrying source, old/new image,
				// target and the per-stage results at supersede time.
				transition.events = append(transition.events,
					supersededEvent(*entry, inFlight, record, outcomes))
			}
		}
		merged = append([]imageflowv1alpha1.DeliveryResult{record}, merged...)
	}
	if len(merged) > limit {
		transition.truncated = len(merged) - limit
		merged = merged[:limit]
	}
	return merged
}

// supersededEvent renders the DeliverySuperseded audit Event planned by
// mergeRecentDeliveries. The target-side copy rides the fresh workload
// object held by this pass's outcome (uncached read before the apply); when
// the outcome carries none (e.g. the engine pass did not re-read the
// target), emitPlannedEffects writes the subscription-side copy only.
func supersededEvent(
	entry imageflowv1alpha1.DeliveryResult,
	inFlight imageflowv1alpha1.RolloutStatus,
	record imageflowv1alpha1.DeliveryResult,
	outcomes []deliveryOutcome,
) plannedEvent {
	message := fmt.Sprintf(
		"Delivery superseded (latest-wins, ADR-008): the in-flight delivery of %s (source %s %s/%s) to %s was replaced by %s before its rollout finished; recorded Superseded — terminal, no retry, no cross-target rollback; the native rollout controller converges the workload on its own. Stages at supersede time — imageApplied: %t, rollout: %s, health: %s.",
		entry.NewImage, entry.Source.Kind, entry.Source.Namespace, entry.Source.Name,
		authz.FormatTarget(entry.TargetRef), record.NewImage,
		entry.ImageApplied, inFlight, healthOrUnknown(entry.HealthResult))
	ev := plannedEvent{
		eventType: corev1.EventTypeNormal,
		reason:    EventReasonDeliverySuperseded,
		message:   authz.TruncateMessage(message, 1024),
	}
	for i := range outcomes {
		if outcomes[i].ref == entry.TargetRef && outcomes[i].targetObject != nil {
			ev.targetSide = true
			ev.targetObject = outcomes[i].targetObject
			break
		}
	}
	return ev
}

// healthOrUnknown renders an unset health result honestly as Unknown.
func healthOrUnknown(health imageflowv1alpha1.HealthStatus) string {
	if health == "" {
		return string(imageflowv1alpha1.HealthUnknown)
	}
	return string(health)
}

// setDeliveryConditions maintains the two T9-owned subscription conditions
// (Conflict, Unsupported) with clock-pinned transitions (seam S3, same
// discipline as the T4/T8 condition writers). The Forbidden condition belongs
// to T8 and is never touched here.
func setDeliveryConditions(
	status *imageflowv1alpha1.ImageSubscriptionStatus,
	generation int64,
	outcomes []deliveryOutcome,
	now time.Time,
) {
	var conflictTargets, unsupportedTargets []string
	for i := range outcomes {
		switch outcomes[i].reason {
		case TargetReasonConflict:
			conflictTargets = append(conflictTargets,
				authz.FormatTarget(outcomes[i].ref))
		case TargetReasonUnsupported:
			unsupportedTargets = append(unsupportedTargets,
				authz.FormatTarget(outcomes[i].ref))
		}
	}

	conflict := metav1.Condition{
		Type:               imageflowv1alpha1.ConditionTypeConflict,
		ObservedGeneration: generation,
	}
	if len(conflictTargets) > 0 {
		conflict.Status = metav1.ConditionTrue
		conflict.Reason = ReasonFieldOwnershipConflict
		conflict.Message = authz.TruncateMessage(fmt.Sprintf(
			"First-wins field conflict on %d target(s): %s. Another live subscription owns the "+
				"declared image field(s) (managedFields registry, ADR-008); ZERO writes were "+
				"performed. Reclaim by deleting or editing the occupying subscription.",
			len(conflictTargets), boundedList(conflictTargets)), 2048)
	} else {
		conflict.Status = metav1.ConditionFalse
		conflict.Reason = ReasonNoConflicts
		conflict.Message = "No field-ownership conflicts: every declared image field is " +
			"writable by this subscription under the ADR-008 first-wins registry."
	}

	unsupported := metav1.Condition{
		Type:               imageflowv1alpha1.ConditionTypeUnsupported,
		ObservedGeneration: generation,
	}
	if len(unsupportedTargets) > 0 {
		unsupported.Status = metav1.ConditionTrue
		unsupported.Reason = ReasonUnsupportedTargets
		unsupported.Message = authz.TruncateMessage(fmt.Sprintf(
			"%d target(s) are outside the supported adapter set: %s. The built-in adapters serve "+
				"Deployment/StatefulSet/DaemonSet by containerNames (ADR-006); third-party kinds "+
				"require imageFields plus the generic resolver. Extending the built-in set "+
				"requires a new ADR.", len(unsupportedTargets), boundedList(unsupportedTargets)), 2048)
	} else {
		unsupported.Status = metav1.ConditionFalse
		unsupported.Reason = ReasonAllTargetsSupported
		unsupported.Message = "Every target kind is served by a registered delivery adapter."
	}

	for _, desired := range []metav1.Condition{conflict, unsupported} {
		// Snapshot by VALUE: FindStatusCondition returns a pointer into the
		// slice that SetStatusCondition mutates in place (the T8 lesson).
		previous := findConditionValue(status.Conditions, desired.Type)
		meta.SetStatusCondition(&status.Conditions, desired)
		if !previous.found || previous.status != desired.Status {
			// Pin the transition timestamp to the injected clock (seam S3);
			// reason/message-only changes keep the original transition time.
			if c := meta.FindStatusCondition(status.Conditions, desired.Type); c != nil {
				c.LastTransitionTime = metav1.NewTime(now)
			}
		}
	}
}

// conditionSnapshot is a by-value snapshot of a persisted condition (pointers
// into the slice alias SetStatusCondition's in-place mutations — the T8
// lesson).
type conditionSnapshot struct {
	found  bool
	status metav1.ConditionStatus
}

func findConditionValue(conditions []metav1.Condition, condType string) conditionSnapshot {
	if c := meta.FindStatusCondition(conditions, condType); c != nil {
		return conditionSnapshot{found: true, status: c.Status}
	}
	return conditionSnapshot{}
}

// boundedList renders at most authz.MaxListedDeniedTargets entries and
// summarizes the remainder (shared bound with the T8 denied-target lists).
func boundedList(refs []string) string {
	if len(refs) <= authz.MaxListedDeniedTargets {
		return strings.Join(refs, "; ")
	}
	return strings.Join(refs[:authz.MaxListedDeniedTargets], "; ") +
		fmt.Sprintf("; and %d more", len(refs)-authz.MaxListedDeniedTargets)
}

// AggregateRolloutStatus computes the subscription-level worst-state
// aggregate (ADR-008: Failed > Unknown > Progressing > Succeeded), extended
// deterministically for the ADR-008 terminal states:
//
//   - Conflict ranks directly below Failed: a definite non-success terminal
//     state that needs user action (reclaim the field);
//   - Superseded ranks between Progressing and Succeeded: a superseding
//     delivery is in flight, so the aggregate must not claim success;
//   - an empty or unrecognized per-target state ranks as Unknown (never
//     defaulted to success, ADR-005 §5).
//
// An empty target list aggregates to the empty value (nothing reported yet).
// T11 (rollout observer) MUST reuse this function when it writes the
// aggregate, so both writers compute one consistent value.
func AggregateRolloutStatus(
	targets []imageflowv1alpha1.TargetDeliveryStatus,
) imageflowv1alpha1.RolloutStatus {
	if len(targets) == 0 {
		return ""
	}
	var hasFailed, hasConflict, hasUnknown, hasProgressing, hasSuperseded, hasSucceeded bool
	for i := range targets {
		switch targets[i].RolloutStatus {
		case imageflowv1alpha1.RolloutFailed:
			hasFailed = true
		case imageflowv1alpha1.RolloutConflict:
			hasConflict = true
		case imageflowv1alpha1.RolloutProgressing:
			hasProgressing = true
		case imageflowv1alpha1.RolloutSuperseded:
			hasSuperseded = true
		case imageflowv1alpha1.RolloutSucceeded:
			hasSucceeded = true
		default:
			hasUnknown = true
		}
	}
	switch {
	case hasFailed:
		return imageflowv1alpha1.RolloutFailed
	case hasConflict:
		return imageflowv1alpha1.RolloutConflict
	case hasUnknown:
		return imageflowv1alpha1.RolloutUnknown
	case hasProgressing:
		return imageflowv1alpha1.RolloutProgressing
	case hasSuperseded:
		return imageflowv1alpha1.RolloutSuperseded
	case hasSucceeded:
		return imageflowv1alpha1.RolloutSucceeded
	}
	return ""
}

// deliveryLimit is the effective recentDeliveries retention: the
// --subscription-delivery-limit flag clamped into
// [1, SubscriptionDeliveryMaxItems]. A flag above the CRD cap would make the
// API server reject the status write (structural etcd guard, T2), so the
// controller never exceeds the cap regardless of configuration.
func deliveryLimit(cfg config.Config) int {
	limit := cfg.SubscriptionDeliveryLimit
	if limit <= 0 || limit > imageflowv1alpha1.SubscriptionDeliveryMaxItems {
		return imageflowv1alpha1.SubscriptionDeliveryMaxItems
	}
	return limit
}

// retryBackoffFor renders the persisted nextRetryTime estimate of a transient
// failure: exponential from retryBackoffBase, capped at retryBackoffMax. The
// authoritative retry timing is the workqueue backoff of the returned error;
// the persisted value is the user-visible estimate.
func retryBackoffFor(retryCount int32) time.Duration {
	backoff := retryBackoffBase
	for i := int32(1); i < retryCount; i++ {
		backoff *= 2
		if backoff >= retryBackoffMax {
			return retryBackoffMax
		}
	}
	if backoff > retryBackoffMax {
		return retryBackoffMax
	}
	return backoff
}

// deliveryFailureLabel maps a per-target failure reason to the bounded
// imageflow_delivery_target_failures_total label vocabulary (metrics
// contract); ok is false for non-failure reasons.
func deliveryFailureLabel(reason string) (label string, ok bool) {
	switch reason {
	case TargetReasonTargetNotFound:
		return failureLabelTargetNotFound, true
	case TargetReasonContainerNotFound:
		return failureLabelContainerNotFound, true
	case TargetReasonApplyForbidden:
		return failureLabelApplyForbidden, true
	case TargetReasonApplyFailed:
		return failureLabelApplyError, true
	case TargetReasonUnsupported:
		return failureLabelUnsupported, true
	case TargetReasonSourceUnresolved:
		return failureLabelSourceUnresolved, true
	case TargetReasonInvalidImageReference:
		return failureLabelInvalidImageReference, true
	case TargetReasonInvalidTarget:
		return failureLabelInvalidTarget, true
	case TargetReasonInvalidImageField:
		return failureLabelInvalidImageField, true
	case TargetReasonKindNotInstalled:
		return failureLabelKindNotInstalled, true
	case TargetReasonConflict:
		return failureLabelConflict, true
	}
	return "", false
}

// emitPlannedEffects runs the transition-gated effects after a successful
// status sync: dual-namespace delivery Events, failure transition Events,
// the truncation Event/metric, failure metrics and latency observations.
func (r *SubscriptionDeliveryReconciler) emitPlannedEffects(
	sub *imageflowv1alpha1.ImageSubscription,
	transition deliveryTransition,
) {
	for _, event := range transition.events {
		r.recorder.Eventf(sub, event.eventType, event.reason, "%s", event.message)
		if event.targetSide && event.targetObject != nil {
			r.recorder.Eventf(event.targetObject, event.eventType, event.reason, "%s", event.message)
		}
	}
	for _, label := range transition.failureReasons {
		r.metrics.failures.WithLabelValues(label).Inc()
	}
	for _, latency := range transition.latencies {
		r.metrics.latency.Observe(latency.Seconds())
	}
	if transition.truncated > 0 {
		r.metrics.truncated.Add(float64(transition.truncated))
		r.recorder.Eventf(sub, corev1.EventTypeNormal, EventReasonDeliveryHistoryTruncated,
			"recentDeliveries is bounded to %d entries (--subscription-delivery-limit, CRD cap %d, "+
				"ADR-008); %d older entries left the window — full history lives in kube Events and "+
				"the cluster audit log",
			deliveryLimit(r.config), imageflowv1alpha1.SubscriptionDeliveryMaxItems,
			transition.truncated)
	}
}
