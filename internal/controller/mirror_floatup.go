package controller

// Mirror status float-up for delivery results (T11, ADR-008 §4).
//
// Every delivery recorded on ImageSubscription.status.recentDeliveries is
// summarized onto the two mirror objects related to its source:
//
//   - the ImageStreamTag mirror of <stream>:<tag> (all records of this
//     subscription), and
//   - the ImageTag mirror of each distinct delivered digest (only the
//     records that landed that digest).
//
// The summary entries are MirrorStatusEvent{Type: Delivery} written through
// the status subresource with the same discipline as the T5 synthesis loop:
// union-merge via mergeMirrorEvents (which preserves ImageChanged entries
// and other subscriptions' Delivery entries), deterministic newest-first
// sort, bound to --mirror-event-limit (CRD cap 5), a semantic DeepEqual
// no-write gate, and retry-on-conflict. Because a record's Result/Reason/
// Message evolve while the delivery is in flight (the engine seeds
// Progressing, this observer heals it to Succeeded/Failed, latest-wins
// supersedes it) and mirrorEventKey includes those fields, an update is
// rendered as remove-then-insert on the delivery's stable identity
// (subscription, target, old digest, new digest): stored Delivery entries
// with the same identity are dropped before the fresh render is merged in.
//
// Overflow behavior mirrors T5's truncation contract: dropped entries are
// counted on the existing imageflow_mirror_events_truncated_total{kind}
// collector (the same fact: entries left the bounded mirror window) and
// announced with an EventSummaryTruncated Event on the mirror object — the
// full audit always lives in the dual-namespace kube Events (AC6) and the
// cluster audit log.
//
// Ownership: mirror SPECS are CEL-immutable and synthesis never writes
// Delivery entries, so this writer is the only producer of Delivery
// summaries; T5's mergeMirrorEvents(stored, ...) union preserves them on
// every synthesis pass.

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/authz"
)

// mirrorEventReasonSummaryTruncated is the shared Event reason of the T5
// synthesis loop and this float-up writer: both announce the same fact —
// summaries left the bounded mirror window (ADR-008 §4).
const mirrorEventReasonSummaryTruncated = "EventSummaryTruncated"

// floatUpDeliveries renders the subscription's persisted delivery window
// onto its related mirror objects. It re-reads the subscription uncached so
// the float reflects the just-written status even before the informer
// converges, and it is strictly best-effort: a missing mirror object is a
// silent skip (the catalog may not have synthesized it yet) and any error is
// logged — the next observer pass retries (status is the source of truth,
// mirrors are eventually-consistent summaries).
func (r *SubscriptionObserverReconciler) floatUpDeliveries(ctx context.Context, key types.NamespacedName) {
	var sub imageflowv1alpha1.ImageSubscription
	if err := r.reader().Get(ctx, key, &sub); err != nil {
		if !apierrors.IsNotFound(err) {
			log.FromContext(ctx).V(1).Info("mirror float-up skipped: subscription re-get failed", "error", err)
		}
		return
	}
	if len(sub.Status.RecentDeliveries) == 0 {
		return
	}
	src := sub.Spec.Source
	if src.Kind != mirrorSourceKindImageStreamTag {
		return // v1 admits only ImageStreamTag sources (webhook-enforced)
	}
	stream, tag, ok := splitSubscriptionSourceName(src.Name)
	if !ok {
		return
	}
	rendered := renderFloatEvents(&sub)
	if len(rendered) == 0 {
		return
	}
	r.syncImageStreamTagFloat(ctx, src.Namespace, stream, tag, rendered)

	// ImageTag mirrors are per digest: each receives only the summaries of
	// the deliveries that landed that digest.
	byDigest := map[string][]imageflowv1alpha1.MirrorStatusEvent{}
	var order []string
	for _, ev := range rendered {
		if ev.NewDigest == "" {
			continue
		}
		if _, seen := byDigest[ev.NewDigest]; !seen {
			order = append(order, ev.NewDigest)
		}
		byDigest[ev.NewDigest] = append(byDigest[ev.NewDigest], ev)
	}
	for _, digest := range order {
		r.syncImageTagFloat(ctx, src.Namespace, stream, digest, byDigest[digest])
	}
}

// renderFloatEvents converts the persisted delivery window (newest first)
// into mirror Delivery summaries, keeping the FIRST (newest) render per
// delivery identity so an in-flight record's latest state wins.
func renderFloatEvents(sub *imageflowv1alpha1.ImageSubscription) []imageflowv1alpha1.MirrorStatusEvent {
	rendered := make([]imageflowv1alpha1.MirrorStatusEvent, 0, len(sub.Status.RecentDeliveries))
	seen := map[string]bool{}
	for i := range sub.Status.RecentDeliveries {
		rec := &sub.Status.RecentDeliveries[i]
		ev := renderFloatEvent(sub, rec)
		id := floatIdentity(ev)
		if seen[id] {
			continue
		}
		seen[id] = true
		rendered = append(rendered, ev)
	}
	return rendered
}

func renderFloatEvent(sub *imageflowv1alpha1.ImageSubscription, rec *imageflowv1alpha1.DeliveryResult) imageflowv1alpha1.MirrorStatusEvent {
	oldDigest := ""
	if _, digest, err := SplitDigestReference(rec.OldImage); err == nil {
		oldDigest = digest
	}
	newDigest := ""
	if _, digest, err := SplitDigestReference(rec.NewImage); err == nil {
		newDigest = digest
	}
	target := rec.TargetRef.DeepCopy()
	return imageflowv1alpha1.MirrorStatusEvent{
		Type:       imageflowv1alpha1.MirrorEventDelivery,
		Reason:     rec.Reason,
		Message:    authz.TruncateMessage(floatSummaryMessage(sub, rec), 2048),
		ObservedAt: rec.ObservedAt,
		OldDigest:  oldDigest,
		NewDigest:  newDigest,
		Subscription: &imageflowv1alpha1.SubscriptionObjectRef{
			Namespace: sub.Namespace,
			Name:      sub.Name,
		},
		Target: target,
		Result: rec.RolloutResult,
	}
}

// floatSummaryMessage renders the bounded delivery summary shown on mirror
// status: source, target, old/new image, and the per-stage results as
// recorded — with the standing honesty note (imageApplied never implies
// rollout success, ADR-005 §3).
func floatSummaryMessage(sub *imageflowv1alpha1.ImageSubscription, rec *imageflowv1alpha1.DeliveryResult) string {
	oldImage := auditImageNoneRecorded
	if rec.OldImage != "" {
		oldImage = rec.OldImage
	}
	result := string(rec.RolloutResult)
	if result == "" {
		result = "Pending"
	}
	health := string(rec.HealthResult)
	if health == "" {
		health = string(imageflowv1alpha1.HealthUnknown)
	}
	return fmt.Sprintf(
		"Delivery summary (%s %s/%s → %s): %s ⇒ %s; imageApplied: %t; rollout: %s; health: %s (recorded %s; imageApplied never implies rollout success, ADR-005 §3); bounded mirror window per --mirror-event-limit, full audit in kube Events of both namespaces (ADR-008 §4).",
		sub.Spec.Source.Kind, sub.Spec.Source.Namespace, sub.Spec.Source.Name,
		authz.FormatTarget(rec.TargetRef),
		oldImage, rec.NewImage, rec.ImageApplied, result, health,
		rec.ObservedAt.UTC().Format(time.RFC3339))
}

// floatIdentity is the stable delivery identity used for remove-then-insert
// replacement of in-flight summaries (mirrorEventKey itself includes the
// evolving Result/Reason/Message, so it cannot serve as the identity).
func floatIdentity(ev imageflowv1alpha1.MirrorStatusEvent) string {
	return string(ev.Type) + "\x00" + subscriptionRefKey(ev.Subscription) + "\x00" +
		targetRefKey(ev.Target) + "\x00" + ev.OldDigest + "\x00" + ev.NewDigest
}

// mergeDeliveryFloatEvents drops stored Delivery entries that share an
// identity with a fresh render (they are superseded views of the same
// delivery), then union-merges through the synthesis helper so ImageChanged
// entries and other subscriptions' summaries are preserved, sorted and
// bounded exactly like the T5 loop does.
func mergeDeliveryFloatEvents(stored, rendered []imageflowv1alpha1.MirrorStatusEvent, limit int) ([]imageflowv1alpha1.MirrorStatusEvent, int) {
	identities := map[string]bool{}
	for _, ev := range rendered {
		identities[floatIdentity(ev)] = true
	}
	kept := make([]imageflowv1alpha1.MirrorStatusEvent, 0, len(stored))
	for _, ev := range stored {
		if ev.Type == imageflowv1alpha1.MirrorEventDelivery && identities[floatIdentity(ev)] {
			continue
		}
		kept = append(kept, ev)
	}
	return mergeMirrorEvents(kept, rendered, limit)
}

// syncImageStreamTagFloat writes the rendered summaries onto the
// ImageStreamTag mirror of <stream>:<tag> in the source namespace.
func (r *SubscriptionObserverReconciler) syncImageStreamTagFloat(ctx context.Context, namespace, stream, tag string, rendered []imageflowv1alpha1.MirrorStatusEvent) {
	ist := &imageflowv1alpha1.ImageStreamTag{}
	key := types.NamespacedName{Namespace: namespace, Name: EncodeImageStreamTagName(stream, tag)}
	r.syncMirrorFloat(ctx, mirrorKindImageStreamTag, key, ist, rendered)
}

// syncImageTagFloat writes the digest-scoped summaries onto the ImageTag
// mirror of <stream>@<digest>.
func (r *SubscriptionObserverReconciler) syncImageTagFloat(ctx context.Context, namespace, stream, digest string, rendered []imageflowv1alpha1.MirrorStatusEvent) {
	it := &imageflowv1alpha1.ImageTag{}
	key := types.NamespacedName{Namespace: namespace, Name: EncodeImageTagName(stream, digest)}
	r.syncMirrorFloat(ctx, mirrorKindImageTag, key, it, rendered)
}

// syncMirrorFloat is the shared bounded status writer for the two mirror
// kinds (both inline MirrorObjectStatus); only Status.Events is ever
// touched — Image/Conditions/Consumers/ObservedGeneration belong to the T5
// synthesis loop. NotFound is a silent skip: the catalog may legitimately
// not have synthesized this mirror (yet).
func (r *SubscriptionObserverReconciler) syncMirrorFloat(
	ctx context.Context,
	kind string,
	key types.NamespacedName,
	obj client.Object,
	rendered []imageflowv1alpha1.MirrorStatusEvent,
) {
	limit := effectiveMirrorEventLimit(r.config.MirrorEventLimit)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.client.Get(ctx, key, obj); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		events := mirrorEventsOf(obj)
		if events == nil {
			return fmt.Errorf("mirror float-up: %T has no mirror status surface", obj)
		}
		merged, dropped := mergeDeliveryFloatEvents(*events, rendered, limit)
		if equality.Semantic.DeepEqual(*events, merged) {
			return nil // steady state: no write, no events, no metrics
		}
		*events = merged
		if err := r.client.Status().Update(ctx, obj); err != nil {
			return err
		}
		if dropped > 0 {
			recordFloatTruncation(ctx, r.recorder, obj, kind, dropped, limit)
		}
		return nil
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "mirror delivery float-up write failed; next observer pass retries",
			"mirrorKind", kind, "mirror", key)
	}
}

// mirrorEventsOf exposes the bounded events window of either mirror kind.
func mirrorEventsOf(obj client.Object) *[]imageflowv1alpha1.MirrorStatusEvent {
	switch o := obj.(type) {
	case *imageflowv1alpha1.ImageStreamTag:
		return &o.Status.Events
	case *imageflowv1alpha1.ImageTag:
		return &o.Status.Events
	}
	return nil
}

// recordFloatTruncation reuses the T5 truncation contract verbatim: the
// shared imageflow_mirror_events_truncated_total{kind} collector (same
// fact: summaries left the bounded mirror window) plus an
// EventSummaryTruncated Event on the mirror object and a V(1) log line
// (ADR-008 §4: overflow goes to Events + logs).
func recordFloatTruncation(ctx context.Context, rec record.EventRecorder, obj client.Object, kind string, dropped, limit int) {
	mirrorEventsTruncated.WithLabelValues(kind).Add(float64(dropped))
	rec.Eventf(obj, corev1.EventTypeNormal, mirrorEventReasonSummaryTruncated,
		"mirror event summary is bounded to %d entries; %d older entries left the window "+
			"(full history lives in kube Events and the cluster audit log, ADR-008)",
		limit, dropped)
	log.FromContext(ctx).V(1).Info("mirror delivery float-up dropped oldest summaries",
		"mirrorKind", kind, "mirror", client.ObjectKeyFromObject(obj), "dropped", dropped)
}
