package controller

// Pure synthesis helpers for the mirror controller (ticket T5). Everything in
// this file is deterministic and free of client I/O so the synthesis decisions
// can be unit-tested with fake objects; the reconciler in mirror_controller.go
// performs the reads/writes and merges the results into stored objects.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// Vocabulary introduced by behavior ticket T5 (api/v1alpha1 explicitly allows
// behavior tickets to add condition types; the CRD CEL guards spec, not this).
const (
	// mirrorConditionReady reports that the mirror reflects the source stream
	// status (the mirror exists only when an association exists).
	mirrorConditionReady = "Ready"
	// mirrorConditionArtifactLinked reports whether the cluster-scoped
	// ImageArtifact for the mirrored digest is registered. Absence is
	// tolerated (registration belongs to the mapping/import paths, T4/T9):
	// artifactRef simply stays empty until the artifact appears.
	mirrorConditionArtifactLinked = "ArtifactLinked"

	mirrorReasonSynthesized    = "Synthesized"
	mirrorReasonArtifactFound  = "ArtifactFound"
	mirrorReasonArtifactAbsent = "ArtifactNotFound"

	// mirrorReasonAssociated is the stable reason of ImageChanged entries
	// synthesized from stream tag history items.
	mirrorReasonAssociated = "Associated"

	// mirrorSourceKindImageStreamTag is the only subscription source kind in v1
	// (frozen enum on SubscriptionSourceRef).
	mirrorSourceKindImageStreamTag = "ImageStreamTag"
)

// effectiveMirrorEventLimit clamps the --mirror-event-limit flag into
// [1, MirrorEventMaxItems]: a non-positive value (e.g. a zero-valued config in
// tests) falls back to the CRD cap, and values above the CRD cap are clamped
// because the API server rejects larger status arrays (T2 structural guard).
func effectiveMirrorEventLimit(configured int) int {
	if configured <= 0 {
		return imageflowv1alpha1.MirrorEventMaxItems
	}
	if configured > imageflowv1alpha1.MirrorEventMaxItems {
		return imageflowv1alpha1.MirrorEventMaxItems
	}
	return configured
}

// synthesizeMirrors computes the complete desired set of mirror objects for
// one ImageStream: one ImageStreamTag per status tag with at least one
// association (current digest), and one ImageTag per distinct digest anywhere
// in the bounded per-tag histories (so rollback/audit can address digests that
// recently left a tag). artifactExists reports whether the cluster-scoped
// ImageArtifact for a digest is registered; now is the injected clock (seam
// S3) used for condition transition timestamps; the returned objects carry
// recomputed ImageChanged events only — merging with stored events (which may
// contain Delivery summaries written by the subscription controller, T11) and
// applying the event bound happens in mergeMirrorEvents at apply time.
//
// Both returned slices are sorted by object name for deterministic behavior.
func synthesizeMirrors(
	stream *imageflowv1alpha1.ImageStream,
	subs []imageflowv1alpha1.ImageSubscription,
	artifactExists func(digest string) bool,
	now time.Time,
) (ists []imageflowv1alpha1.ImageStreamTag, its []imageflowv1alpha1.ImageTag) {
	consumers := consumersByTag(subs, stream.Namespace, stream.Name)

	// digestOccurrences collects every (tag, history index) a digest appears
	// at, so the ImageTag view of a digest sees all of its associations.
	occurrences := map[string][]digestOccurrence{}

	for _, tagEntry := range stream.Status.Tags {
		if len(tagEntry.Items) == 0 {
			continue
		}
		current := tagEntry.Items[0]
		ist := imageflowv1alpha1.ImageStreamTag{
			ObjectMeta: metav1.ObjectMeta{
				Name:            EncodeImageStreamTagName(stream.Name, tagEntry.Tag),
				Namespace:       stream.Namespace,
				OwnerReferences: []metav1.OwnerReference{mirrorOwnerReference(stream)},
			},
			Spec: imageflowv1alpha1.ImageStreamTagSpec{
				Stream: stream.Name,
				Tag:    tagEntry.Tag,
			},
		}
		ist.Status.ObservedGeneration = stream.Generation
		ist.Status.Image = mirrorImageFor(current, artifactExists)
		ist.Status.Events = imageChangedEventsForTag(tagEntry.Tag, tagEntry.Items)
		ist.Status.Consumers = normalizeConsumers(consumers[tagEntry.Tag])
		ist.Status.Conditions = mirrorConditions(
			fmt.Sprintf("Synthesized from ImageStream %q tag %q.", stream.Name, tagEntry.Tag),
			current.Digest, artifactExists, stream.Generation, now)
		ists = append(ists, ist)

		for i, item := range tagEntry.Items {
			occurrences[item.Digest] = append(occurrences[item.Digest],
				digestOccurrence{tag: tagEntry.Tag, index: i, item: item, items: tagEntry.Items})
		}
	}

	for digest, occ := range occurrences {
		it := imageflowv1alpha1.ImageTag{
			ObjectMeta: metav1.ObjectMeta{
				Name:            EncodeImageTagName(stream.Name, digest),
				Namespace:       stream.Namespace,
				OwnerReferences: []metav1.OwnerReference{mirrorOwnerReference(stream)},
			},
			Spec: imageflowv1alpha1.ImageTagSpec{
				Stream: stream.Name,
				Digest: digest,
			},
		}
		it.Status.ObservedGeneration = stream.Generation

		// Image identity across associations: created = first time the digest
		// was recorded on this stream; imageReference = most recent recording.
		first := occ[0].item
		latest := occ[0].item
		for _, o := range occ[1:] {
			if o.item.Created.Before(&first.Created) {
				first = o.item
			}
			if latest.Created.Before(&o.item.Created) {
				latest = o.item
			}
		}
		it.Status.Image = mirrorImageFor(first, artifactExists)
		it.Status.Image.ImageReference = latest.ImageReference

		it.Status.Events = imageChangedEventsForDigest(occ)
		// Consumers of a digest = consumers of every tag currently pointing at
		// it (historic-only associations have no live consumers).
		var digestConsumers []imageflowv1alpha1.ConsumerReference
		for _, tagEntry := range stream.Status.Tags {
			if len(tagEntry.Items) > 0 && tagEntry.Items[0].Digest == digest {
				digestConsumers = append(digestConsumers, consumers[tagEntry.Tag]...)
			}
		}
		it.Status.Consumers = normalizeConsumers(digestConsumers)
		it.Status.Conditions = mirrorConditions(
			fmt.Sprintf("Synthesized from ImageStream %q digest %s.", stream.Name, digest),
			digest, artifactExists, stream.Generation, now)
		its = append(its, it)
	}

	sort.Slice(ists, func(i, j int) bool { return ists[i].Name < ists[j].Name })
	sort.Slice(its, func(i, j int) bool { return its[i].Name < its[j].Name })
	return ists, its
}

// imageChangedEventsForTag renders the bounded stream history of one tag as
// ImageChanged mirror events, newest first. items are ordered most recent
// first (ImageStream.status.tags[].items, ADR-008); the previous association
// in that list is the oldDigest of the change.
func imageChangedEventsForTag(tag string, items []imageflowv1alpha1.TagEvent) []imageflowv1alpha1.MirrorStatusEvent {
	events := make([]imageflowv1alpha1.MirrorStatusEvent, 0, len(items))
	for i, item := range items {
		ev := imageflowv1alpha1.MirrorStatusEvent{
			Type:    imageflowv1alpha1.MirrorEventImageChanged,
			Reason:  mirrorReasonAssociated,
			Message: fmt.Sprintf("tag %q associated with %s", tag, item.ImageReference),

			ObservedAt: *item.Created.DeepCopy(),
			NewDigest:  item.Digest,
		}
		if i+1 < len(items) {
			ev.OldDigest = items[i+1].Digest
		}
		events = append(events, ev)
	}
	sortMirrorEvents(events)
	return events
}

// digestOccurrence is one (tag, history position) a digest appears at inside
// ImageStream.status.tags[]. items is the full history of that tag so the
// previous association (oldDigest of the change) can be resolved.
type digestOccurrence struct {
	tag   string
	index int
	item  imageflowv1alpha1.TagEvent
	items []imageflowv1alpha1.TagEvent
}

// imageChangedEventsForDigest renders every (tag, association) that points at
// the digest of an ImageTag mirror as ImageChanged events, newest first.
func imageChangedEventsForDigest(occ []digestOccurrence) []imageflowv1alpha1.MirrorStatusEvent {
	events := make([]imageflowv1alpha1.MirrorStatusEvent, 0, len(occ))
	for _, o := range occ {
		ev := imageflowv1alpha1.MirrorStatusEvent{
			Type:       imageflowv1alpha1.MirrorEventImageChanged,
			Reason:     mirrorReasonAssociated,
			Message:    fmt.Sprintf("tag %q associated with this digest (%s)", o.tag, o.item.ImageReference),
			ObservedAt: *o.item.Created.DeepCopy(),
			NewDigest:  o.item.Digest,
		}
		if o.index+1 < len(o.items) {
			ev.OldDigest = o.items[o.index+1].Digest
		}
		events = append(events, ev)
	}
	sortMirrorEvents(events)
	return events
}

// mirrorImageFor builds the current-association block; artifactRef is set only
// when the ImageArtifact actually exists (absence is tolerated and reported
// through the ArtifactLinked condition rather than a dangling reference).
func mirrorImageFor(item imageflowv1alpha1.TagEvent, artifactExists func(string) bool) *imageflowv1alpha1.MirrorImage {
	img := &imageflowv1alpha1.MirrorImage{
		Digest:         item.Digest,
		ImageReference: item.ImageReference,
		Created:        item.Created.DeepCopy(),
	}
	if artifactExists != nil && artifactExists(item.Digest) {
		img.ArtifactRef = ArtifactName(item.Digest)
	}
	return img
}

// mirrorConditions builds the desired condition set. LastTransitionTime uses
// the injected clock; mergeMirrorConditions preserves stored transition times
// when the condition status does not actually change.
func mirrorConditions(
	readyMessage, digest string,
	artifactExists func(string) bool,
	generation int64,
	now time.Time,
) []metav1.Condition {
	conds := []metav1.Condition{{
		Type:               mirrorConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             mirrorReasonSynthesized,
		Message:            readyMessage,
		ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(now),
	}}
	artifact := ArtifactName(digest)
	linked := metav1.Condition{
		Type:               mirrorConditionArtifactLinked,
		Status:             metav1.ConditionFalse,
		Reason:             mirrorReasonArtifactAbsent,
		Message:            fmt.Sprintf("ImageArtifact %q is not registered yet; artifactRef stays empty (absence tolerated, registration belongs to the mapping/import paths).", artifact),
		ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(now),
	}
	if artifactExists != nil && artifactExists(digest) {
		linked.Status = metav1.ConditionTrue
		linked.Reason = mirrorReasonArtifactFound
		linked.Message = fmt.Sprintf("ImageArtifact %q is registered.", artifact)
	}
	return append(conds, linked)
}

// mergeMirrorConditions applies desired conditions over the stored ones with
// meta.SetStatusCondition semantics: stored LastTransitionTime is preserved
// unless the condition status actually changes, in which case the caller's
// (clock-derived) transition time is used. Stored order is preserved; new
// condition types append.
func mergeMirrorConditions(stored, desired []metav1.Condition) []metav1.Condition {
	out := make([]metav1.Condition, len(stored))
	copy(out, stored)
	for _, c := range desired {
		meta.SetStatusCondition(&out, c)
	}
	return out
}

// mergeMirrorEvents unions recomputed ImageChanged events with the stored
// event summary, preserving entries the synthesis controller does not own —
// Delivery summaries are written by the subscription controller (T11 floats
// delivery outcomes up to mirrors; this merge is its durable write entry
// point) and historical ImageChanged entries that already fell out of the
// bounded stream history. Exact duplicates are removed, the result is ordered
// newest-first with a deterministic total order, and truncated to limit
// (ADR-008 bounded retention; overflow surfaces via kube Events at apply
// time). It returns the merged slice and how many entries were dropped.
func mergeMirrorEvents(
	stored, recomputed []imageflowv1alpha1.MirrorStatusEvent,
	limit int,
) (merged []imageflowv1alpha1.MirrorStatusEvent, dropped int) {
	if limit <= 0 {
		limit = imageflowv1alpha1.MirrorEventMaxItems
	}
	union := make([]imageflowv1alpha1.MirrorStatusEvent, 0, len(stored)+len(recomputed))
	seen := make(map[string]struct{}, len(stored)+len(recomputed))
	add := func(events []imageflowv1alpha1.MirrorStatusEvent) {
		for _, ev := range events {
			key := mirrorEventKey(ev)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			union = append(union, ev)
		}
	}
	add(recomputed)
	add(stored)

	sortMirrorEvents(union)
	if len(union) > limit {
		dropped = len(union) - limit
		union = union[:limit]
	}
	if len(union) == 0 {
		return nil, dropped
	}
	return union, dropped
}

// sortMirrorEvents orders events newest-first with a deterministic total
// order (identical timestamps tie-break on the remaining fields) so repeated
// synthesis of the same facts never reorders the array — a prerequisite for
// the no-write-amplification acceptance criterion.
func sortMirrorEvents(events []imageflowv1alpha1.MirrorStatusEvent) {
	sort.Slice(events, func(i, j int) bool { return mirrorEventLess(events[i], events[j]) })
}

func mirrorEventLess(a, b imageflowv1alpha1.MirrorStatusEvent) bool {
	if !a.ObservedAt.Equal(&b.ObservedAt) {
		return a.ObservedAt.After(b.ObservedAt.Time)
	}
	if a.Type != b.Type {
		return a.Type < b.Type
	}
	if a.OldDigest != b.OldDigest {
		return a.OldDigest < b.OldDigest
	}
	if a.NewDigest != b.NewDigest {
		return a.NewDigest < b.NewDigest
	}
	if a.Reason != b.Reason {
		return a.Reason < b.Reason
	}
	if a.Result != b.Result {
		return a.Result < b.Result
	}
	if as, bs := subscriptionRefKey(a.Subscription), subscriptionRefKey(b.Subscription); as != bs {
		return as < bs
	}
	if at, bt := targetRefKey(a.Target), targetRefKey(b.Target); at != bt {
		return at < bt
	}
	return a.Message < b.Message
}

// mirrorEventKey identifies an event for deduplication: two entries with the
// same key are the same fact rendered identically (synthesis is deterministic,
// so a recomputed ImageChanged entry equals its previously stored form field
// by field).
func mirrorEventKey(ev imageflowv1alpha1.MirrorStatusEvent) string {
	return strings.Join([]string{
		string(ev.Type),
		ev.Reason,
		ev.Message,
		strconv.FormatInt(ev.ObservedAt.UTC().UnixNano(), 10),
		ev.OldDigest,
		ev.NewDigest,
		subscriptionRefKey(ev.Subscription),
		targetRefKey(ev.Target),
		string(ev.Result),
	}, "\x00")
}

func subscriptionRefKey(ref *imageflowv1alpha1.SubscriptionObjectRef) string {
	if ref == nil {
		return ""
	}
	return ref.Namespace + "/" + ref.Name
}

func targetRefKey(ref *imageflowv1alpha1.TargetReference) string {
	if ref == nil {
		return ""
	}
	return ref.APIVersion + "/" + ref.Kind + "/" + ref.Namespace + "/" + ref.Name
}

// consumersByTag buckets subscription consumer references per tag of one
// stream. Subscriptions may live in any namespace; the source namespace must
// match the stream namespace and the source name is the logical
// "<stream>:<tag>" colon form (ADR-002 — users never write encoded names).
func consumersByTag(
	subs []imageflowv1alpha1.ImageSubscription,
	streamNamespace, streamName string,
) map[string][]imageflowv1alpha1.ConsumerReference {
	out := map[string][]imageflowv1alpha1.ConsumerReference{}
	for _, sub := range subs {
		src := sub.Spec.Source
		if src.Kind != mirrorSourceKindImageStreamTag || src.Namespace != streamNamespace {
			continue
		}
		stream, tag, ok := splitSubscriptionSourceName(src.Name)
		if !ok || stream != streamName {
			continue
		}
		out[tag] = append(out[tag], imageflowv1alpha1.ConsumerReference{
			Namespace: sub.Namespace,
			Name:      sub.Name,
		})
	}
	for tag := range out {
		out[tag] = normalizeConsumers(out[tag])
	}
	return out
}

// splitSubscriptionSourceName splits the logical "<stream>:<tag>" source form
// on its single colon (stream names are DNS-1123 and tags are OCI grammar, so
// neither can contain ':').
func splitSubscriptionSourceName(name string) (stream, tag string, ok bool) {
	parts := strings.SplitN(name, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// normalizeConsumers sorts, deduplicates and caps the consumer list at the
// structural CRD guard MirrorConsumerMaxItems (50). The cap is not a flag:
// ADR-002 R4 requires bounded enrichment without naming a number (T2 froze
// the structural cap).
func normalizeConsumers(refs []imageflowv1alpha1.ConsumerReference) []imageflowv1alpha1.ConsumerReference {
	if len(refs) == 0 {
		return nil
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Namespace != refs[j].Namespace {
			return refs[i].Namespace < refs[j].Namespace
		}
		return refs[i].Name < refs[j].Name
	})
	deduped := refs[:1]
	for _, ref := range refs[1:] {
		last := deduped[len(deduped)-1]
		if ref.Namespace != last.Namespace || ref.Name != last.Name {
			deduped = append(deduped, ref)
		}
	}
	if len(deduped) > imageflowv1alpha1.MirrorConsumerMaxItems {
		deduped = deduped[:imageflowv1alpha1.MirrorConsumerMaxItems]
	}
	return deduped
}

// mirrorOwnerReference builds the controlling ownerRef to the source stream:
// deleting the ImageStream cascades to every mirror through Kubernetes GC
// (user story 8, ADR-002 R4/R9).
func mirrorOwnerReference(stream *imageflowv1alpha1.ImageStream) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         imageflowv1alpha1.GroupVersion.String(),
		Kind:               "ImageStream",
		Name:               stream.Name,
		UID:                stream.UID,
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}
}
