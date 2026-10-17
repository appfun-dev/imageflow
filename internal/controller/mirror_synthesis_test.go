package controller

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

const (
	synthNS       = "imageflow-system"
	synthStream   = "payments"
	tagStableUnit = "stable"
	tagEdgeUnit   = "edge"
	hexB          = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hexC          = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	digestA       = "sha256:" + testHexA
	digestB       = "sha256:" + hexB
	digestC       = "sha256:" + hexC
	refA          = "quay.io/example/payments@" + digestA
	refA2         = "quay.io/mirror/payments@" + digestA
	refB          = "quay.io/example/payments@" + digestB
	refC          = "quay.io/example/payments@" + digestC
)

var (
	t0 = metav1.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	t1 = metav1.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)
	t2 = metav1.Date(2026, 10, 3, 8, 0, 0, 0, time.UTC)
	t3 = metav1.Date(2026, 10, 4, 8, 0, 0, 0, time.UTC)
)

func tagEvent(digest, ref string, created metav1.Time) imageflowv1alpha1.TagEvent {
	return imageflowv1alpha1.TagEvent{Digest: digest, ImageReference: ref, Created: created}
}

func synthStreamFixture() *imageflowv1alpha1.ImageStream {
	return &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  synthNS,
			Name:       synthStream,
			UID:        types.UID("stream-uid-1"),
			Generation: 7,
		},
		Status: imageflowv1alpha1.ImageStreamStatus{
			Tags: []imageflowv1alpha1.NamedTagEventList{
				{Tag: tagStableUnit, Items: []imageflowv1alpha1.TagEvent{
					tagEvent(digestA, refA, t3), tagEvent(digestB, refB, t2),
				}},
				{Tag: tagEdgeUnit, Items: []imageflowv1alpha1.TagEvent{
					tagEvent(digestA, refA2, t1),
				}},
				{Tag: "latest", Items: []imageflowv1alpha1.TagEvent{
					tagEvent(digestC, refC, t0),
				}},
				{Tag: "declared-only"}, // no items: nothing to mirror yet
			},
		},
	}
}

func synthSubFixture(ns, name, sourceName string) imageflowv1alpha1.ImageSubscription {
	return imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      mirrorSourceKindImageStreamTag,
				Name:      sourceName,
				Namespace: synthNS,
			},
		},
	}
}

func onlyArtifactA(digest string) bool { return digest == digestA }

func TestEffectiveMirrorEventLimit(t *testing.T) {
	cases := map[int]int{
		0:  imageflowv1alpha1.MirrorEventMaxItems,
		-3: imageflowv1alpha1.MirrorEventMaxItems,
		1:  1,
		5:  imageflowv1alpha1.MirrorEventMaxItems,
		9:  imageflowv1alpha1.MirrorEventMaxItems, // clamped to the CRD cap
	}
	for in, want := range cases {
		if got := effectiveMirrorEventLimit(in); got != want {
			t.Fatalf("effectiveMirrorEventLimit(%d) = %d, want %d", in, got, want)
		}
	}
}

func deliveryEvent(sub string, observed metav1.Time) imageflowv1alpha1.MirrorStatusEvent {
	return imageflowv1alpha1.MirrorStatusEvent{
		Type:       imageflowv1alpha1.MirrorEventDelivery,
		Reason:     "Applied",
		Message:    "delivery summary from the subscription controller",
		ObservedAt: observed,
		Result:     imageflowv1alpha1.RolloutSucceeded,
		Subscription: &imageflowv1alpha1.SubscriptionObjectRef{
			Namespace: "team-a", Name: sub,
		},
	}
}

func imageChangedEvent(digest string, observed metav1.Time) imageflowv1alpha1.MirrorStatusEvent {
	return imageflowv1alpha1.MirrorStatusEvent{
		Type:       imageflowv1alpha1.MirrorEventImageChanged,
		Reason:     mirrorReasonAssociated,
		Message:    "recomputed",
		ObservedAt: observed,
		NewDigest:  digest,
	}
}

func TestMergeMirrorEventsDedupesAndPreservesDelivery(t *testing.T) {
	ic1 := imageChangedEvent(digestA, t1)
	ic1.Message = "tag \"stable\" associated with " + refA // exactly what synthesis recomputes
	ic2 := imageChangedEvent(digestB, t0)
	del := deliveryEvent("sub-1", t2)

	stored := []imageflowv1alpha1.MirrorStatusEvent{del, ic1}
	recomputed := []imageflowv1alpha1.MirrorStatusEvent{ic1, ic2}

	merged, dropped := mergeMirrorEvents(stored, recomputed, 5)
	if dropped != 0 {
		t.Fatalf("nothing may be dropped under the limit, dropped %d", dropped)
	}
	want := []imageflowv1alpha1.MirrorStatusEvent{del, ic1, ic2} // newest first
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("merged =\n%#v\nwant\n%#v", merged, want)
	}
}

func TestMergeMirrorEventsBoundsAndReportsDropped(t *testing.T) {
	stored := []imageflowv1alpha1.MirrorStatusEvent{
		deliveryEvent("s1", t3), deliveryEvent("s2", t2),
	}
	recomputed := []imageflowv1alpha1.MirrorStatusEvent{
		imageChangedEvent(digestA, t1), imageChangedEvent(digestB, t0),
	}
	merged, dropped := mergeMirrorEvents(stored, recomputed, 2)
	if len(merged) != 2 {
		t.Fatalf("merged must be bounded to 2, got %d", len(merged))
	}
	if dropped != 2 {
		t.Fatalf("dropped must be 2, got %d", dropped)
	}
	// The two newest entries survive (the Delivery entries at t3 and t2).
	if merged[0].Type != imageflowv1alpha1.MirrorEventDelivery || !merged[0].ObservedAt.Equal(&t3) {
		t.Fatalf("newest entry must be the t3 delivery, got %#v", merged[0])
	}
	if merged[1].Type != imageflowv1alpha1.MirrorEventDelivery || !merged[1].ObservedAt.Equal(&t2) {
		t.Fatalf("second entry must be the t2 delivery, got %#v", merged[1])
	}
}

func TestMergeMirrorEventsDeterministicOrderOnEqualTimestamps(t *testing.T) {
	ev := func(digest string) imageflowv1alpha1.MirrorStatusEvent {
		return imageChangedEvent(digest, t1) // identical timestamps on purpose
	}
	stored := []imageflowv1alpha1.MirrorStatusEvent{ev(digestC), ev(digestA), ev(digestB)}
	for i := 0; i < 5; i++ {
		merged, _ := mergeMirrorEvents(stored, nil, 5)
		if merged[0].NewDigest != digestA || merged[1].NewDigest != digestB || merged[2].NewDigest != digestC {
			t.Fatalf("merge order must be deterministic across calls, got %v",
				[]string{merged[0].NewDigest, merged[1].NewDigest, merged[2].NewDigest})
		}
	}
}

func TestMergeMirrorEventsEmpty(t *testing.T) {
	merged, dropped := mergeMirrorEvents(nil, nil, 5)
	if merged != nil || dropped != 0 {
		t.Fatalf("empty merge must yield nil slice and zero dropped, got %#v/%d", merged, dropped)
	}
}

func TestNormalizeConsumersSortDedupeCap(t *testing.T) {
	refs := make([]imageflowv1alpha1.ConsumerReference, 0, 122)
	for i := 0; i < 60; i++ {
		refs = append(refs, imageflowv1alpha1.ConsumerReference{
			Namespace: "ns-b", Name: fmt.Sprintf("sub-%02d", i),
		})
	}
	// Duplicates plus a namespace that sorts first.
	refs = append(refs,
		imageflowv1alpha1.ConsumerReference{Namespace: "ns-b", Name: "sub-00"},
		imageflowv1alpha1.ConsumerReference{Namespace: "ns-a", Name: "sub-99"},
	)
	got := normalizeConsumers(refs)
	if len(got) != imageflowv1alpha1.MirrorConsumerMaxItems {
		t.Fatalf("consumers must cap at %d, got %d", imageflowv1alpha1.MirrorConsumerMaxItems, len(got))
	}
	if got[0].Namespace != "ns-a" || got[0].Name != "sub-99" {
		t.Fatalf("consumers must sort by (namespace,name), got %#v", got[0])
	}
	if got[1].Namespace != "ns-b" || got[1].Name != "sub-00" || got[2].Name != "sub-01" {
		t.Fatalf("duplicates must be removed and order kept, got %#v %#v", got[1], got[2])
	}
	if normalizeConsumers(nil) != nil {
		t.Fatal("empty input must yield nil")
	}
}

func TestConsumersByTag(t *testing.T) {
	subs := []imageflowv1alpha1.ImageSubscription{
		synthSubFixture("team-a", "sub-1", synthStream+":stable"),
		synthSubFixture("team-b", "sub-2", synthStream+":stable"),
		synthSubFixture("team-a", "sub-3", synthStream+":edge"),
		synthSubFixture("team-a", "ignored-other-stream", "other:stable"),
		func() imageflowv1alpha1.ImageSubscription {
			s := synthSubFixture("team-a", "ignored-bad-name", "no-colon")
			return s
		}(),
		func() imageflowv1alpha1.ImageSubscription {
			s := synthSubFixture("team-a", "ignored-other-ns", synthStream+":stable")
			s.Spec.Source.Namespace = "some-other-catalog"
			return s
		}(),
	}
	got := consumersByTag(subs, synthNS, synthStream)
	wantStable := []imageflowv1alpha1.ConsumerReference{
		{Namespace: "team-a", Name: "sub-1"},
		{Namespace: "team-b", Name: "sub-2"},
	}
	if !reflect.DeepEqual(got[tagStableUnit], wantStable) {
		t.Fatalf("stable consumers = %#v, want %#v", got["stable"], wantStable)
	}
	if !reflect.DeepEqual(got[tagEdgeUnit], []imageflowv1alpha1.ConsumerReference{{Namespace: "team-a", Name: "sub-3"}}) {
		t.Fatalf("edge consumers = %#v", got[tagEdgeUnit])
	}
	if len(got) != 2 {
		t.Fatalf("filtered subscriptions must not leak into other tags: %#v", got)
	}
}

func TestImageChangedEventsForTag(t *testing.T) {
	items := []imageflowv1alpha1.TagEvent{
		tagEvent(digestA, refA, t3), tagEvent(digestB, refB, t2), tagEvent(digestC, refC, t1),
	}
	events := imageChangedEventsForTag(tagStableUnit, items)
	if len(events) != 3 {
		t.Fatalf("want one event per history item, got %d", len(events))
	}
	// Newest first, oldDigest chains through the history, oldest has none.
	if events[0].NewDigest != digestA || events[0].OldDigest != digestB || !events[0].ObservedAt.Equal(&t3) {
		t.Fatalf("event[0] = %#v", events[0])
	}
	if events[1].NewDigest != digestB || events[1].OldDigest != digestC || !events[1].ObservedAt.Equal(&t2) {
		t.Fatalf("event[1] = %#v", events[1])
	}
	if events[2].NewDigest != digestC || events[2].OldDigest != "" || !events[2].ObservedAt.Equal(&t1) {
		t.Fatalf("event[2] = %#v", events[2])
	}
	for _, ev := range events {
		if ev.Type != imageflowv1alpha1.MirrorEventImageChanged || ev.Reason != mirrorReasonAssociated {
			t.Fatalf("event type/reason = %q/%q", ev.Type, ev.Reason)
		}
	}
}

// synthesizeFixture runs the shared two-tags-plus-edge synthesis fact set:
// stable [A(t3), B(t2)], edge [A(t1)], latest [C(t0)], declared-only (no
// items); consumers sub-1/sub-2 on stable and sub-3 on edge; artifact exists
// for digest A only.
func synthesizeFixture(t *testing.T) (
	*imageflowv1alpha1.ImageStream,
	[]imageflowv1alpha1.ImageStreamTag,
	[]imageflowv1alpha1.ImageTag,
) {
	t.Helper()
	stream := synthStreamFixture()
	subs := []imageflowv1alpha1.ImageSubscription{
		synthSubFixture("team-a", "sub-1", synthStream+":"+tagStableUnit),
		synthSubFixture("team-b", "sub-2", synthStream+":"+tagStableUnit),
		synthSubFixture("team-a", "sub-3", synthStream+":"+tagEdgeUnit),
	}
	now := metav1.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC).Time
	ists, its := synthesizeMirrors(stream, subs, onlyArtifactA, now)
	return stream, ists, its
}

func TestSynthesizeMirrorsImageStreamTagViews(t *testing.T) {
	stream, ists, its := synthesizeFixture(t)

	if len(ists) != 3 {
		t.Fatalf("want 3 ImageStreamTag mirrors (declared-only tag has no association), got %d", len(ists))
	}
	if len(its) != 3 {
		t.Fatalf("want 3 ImageTag mirrors (A,B,C), got %d", len(its))
	}

	byName := map[string]imageflowv1alpha1.ImageStreamTag{}
	for _, ist := range ists {
		byName[ist.Name] = ist
	}

	stable, ok := byName[synthStream+"--"+tagStableUnit]
	if !ok {
		t.Fatalf("missing stable mirror in %#v", byName)
	}
	if stable.Spec.Stream != synthStream || stable.Spec.Tag != tagStableUnit {
		t.Fatalf("stable spec = %#v", stable.Spec)
	}
	assertOwnerRef(t, stable.OwnerReferences, stream)
	if stable.Status.ObservedGeneration != 7 {
		t.Fatalf("observedGeneration = %d, want the source stream generation 7", stable.Status.ObservedGeneration)
	}
	img := stable.Status.Image
	if img == nil || img.Digest != digestA || img.ImageReference != refA ||
		img.ArtifactRef != "sha256-"+testHexA || !img.Created.Equal(&t3) {
		t.Fatalf("stable image = %#v", img)
	}
	if len(stable.Status.Events) != 2 || stable.Status.Events[0].NewDigest != digestA ||
		stable.Status.Events[0].OldDigest != digestB || stable.Status.Events[1].NewDigest != digestB {
		t.Fatalf("stable events = %#v", stable.Status.Events)
	}
	wantConsumers := []imageflowv1alpha1.ConsumerReference{
		{Namespace: "team-a", Name: "sub-1"}, {Namespace: "team-b", Name: "sub-2"},
	}
	if !reflect.DeepEqual(stable.Status.Consumers, wantConsumers) {
		t.Fatalf("stable consumers = %#v", stable.Status.Consumers)
	}
	assertCondition(t, stable.Status.Conditions, mirrorConditionReady, metav1.ConditionTrue, mirrorReasonSynthesized)
	assertCondition(t, stable.Status.Conditions, mirrorConditionArtifactLinked,
		metav1.ConditionTrue, mirrorReasonArtifactFound)

	latest := byName[synthStream+"--latest"]
	if latest.Status.Image == nil || latest.Status.Image.Digest != digestC ||
		latest.Status.Image.ArtifactRef != "" {
		t.Fatalf("latest image must tolerate a missing artifact: %#v", latest.Status.Image)
	}
	assertCondition(t, latest.Status.Conditions, mirrorConditionArtifactLinked,
		metav1.ConditionFalse, mirrorReasonArtifactAbsent)
	if len(latest.Status.Consumers) != 0 {
		t.Fatalf("latest has no consumers: %#v", latest.Status.Consumers)
	}
}

func TestSynthesizeMirrorsImageTagViews(t *testing.T) {
	_, _, its := synthesizeFixture(t)

	itByDigest := map[string]imageflowv1alpha1.ImageTag{}
	for _, it := range its {
		itByDigest[it.Spec.Digest] = it
	}

	itA := itByDigest[digestA]
	if itA.Name != synthStream+"--sha256-"+testHexA {
		t.Fatalf("ImageTag name = %q", itA.Name)
	}
	// digest A is associated from two tags: created = earliest recording,
	// imageReference = most recent recording.
	if !itA.Status.Image.Created.Equal(&t1) {
		t.Fatalf("ImageTag A created must be the earliest recording t1: %#v", itA.Status.Image.Created)
	}
	if itA.Status.Image.ImageReference != refA {
		t.Fatalf("ImageTag A imageReference must be the most recent recording refA: %#v", itA.Status.Image)
	}
	// Consumers of digest A = consumers of every tag currently pointing at A
	// (stable and edge), unioned and sorted.
	wantItAConsumers := []imageflowv1alpha1.ConsumerReference{
		{Namespace: "team-a", Name: "sub-1"}, {Namespace: "team-a", Name: "sub-3"},
		{Namespace: "team-b", Name: "sub-2"},
	}
	if !reflect.DeepEqual(itA.Status.Consumers, wantItAConsumers) {
		t.Fatalf("ImageTag A consumers = %#v", itA.Status.Consumers)
	}
	if len(itA.Status.Events) != 2 {
		t.Fatalf("ImageTag A must carry both associations: %#v", itA.Status.Events)
	}
	if !itA.Status.Events[0].ObservedAt.Equal(&t3) || !itA.Status.Events[1].ObservedAt.Equal(&t1) {
		t.Fatalf("ImageTag A events must be newest first: %#v", itA.Status.Events)
	}

	itB := itByDigest[digestB]
	if len(itB.Status.Events) != 1 || itB.Status.Events[0].NewDigest != digestB {
		t.Fatalf("ImageTag B events = %#v", itB.Status.Events)
	}
	if len(itB.Status.Consumers) != 0 {
		t.Fatalf("historic-only digest B must have no consumers: %#v", itB.Status.Consumers)
	}
}

func TestSynthesizeMirrorsDeterministic(t *testing.T) {
	// Synthesis must be deterministic: two passes over the same facts yield
	// deep-equal objects (the no-write-amplification precondition).
	stream1, ists1, its1 := synthesizeFixture(t)
	stream2, ists2, its2 := synthesizeFixture(t)
	if !reflect.DeepEqual(stream1, stream2) {
		t.Fatal("fixtures diverge; determinism check is meaningless")
	}
	if !reflect.DeepEqual(ists1, ists2) || !reflect.DeepEqual(its1, its2) {
		t.Fatal("synthesizeMirrors is not deterministic across calls")
	}
}

func TestMergeMirrorConditions(t *testing.T) {
	now := metav1.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	later := metav1.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)

	stored := []metav1.Condition{{
		Type: mirrorConditionReady, Status: metav1.ConditionTrue, Reason: mirrorReasonSynthesized,
		Message: "old", LastTransitionTime: now,
	}}
	desired := []metav1.Condition{{
		Type: mirrorConditionReady, Status: metav1.ConditionTrue, Reason: mirrorReasonSynthesized,
		Message: "old", LastTransitionTime: later,
	}, {
		Type: mirrorConditionArtifactLinked, Status: metav1.ConditionFalse, Reason: mirrorReasonArtifactAbsent,
		Message: "absent", LastTransitionTime: later,
	}}

	merged := mergeMirrorConditions(stored, desired)
	if len(merged) != 2 {
		t.Fatalf("new condition types must append: %#v", merged)
	}
	if !merged[0].LastTransitionTime.Equal(&now) {
		t.Fatalf("unchanged status must preserve the stored transition time: %#v", merged[0])
	}
	if merged[0].Message != "old" {
		t.Fatalf("message must not drift: %#v", merged[0])
	}

	// A status flip adopts the caller-provided (clock-derived) transition time.
	flipped := []metav1.Condition{{
		Type: mirrorConditionArtifactLinked, Status: metav1.ConditionTrue, Reason: mirrorReasonArtifactFound,
		Message: "registered", LastTransitionTime: later,
	}}
	merged = mergeMirrorConditions(merged, flipped)
	linked := meta.FindStatusCondition(merged, mirrorConditionArtifactLinked)
	if linked == nil {
		t.Fatalf("ArtifactLinked condition missing: %#v", merged)
	}
	if linked.Status != metav1.ConditionTrue || !linked.LastTransitionTime.Equal(&later) {
		t.Fatalf("flipped condition must adopt the new transition time: %#v", linked)
	}
}

func assertOwnerRef(t *testing.T, refs []metav1.OwnerReference, stream *imageflowv1alpha1.ImageStream) {
	t.Helper()
	if len(refs) != 1 {
		t.Fatalf("want exactly one ownerRef, got %#v", refs)
	}
	ref := refs[0]
	if ref.APIVersion != imageflowv1alpha1.GroupVersion.String() || ref.Kind != "ImageStream" {
		t.Fatalf("ownerRef GVK = %s/%s", ref.APIVersion, ref.Kind)
	}
	if ref.Name != stream.Name || ref.UID != stream.UID {
		t.Fatalf("ownerRef target = %s/%s", ref.Name, ref.UID)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Fatal("ownerRef must be controlling (cascade GC contract)")
	}
	if ref.BlockOwnerDeletion == nil || !*ref.BlockOwnerDeletion {
		t.Fatal("ownerRef must block owner deletion until dependents are gone")
	}
}

func assertCondition(
	t *testing.T,
	conds []metav1.Condition,
	condType string,
	status metav1.ConditionStatus,
	reason string,
) {
	t.Helper()
	c := meta.FindStatusCondition(conds, condType)
	if c == nil {
		t.Fatalf("condition %s missing: %#v", condType, conds)
	}
	if c.Status != status || c.Reason != reason {
		t.Fatalf("condition %s = (%s,%s), want (%s,%s)", condType, c.Status, c.Reason, status, reason)
	}
	if c.LastTransitionTime.IsZero() {
		t.Fatalf("condition %s must carry a transition time", condType)
	}
}
