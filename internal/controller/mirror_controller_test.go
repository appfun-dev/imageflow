package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
)

// newMirrorTestReconciler builds a mirrorReconciler on a fake client with the
// status subresource enabled for the mirror kinds, a fake clock (seam S3) and
// a fake event recorder.
func newMirrorTestReconciler(
	t *testing.T,
	eventLimit int,
	objs ...client.Object,
) (*mirrorReconciler, client.WithWatch, *record.FakeRecorder, *clock.Fake) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&imageflowv1alpha1.ImageStream{}, // tests write stream status directly (T4's lane in production)
			&imageflowv1alpha1.ImageStreamTag{},
			&imageflowv1alpha1.ImageTag{},
		).
		WithObjects(objs...).
		Build()
	fakeClock := clock.NewFake(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	recorder := record.NewFakeRecorder(256)
	r := &mirrorReconciler{
		client:     cl,
		recorder:   recorder,
		clock:      fakeClock,
		eventLimit: effectiveMirrorEventLimit(eventLimit),
	}
	return r, cl, recorder, fakeClock
}

func synthRequest() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: synthNS, Name: synthStream}}
}

func reconcileOK(t *testing.T, r *mirrorReconciler) {
	t.Helper()
	res, err := r.Reconcile(context.Background(), synthRequest())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Fatalf("Reconcile must not requeue on success: %#v", res)
	}
}

func getImageStreamTag(t *testing.T, cl client.Client, name string) *imageflowv1alpha1.ImageStreamTag {
	t.Helper()
	ist := &imageflowv1alpha1.ImageStreamTag{}
	key := client.ObjectKey{Namespace: synthNS, Name: name}
	if err := cl.Get(context.Background(), key, ist); err != nil {
		t.Fatalf("get ImageStreamTag %s: %v", name, err)
	}
	return ist
}

func getImageTag(t *testing.T, cl client.Client, digest string) *imageflowv1alpha1.ImageTag {
	t.Helper()
	it := &imageflowv1alpha1.ImageTag{}
	key := client.ObjectKey{Namespace: synthNS, Name: EncodeImageTagName(synthStream, digest)}
	if err := cl.Get(context.Background(), key, it); err != nil {
		t.Fatalf("get ImageTag for %s: %v", digest, err)
	}
	return it
}

func listMirrorRVs(t *testing.T, cl client.Client) map[string]string {
	t.Helper()
	ists := &imageflowv1alpha1.ImageStreamTagList{}
	if err := cl.List(context.Background(), ists, client.InNamespace(synthNS)); err != nil {
		t.Fatalf("list ISTs: %v", err)
	}
	its := &imageflowv1alpha1.ImageTagList{}
	if err := cl.List(context.Background(), its, client.InNamespace(synthNS)); err != nil {
		t.Fatalf("list ITs: %v", err)
	}
	rvs := map[string]string{}
	for i := range ists.Items {
		rvs["ImageStreamTag/"+ists.Items[i].Name] = ists.Items[i].ResourceVersion
	}
	for i := range its.Items {
		rvs["ImageTag/"+its.Items[i].Name] = its.Items[i].ResourceVersion
	}
	return rvs
}

func expectEventContaining(t *testing.T, recorder *record.FakeRecorder, substr string) {
	t.Helper()
	for {
		select {
		case ev := <-recorder.Events:
			if strings.Contains(ev, substr) {
				return
			}
		default:
			t.Fatalf("no recorded event containing %q", substr)
		}
	}
}

func artifactFixture(digest string) *imageflowv1alpha1.ImageArtifact {
	return &imageflowv1alpha1.ImageArtifact{
		ObjectMeta: metav1.ObjectMeta{Name: ArtifactName(digest)},
		Spec:       imageflowv1alpha1.ImageArtifactSpec{Digest: digest},
	}
}

func TestReconcileSynthesizesMirrorsFromStreamStatus(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, _, _ := newMirrorTestReconciler(t, 5, stream, artifactFixture(digestA))

	reconcileOK(t, r)

	ist := getImageStreamTag(t, cl, synthStream+"--stable")
	if ist.Spec.Stream != synthStream || ist.Spec.Tag != tagStableUnit {
		t.Fatalf("IST spec = %#v", ist.Spec)
	}
	assertOwnerRef(t, ist.OwnerReferences, stream)
	if ist.Status.ObservedGeneration != stream.Generation {
		t.Fatalf("observedGeneration = %d, want %d", ist.Status.ObservedGeneration, stream.Generation)
	}
	if ist.Status.Image == nil || ist.Status.Image.Digest != digestA ||
		ist.Status.Image.ArtifactRef != "sha256-"+testHexA {
		t.Fatalf("IST image = %#v", ist.Status.Image)
	}
	if len(ist.Status.Events) != 2 {
		t.Fatalf("IST events = %#v", ist.Status.Events)
	}
	assertCondition(t, ist.Status.Conditions, mirrorConditionReady, metav1.ConditionTrue, mirrorReasonSynthesized)

	itA := getImageTag(t, cl, digestA)
	if itA.Spec.Stream != synthStream || itA.Spec.Digest != digestA {
		t.Fatalf("IT spec = %#v", itA.Spec)
	}
	assertOwnerRef(t, itA.OwnerReferences, stream)
	if itA.Status.Image == nil || itA.Status.Image.ArtifactRef != "sha256-"+testHexA {
		t.Fatalf("IT A image = %#v", itA.Status.Image)
	}

	// Digest B has no artifact: absence is tolerated, artifactRef stays empty.
	itB := getImageTag(t, cl, digestB)
	if itB.Status.Image == nil || itB.Status.Image.ArtifactRef != "" {
		t.Fatalf("IT B must tolerate the missing artifact: %#v", itB.Status.Image)
	}
	assertCondition(t, itB.Status.Conditions, mirrorConditionArtifactLinked, metav1.ConditionFalse, mirrorReasonArtifactAbsent)

	// Three ISTs (stable, edge, latest) and three ITs (A, B, C) exist.
	rvs := listMirrorRVs(t, cl)
	if len(rvs) != 6 {
		t.Fatalf("want 6 mirror objects, got %d: %v", len(rvs), rvs)
	}
}

func TestReconcileIsIdempotentWithoutWriteAmplification(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, _, fakeClock := newMirrorTestReconciler(t, 5, stream, artifactFixture(digestA))

	reconcileOK(t, r)
	first := listMirrorRVs(t, cl)

	// Advance the clock: nothing time-dependent may drift on a no-change pass
	// (condition transition times are preserved unless a status flips).
	fakeClock.Advance(time.Hour)
	reconcileOK(t, r)
	second := listMirrorRVs(t, cl)
	reconcileOK(t, r)
	third := listMirrorRVs(t, cl)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("second reconcile rewrote objects:\nfirst  = %v\nsecond = %v", first, second)
	}
	if !reflect.DeepEqual(second, third) {
		t.Fatalf("third reconcile rewrote objects:\nsecond = %v\nthird  %v", second, third)
	}
}

func TestReconcileAppliesEventLimitFlag(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, recorder, _ := newMirrorTestReconciler(t, 2, stream)

	reconcileOK(t, r)

	// "stable" has 2 history items; "edge" 1; digest A's ImageTag view sees
	// both associations (2 events). With limit 2 nothing is dropped yet — so
	// also push a third event via a Delivery entry (T11 write path) below.
	ist := getImageStreamTag(t, cl, synthStream+"--stable")
	if len(ist.Status.Events) != 2 {
		t.Fatalf("events = %#v", ist.Status.Events)
	}

	// Simulate the subscription controller floating delivery summaries up:
	// after this write the summary window (2) must keep only the newest two.
	ist.Status.Events = append(ist.Status.Events,
		deliveryEvent("sub-new", metav1.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)))
	if err := cl.Status().Update(context.Background(), ist); err != nil {
		t.Fatalf("inject delivery event: %v", err)
	}

	reconcileOK(t, r)
	ist = getImageStreamTag(t, cl, synthStream+"--stable")
	if len(ist.Status.Events) != 2 {
		t.Fatalf("events must be bounded to the flag value 2: %#v", ist.Status.Events)
	}
	if ist.Status.Events[0].Type != imageflowv1alpha1.MirrorEventDelivery {
		t.Fatalf("newest entry must be the delivery: %#v", ist.Status.Events[0])
	}
	expectEventContaining(t, recorder, "EventSummaryTruncated")
}

func TestReconcilePreservesDeliveryEventsFromSubscriptionController(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, _, _ := newMirrorTestReconciler(t, 5, stream)
	reconcileOK(t, r)

	// T11 write path: the subscription controller appends Delivery summaries
	// directly to the mirror status subresource.
	ist := getImageStreamTag(t, cl, synthStream+"--stable")
	delivery := deliveryEvent("sub-1", metav1.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))
	ist.Status.Events = append(ist.Status.Events, delivery)
	if err := cl.Status().Update(context.Background(), ist); err != nil {
		t.Fatalf("inject delivery event: %v", err)
	}

	reconcileOK(t, r)
	ist = getImageStreamTag(t, cl, synthStream+"--stable")
	if len(ist.Status.Events) != 3 {
		t.Fatalf("re-synthesis must keep the delivery entry alongside the 2 ImageChanged entries: %#v", ist.Status.Events)
	}
	if ist.Status.Events[0].Type != imageflowv1alpha1.MirrorEventDelivery ||
		ist.Status.Events[0].Subscription.Name != "sub-1" {
		t.Fatalf("delivery must float first (newest): %#v", ist.Status.Events[0])
	}
	if ist.Status.Events[1].Type != imageflowv1alpha1.MirrorEventImageChanged ||
		ist.Status.Events[2].Type != imageflowv1alpha1.MirrorEventImageChanged {
		t.Fatalf("ImageChanged entries must be preserved and deduplicated: %#v", ist.Status.Events)
	}
}

func TestReconcileCleansUpMirrorsAfterStreamDeletion(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, _, _ := newMirrorTestReconciler(t, 5, stream)
	reconcileOK(t, r)
	if len(listMirrorRVs(t, cl)) != 6 {
		t.Fatal("setup: expected 6 mirrors")
	}

	if err := cl.Delete(context.Background(), stream); err != nil {
		t.Fatalf("delete stream: %v", err)
	}
	reconcileOK(t, r) // NotFound path triggers the sweep

	if left := listMirrorRVs(t, cl); len(left) != 0 {
		t.Fatalf("all mirrors must be swept when the stream is gone: %v", left)
	}

	// Reconciling a stream that never existed is a silent no-op.
	reconcileOK(t, r)
}

func TestReconcilePrunesStaleMirrors(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, _, _ := newMirrorTestReconciler(t, 5, stream)
	reconcileOK(t, r)

	// "edge" leaves the status; digest B falls out of "stable" history; C and
	// the latest tag remain.
	fresh := &imageflowv1alpha1.ImageStream{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(stream), fresh); err != nil {
		t.Fatalf("get stream: %v", err)
	}
	fresh.Status.Tags = []imageflowv1alpha1.NamedTagEventList{
		{Tag: tagStableUnit, Items: []imageflowv1alpha1.TagEvent{tagEvent(digestA, refA, t3)}},
		{Tag: "latest", Items: []imageflowv1alpha1.TagEvent{tagEvent(digestC, refC, t0)}},
	}
	if err := cl.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("update stream status: %v", err)
	}

	reconcileOK(t, r)

	if err := cl.Get(context.Background(),
		client.ObjectKey{Namespace: synthNS, Name: synthStream + "--edge"},
		&imageflowv1alpha1.ImageStreamTag{}); !apierrors.IsNotFound(err) {
		t.Fatalf("edge mirror must be pruned, err = %v", err)
	}
	if err := cl.Get(context.Background(),
		client.ObjectKey{Namespace: synthNS, Name: EncodeImageTagName(synthStream, digestB)},
		&imageflowv1alpha1.ImageTag{}); !apierrors.IsNotFound(err) {
		t.Fatalf("ImageTag of digest B must be pruned when it left every history, err = %v", err)
	}
	getImageStreamTag(t, cl, synthStream+"--stable") // still there
	getImageTag(t, cl, digestA)
	getImageTag(t, cl, digestC)
}

func TestReconcileRestoresDriftedOwnerRef(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, _, _ := newMirrorTestReconciler(t, 5, stream)
	reconcileOK(t, r)

	ist := getImageStreamTag(t, cl, synthStream+"--stable")
	ist.OwnerReferences = nil // metadata is user-writable; ownerRef may drift
	if err := cl.Update(context.Background(), ist); err != nil {
		t.Fatalf("strip ownerRef: %v", err)
	}

	reconcileOK(t, r)
	ist = getImageStreamTag(t, cl, synthStream+"--stable")
	assertOwnerRef(t, ist.OwnerReferences, stream)
}

func TestReconcileLinksArtifactWhenItAppears(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, _, fakeClock := newMirrorTestReconciler(t, 5, stream)
	reconcileOK(t, r)

	ist := getImageStreamTag(t, cl, synthStream+"--stable")
	if ist.Status.Image.ArtifactRef != "" {
		t.Fatalf("artifactRef must stay empty while the artifact is absent: %#v", ist.Status.Image)
	}
	absent := meta.FindStatusCondition(ist.Status.Conditions, mirrorConditionArtifactLinked)
	if absent == nil || absent.Status != metav1.ConditionFalse {
		t.Fatalf("ArtifactLinked must be False while absent: %#v", absent)
	}

	if err := cl.Create(context.Background(), artifactFixture(digestA)); err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	fakeClock.Advance(time.Minute)
	reconcileOK(t, r)

	ist = getImageStreamTag(t, cl, synthStream+"--stable")
	if ist.Status.Image.ArtifactRef != "sha256-"+testHexA {
		t.Fatalf("artifactRef must link once the artifact exists: %#v", ist.Status.Image)
	}
	linked := meta.FindStatusCondition(ist.Status.Conditions, mirrorConditionArtifactLinked)
	if linked == nil || linked.Status != metav1.ConditionTrue || linked.Reason != mirrorReasonArtifactFound {
		t.Fatalf("ArtifactLinked must flip to True: %#v", linked)
	}
}

func TestReconcileSynthesizesConsumersFromSubscriptions(t *testing.T) {
	stream := synthStreamFixture()
	sub := synthSubFixture("team-a", "sub-1", synthStream+":stable")
	r, cl, _, _ := newMirrorTestReconciler(t, 5, stream, &sub)
	reconcileOK(t, r)

	want := []imageflowv1alpha1.ConsumerReference{{Namespace: "team-a", Name: "sub-1"}}
	ist := getImageStreamTag(t, cl, synthStream+"--stable")
	if !reflect.DeepEqual(ist.Status.Consumers, want) {
		t.Fatalf("IST consumers = %#v", ist.Status.Consumers)
	}
	itA := getImageTag(t, cl, digestA)
	if !reflect.DeepEqual(itA.Status.Consumers, want) {
		t.Fatalf("IT consumers (current digest) = %#v", itA.Status.Consumers)
	}
	// Historic digest B has no live consumers.
	itB := getImageTag(t, cl, digestB)
	if len(itB.Status.Consumers) != 0 {
		t.Fatalf("historic IT consumers = %#v", itB.Status.Consumers)
	}

	if err := cl.Delete(context.Background(), &sub); err != nil {
		t.Fatalf("delete sub: %v", err)
	}
	reconcileOK(t, r)
	ist = getImageStreamTag(t, cl, synthStream+"--stable")
	if len(ist.Status.Consumers) != 0 {
		t.Fatalf("consumers must disappear with the subscription: %#v", ist.Status.Consumers)
	}
}

func TestReconcileLeavesNameCollidingObjectUntouched(t *testing.T) {
	stream := synthStreamFixture()
	// An object squatting the encoded index name with a different spec
	// (unreachable behind the deterministic encoder short of hash
	// collisions): it must never be mutated or deleted.
	squatter := &imageflowv1alpha1.ImageStreamTag{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: synthNS,
			Name:      EncodeImageStreamTagName(synthStream, tagStableUnit),
		},
		Spec: imageflowv1alpha1.ImageStreamTagSpec{Stream: synthStream, Tag: "hijack"},
	}
	r, cl, recorder, _ := newMirrorTestReconciler(t, 5, stream, squatter)

	reconcileOK(t, r)

	after := getImageStreamTag(t, cl, squatter.Name)
	if after.Spec.Tag != "hijack" {
		t.Fatalf("squatter spec must be untouched: %#v", after.Spec)
	}
	if after.Status.Image != nil {
		t.Fatalf("squatter status must be untouched: %#v", after.Status)
	}
	expectEventContaining(t, recorder, "MirrorNameCollision")
}

func TestStreamsForSubscriptionMapper(t *testing.T) {
	r, _, _, _ := newMirrorTestReconciler(t, 5)
	ctx := context.Background()

	sub := synthSubFixture("team-a", "sub-1", synthStream+":stable")
	reqs := r.streamsForSubscription(ctx, &sub)
	want := []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: synthNS, Name: synthStream}}}
	if !reflect.DeepEqual(reqs, want) {
		t.Fatalf("requests = %#v, want %#v", reqs, want)
	}

	badKind := synthSubFixture("team-a", "sub-2", synthStream+":stable")
	badKind.Spec.Source.Kind = "ImageTag"
	if got := r.streamsForSubscription(ctx, &badKind); got != nil {
		t.Fatalf("non-ImageStreamTag source must map to nothing: %#v", got)
	}

	badName := synthSubFixture("team-a", "sub-3", "no-colon")
	if got := r.streamsForSubscription(ctx, &badName); got != nil {
		t.Fatalf("malformed source name must map to nothing: %#v", got)
	}
}

func TestStreamsForArtifactMapper(t *testing.T) {
	stream := synthStreamFixture()
	r, _, _, _ := newMirrorTestReconciler(t, 5, stream, artifactFixture(digestA))
	reconcileOK(t, r)

	// The artifact maps back to exactly one stream request even though both
	// an ImageTag and an ImageStreamTag mirror reference the digest.
	artifact := artifactFixture(digestA)
	reqs := r.streamsForArtifact(context.Background(), artifact)
	want := []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: synthNS, Name: synthStream}}}
	if !reflect.DeepEqual(reqs, want) {
		t.Fatalf("requests = %#v, want %#v", reqs, want)
	}

	// An artifact no mirror references maps to nothing.
	unknown := artifactFixture("sha256:" + hexC[:63] + "0")
	if got := r.streamsForArtifact(context.Background(), unknown); len(got) != 0 {
		t.Fatalf("unknown artifact must map to nothing: %#v", got)
	}
}

func TestReconcileWritesStatusThroughSubresourceOnly(t *testing.T) {
	stream := synthStreamFixture()
	r, cl, _, _ := newMirrorTestReconciler(t, 5, stream)
	reconcileOK(t, r)

	// User annotations are the only user-writable surface and must survive
	// synthesis untouched.
	ist := getImageStreamTag(t, cl, synthStream+"--stable")
	ist.Annotations = map[string]string{"imageflow.appfun.dev/note": "keep me"}
	if err := cl.Update(context.Background(), ist); err != nil {
		t.Fatalf("annotate: %v", err)
	}

	// Change a fact so the next pass performs a status write.
	fresh := &imageflowv1alpha1.ImageStream{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(stream), fresh); err != nil {
		t.Fatalf("get stream: %v", err)
	}
	fresh.Status.Tags[0].Items = append([]imageflowv1alpha1.TagEvent{
		tagEvent(digestC, refC, metav1.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)),
	}, fresh.Status.Tags[0].Items...)
	if err := cl.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("update stream status: %v", err)
	}

	reconcileOK(t, r)
	ist = getImageStreamTag(t, cl, synthStream+"--stable")
	if ist.Annotations["imageflow.appfun.dev/note"] != "keep me" {
		t.Fatalf("annotations must survive synthesis: %#v", ist.Annotations)
	}
	if ist.Status.Image == nil || ist.Status.Image.Digest != digestC {
		t.Fatalf("current association must follow the stream: %#v", ist.Status.Image)
	}
	if got := fmt.Sprintf("%v", ist.Status.Events[0].NewDigest); got != digestC {
		t.Fatalf("newest event must be the new association: %#v", ist.Status.Events[0])
	}
}
