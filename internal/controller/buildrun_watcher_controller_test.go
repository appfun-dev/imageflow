package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// T3 (ticket #16) unit tests: fake client + fake clock (seam S3), same
// pattern as mapping_controller_test.go (T4). BuildRun objects are Shipwright
// resources handled as unstructured (zero compile-time Shipwright dependency,
// ADR-001). Covered acceptance surface:
//   - filter: annotation buildrun.imageflow.appfun.dev/target + Succeeded=True
//   - digest gate: .status.output.digest non-empty -> RegisterImage equivalent
//     (RegistrationSourceBuildRun) + Registered=True/Succeeded
//   - empty digest -> Registered=False/MissingDigest + Warning Event
//   - invalid annotation -> Registered=False/InvalidTarget + Warning Event
//   - stream missing -> Registered=False/StreamNotFound + Warning Event,
//     recovers once the stream exists
//   - idempotence: same (stream, tag, digest) re-reconcile = no-op, no
//     duplicate Events / Image Changes / condition re-stamps
//   - transient errors requeue (returned error), NotFound BuildRun is a no-op

const (
	t16TestNS     = "build-test"
	t16TestStream = "payments-api"
	t16TestTag    = "stable"
	t16TestImage  = "quay.io/example/payments-api:latest"
	t16TestRepo   = "quay.io/example/payments-api"
)

var (
	t16TestT0  = time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	t16DigestA = "sha256:" + strings.Repeat("e", 64)
	t16DigestB = "sha256:" + strings.Repeat("f", 64)
	t16RefA    = t16TestRepo + "@" + t16DigestA
)

// --- test object builders ---------------------------------------------------

type buildRunOption func(*unstructured.Unstructured)

func newTestBuildRun(name string, opts ...buildRunOption) *unstructured.Unstructured {
	br := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "shipwright.io/v1beta1",
		"kind":       "BuildRun",
		"metadata": map[string]interface{}{
			"namespace": t16TestNS,
			"name":      name,
		},
	}}
	for _, opt := range opts {
		opt(br)
	}
	return br
}

//nolint:unparam // tag is parameterized for future test cases
func withTargetAnnotation(stream, tag string) buildRunOption {
	return func(br *unstructured.Unstructured) {
		br.SetAnnotations(map[string]string{
			"buildrun.imageflow.appfun.dev/target": `{"stream":"` + stream + `","tag":"` + tag + `"}`,
		})
	}
}

func withRawTargetAnnotation(value string) buildRunOption {
	return func(br *unstructured.Unstructured) {
		br.SetAnnotations(map[string]string{
			"buildrun.imageflow.appfun.dev/target": value,
		})
	}
}

// withSucceeded sets the Shipwright Succeeded condition ("True"/"False").
func withSucceeded(status string) buildRunOption {
	return func(br *unstructured.Unstructured) {
		_ = unstructured.SetNestedSlice(br.Object, []interface{}{
			map[string]interface{}{
				"type":               "Succeeded",
				"status":             status,
				"reason":             "Completed",
				"lastTransitionTime": "2026-02-01T23:59:00Z",
			},
		}, "status", "conditions")
	}
}

// withOutput sets .status.output.digest and .status.buildSpec.output.image,
// the two Shipwright status fields the watcher consumes (quickstart.md).
func withOutput(digest, image string) buildRunOption {
	return func(br *unstructured.Unstructured) {
		if digest != "" {
			_ = unstructured.SetNestedField(br.Object, digest, "status", "output", "digest")
		}
		if image != "" {
			_ = unstructured.SetNestedField(br.Object, image, "status", "buildSpec", "output", "image")
		}
	}
}

// --- test environment -------------------------------------------------------

type t16TestEnv struct {
	t          *testing.T
	client     client.Client
	recorder   *record.FakeRecorder
	clock      *clock.Fake
	reconciler *BuildRunReconciler
}

func newT16TestEnv(t *testing.T, funcs *interceptor.Funcs, objs ...client.Object) *t16TestEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add clientgo scheme: %v", err)
	}
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add imageflow scheme: %v", err)
	}
	buildRunPrototype := &unstructured.Unstructured{}
	buildRunPrototype.SetGroupVersionKind(buildRunGVK)
	builder := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&imageflowv1alpha1.ImageStream{},
			&imageflowv1alpha1.ImageArtifact{},
			buildRunPrototype,
		).
		WithObjects(objs...)
	if funcs != nil {
		builder = builder.WithInterceptorFuncs(*funcs)
	}
	env := &t16TestEnv{
		t:        t,
		client:   builder.Build(),
		recorder: record.NewFakeRecorder(256),
		clock:    clock.NewFake(t16TestT0),
	}
	env.reconciler = NewBuildRunReconciler(env.client, nil, env.recorder, env.clock, config.Config{
		StreamTagHistoryLimit: imageflowv1alpha1.StreamTagHistoryMaxItems,
	})
	return env
}

func (e *t16TestEnv) reconcile(obj client.Object) (ctrl.Result, error) {
	e.t.Helper()
	return e.reconciler.Reconcile(context.Background(), requestFor(obj))
}

func (e *t16TestEnv) getBuildRun(name string) *unstructured.Unstructured {
	e.t.Helper()
	br := &unstructured.Unstructured{}
	br.SetGroupVersionKind(buildRunGVK)
	key := types.NamespacedName{Namespace: t16TestNS, Name: name}
	if err := e.client.Get(context.Background(), key, br); err != nil {
		e.t.Fatalf("get buildrun %q: %v", name, err)
	}
	return br
}

func (e *t16TestEnv) getStream(name string) *imageflowv1alpha1.ImageStream {
	e.t.Helper()
	var s imageflowv1alpha1.ImageStream
	key := types.NamespacedName{Namespace: t16TestNS, Name: name}
	if err := e.client.Get(context.Background(), key, &s); err != nil {
		e.t.Fatalf("get stream %q: %v", name, err)
	}
	return &s
}

func newT16TestStream(name string, generation int64) *imageflowv1alpha1.ImageStream {
	return &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: t16TestNS, Name: name, Generation: generation},
	}
}

// registeredCondition returns the Registered condition of the BuildRun as a
// plain map, or nil when absent.
func registeredCondition(br *unstructured.Unstructured) map[string]interface{} {
	conds, found, err := unstructured.NestedSlice(br.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if m["type"] == "Registered" {
			return m
		}
	}
	return nil
}

func requireRegisteredCondition(t *testing.T, br *unstructured.Unstructured, wantStatus, wantReason string) map[string]interface{} {
	t.Helper()
	cond := registeredCondition(br)
	if cond == nil {
		t.Fatalf("expected Registered condition, got none (conditions: %v)", br.Object["status"])
	}
	if got := cond["status"]; got != wantStatus {
		t.Fatalf("Registered status = %v, want %v (condition: %v)", got, wantStatus, cond)
	}
	if got := cond["reason"]; got != wantReason {
		t.Fatalf("Registered reason = %v, want %v (condition: %v)", got, wantReason, cond)
	}
	return cond
}

func requireNoRegisteredCondition(t *testing.T, br *unstructured.Unstructured) {
	t.Helper()
	if cond := registeredCondition(br); cond != nil {
		t.Fatalf("expected no Registered condition, got %v", cond)
	}
}

func requireNoEvents(t *testing.T, env *t16TestEnv) {
	t.Helper()
	if events := drainFakeEvents(env.recorder); len(events) != 0 {
		t.Fatalf("expected no events, got %v", events)
	}
}

// --- happy path -------------------------------------------------------------

func TestBuildRunWatcherReconcileRegistersDigestAndSetsRegisteredTrue(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 7)
	br := newTestBuildRun("br-1", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	env := newT16TestEnv(t, nil, stream, br)

	res, err := env.reconcile(br)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Fatalf("expected no requeue, got %+v", res)
	}

	t.Run("stream status association", func(t *testing.T) {
		got := env.getStream(t16TestStream)
		if len(got.Status.Tags) != 1 {
			t.Fatalf("expected 1 tag entry, got %d", len(got.Status.Tags))
		}
		entry := got.Status.Tags[0]
		if entry.Tag != t16TestTag || len(entry.Items) != 1 {
			t.Fatalf("unexpected tag entry: %+v", entry)
		}
		item := entry.Items[0]
		if item.Digest != t16DigestA {
			t.Fatalf("digest = %q, want %q", item.Digest, t16DigestA)
		}
		// image tag is stripped: canonical digest reference <repo>@<digest>.
		if item.ImageReference != t16RefA {
			t.Fatalf("imageReference = %q, want %q (tag stripped)", item.ImageReference, t16RefA)
		}
		if !item.Created.Time.Equal(t16TestT0) {
			t.Fatalf("created must come from the injected clock: %v", item.Created.Time)
		}
	})

	t.Run("content-addressed artifact created", func(t *testing.T) {
		var a imageflowv1alpha1.ImageArtifact
		name := "sha256-" + strings.Repeat("e", 64)
		if err := env.client.Get(context.Background(), types.NamespacedName{Name: name}, &a); err != nil {
			t.Fatalf("get artifact: %v", err)
		}
		if a.Spec.Digest != t16DigestA || a.Spec.ImageReference != t16RefA {
			t.Fatalf("unexpected artifact spec: %+v", a.Spec)
		}
	})

	t.Run("Registered=True condition on BuildRun", func(t *testing.T) {
		cond := requireRegisteredCondition(t, env.getBuildRun("br-1"), "True", "Succeeded")
		msg, _ := cond["message"].(string)
		for _, want := range []string{t16RefA, t16TestStream, t16TestTag} {
			if !strings.Contains(msg, want) {
				t.Fatalf("Registered message %q must mention %q", msg, want)
			}
		}
		if cond["lastTransitionTime"] != t16TestT0.UTC().Format("2006-01-02T15:04:05Z") {
			t.Fatalf("lastTransitionTime must come from the injected clock: %v", cond["lastTransitionTime"])
		}
	})

	t.Run("audit events", func(t *testing.T) {
		events := drainFakeEvents(env.recorder)
		if n := countEventsWithReason(events, EventReasonImageArtifactCreated); n != 1 {
			t.Fatalf("expected 1 ImageArtifactCreated event, got %d: %v", n, events)
		}
		if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
			t.Fatalf("expected 1 ImageChanged event, got %d: %v", n, events)
		}
		changed := findEventWithReason(events, EventReasonImageChanged)
		if !strings.Contains(changed, "BuildRun") || !strings.Contains(changed, "br-1") {
			t.Fatalf("ImageChanged event must attribute the BuildRun source, got %q", changed)
		}
		if n := countEventsWithReason(events, EventReasonBuildRunRegistered); n != 1 {
			t.Fatalf("expected 1 BuildRunRegistered event, got %d: %v", n, events)
		}
	})
}

func TestBuildRunWatcherReconcileRegistryWithPortKeepsPort(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 1)
	br := newTestBuildRun("br-port", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, "localhost:5000/example/payments-api:dev"))
	env := newT16TestEnv(t, nil, stream, br)

	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	item := env.getStream(t16TestStream).Status.Tags[0].Items[0]
	want := "localhost:5000/example/payments-api@" + t16DigestA
	if item.ImageReference != want {
		t.Fatalf("imageReference = %q, want %q (registry port kept, tag stripped)", item.ImageReference, want)
	}
}

// --- filter: annotation + Succeeded=True ------------------------------------

func TestBuildRunWatcherReconcileNoAnnotationIsNoop(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 1)
	br := newTestBuildRun("br-noann", withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	env := newT16TestEnv(t, nil, stream, br)

	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := env.getStream(t16TestStream); len(got.Status.Tags) != 0 {
		t.Fatalf("expected no stream effects, got %+v", got.Status.Tags)
	}
	requireNoRegisteredCondition(t, env.getBuildRun("br-noann"))
	requireNoEvents(t, env)
}

func TestBuildRunWatcherReconcileNotSucceededIsNoop(t *testing.T) {
	for _, status := range []string{"False", "Unknown"} {
		t.Run("Succeeded="+status, func(t *testing.T) {
			stream := newT16TestStream(t16TestStream, 1)
			br := newTestBuildRun("br-failed", withTargetAnnotation(t16TestStream, t16TestTag),
				withSucceeded(status), withOutput(t16DigestA, t16TestImage))
			env := newT16TestEnv(t, nil, stream, br)

			if _, err := env.reconcile(br); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if got := env.getStream(t16TestStream); len(got.Status.Tags) != 0 {
				t.Fatalf("BuildRun not Succeeded must not register, got %+v", got.Status.Tags)
			}
			requireNoRegisteredCondition(t, env.getBuildRun("br-failed"))
			requireNoEvents(t, env)
		})
	}

	t.Run("no conditions yet", func(t *testing.T) {
		stream := newT16TestStream(t16TestStream, 1)
		br := newTestBuildRun("br-running", withTargetAnnotation(t16TestStream, t16TestTag))
		env := newT16TestEnv(t, nil, stream, br)

		if _, err := env.reconcile(br); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if got := env.getStream(t16TestStream); len(got.Status.Tags) != 0 {
			t.Fatalf("running BuildRun must not register, got %+v", got.Status.Tags)
		}
		requireNoRegisteredCondition(t, env.getBuildRun("br-running"))
		requireNoEvents(t, env)
	})
}

func TestBuildRunWatcherReconcileDeletedBuildRunIsNoop(t *testing.T) {
	env := newT16TestEnv(t, nil)
	br := newTestBuildRun("br-gone", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("reconcile of deleted BuildRun must be a no-op, got %v", err)
	}
	requireNoEvents(t, env)
}

// --- digest gate: MissingDigest ---------------------------------------------

func TestBuildRunWatcherReconcileEmptyDigestSetsMissingDigest(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 1)
	br := newTestBuildRun("br-nodigest", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput("", t16TestImage))
	env := newT16TestEnv(t, nil, stream, br)

	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("missing digest is terminal for this BuildRun, not a requeue: %v", err)
	}

	cond := requireRegisteredCondition(t, env.getBuildRun("br-nodigest"), "False", "MissingDigest")
	msg, _ := cond["message"].(string)
	if !strings.Contains(msg, ".status.output.digest") || !strings.Contains(msg, "system results") {
		t.Fatalf("MissingDigest message must reference .status.output.digest and system results, got %q", msg)
	}

	if got := env.getStream(t16TestStream); len(got.Status.Tags) != 0 {
		t.Fatalf("missing digest must not register, got %+v", got.Status.Tags)
	}

	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonBuildRunFailed); n != 1 {
		t.Fatalf("expected 1 BuildRunFailed warning event, got %d: %v", n, events)
	}
	if ev := findEventWithReason(events, EventReasonBuildRunFailed); !strings.HasPrefix(ev, corev1.EventTypeWarning+" ") {
		t.Fatalf("BuildRunFailed must be a Warning event, got %q", ev)
	}
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 0 {
		t.Fatalf("missing digest must not produce ImageChanged, got %v", events)
	}
}

// --- failure path: stream not found -----------------------------------------

func TestBuildRunWatcherReconcileStreamNotFound(t *testing.T) {
	br := newTestBuildRun("br-orphan", withTargetAnnotation("no-such-stream", t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	env := newT16TestEnv(t, nil, br)

	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("stream-not-found is terminal for this BuildRun, not a requeue: %v", err)
	}

	cond := requireRegisteredCondition(t, env.getBuildRun("br-orphan"), "False", "StreamNotFound")
	msg, _ := cond["message"].(string)
	if !strings.Contains(msg, "no-such-stream") || !strings.Contains(msg, t16TestNS) {
		t.Fatalf("StreamNotFound message must name stream and namespace, got %q", msg)
	}

	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonBuildRunFailed); n != 1 {
		t.Fatalf("expected 1 BuildRunFailed warning event, got %d: %v", n, events)
	}

	t.Run("recovers once the stream exists", func(t *testing.T) {
		stream := newT16TestStream("no-such-stream", 1)
		if err := env.client.Create(context.Background(), stream); err != nil {
			t.Fatalf("create stream: %v", err)
		}
		env.clock.Advance(5 * time.Minute)
		if _, err := env.reconcile(br); err != nil {
			t.Fatalf("reconcile after stream creation: %v", err)
		}
		requireRegisteredCondition(t, env.getBuildRun("br-orphan"), "True", "Succeeded")
		if got := env.getStream("no-such-stream"); len(got.Status.Tags) != 1 {
			t.Fatalf("expected registration after stream creation, got %+v", got.Status.Tags)
		}
		events := drainFakeEvents(env.recorder)
		if n := countEventsWithReason(events, EventReasonBuildRunRegistered); n != 1 {
			t.Fatalf("expected 1 BuildRunRegistered after recovery, got %d: %v", n, events)
		}
	})
}

// --- failure path: invalid annotation ---------------------------------------

func TestBuildRunWatcherReconcileInvalidAnnotation(t *testing.T) {
	cases := map[string]string{
		"not json":       `not-json`,
		"missing tag":    `{"stream":"payments-api"}`,
		"missing stream": `{"tag":"stable"}`,
		"empty stream":   `{"stream":"","tag":"stable"}`,
	}
	for name, annotation := range cases {
		t.Run(name, func(t *testing.T) {
			stream := newT16TestStream(t16TestStream, 1)
			br := newTestBuildRun("br-badann", withRawTargetAnnotation(annotation),
				withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
			env := newT16TestEnv(t, nil, stream, br)

			if _, err := env.reconcile(br); err != nil {
				t.Fatalf("invalid annotation must not requeue (admission should have rejected it): %v", err)
			}
			requireRegisteredCondition(t, env.getBuildRun("br-badann"), "False", "InvalidTarget")
			if got := env.getStream(t16TestStream); len(got.Status.Tags) != 0 {
				t.Fatalf("invalid annotation must not register, got %+v", got.Status.Tags)
			}
			events := drainFakeEvents(env.recorder)
			if n := countEventsWithReason(events, EventReasonBuildRunFailed); n != 1 {
				t.Fatalf("expected 1 BuildRunFailed warning event, got %d: %v", n, events)
			}
		})
	}
}

// --- failure path: no image reference available -----------------------------

func TestBuildRunWatcherReconcileMissingImageReference(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 1)
	br := newTestBuildRun("br-noimage", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, ""))
	env := newT16TestEnv(t, nil, stream, br)

	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("missing image reference is terminal for this BuildRun, not a requeue: %v", err)
	}
	requireRegisteredCondition(t, env.getBuildRun("br-noimage"), "False", "MissingImageReference")
	if got := env.getStream(t16TestStream); len(got.Status.Tags) != 0 {
		t.Fatalf("missing image reference must not register, got %+v", got.Status.Tags)
	}
	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonBuildRunFailed); n != 1 {
		t.Fatalf("expected 1 BuildRunFailed warning event, got %d: %v", n, events)
	}
}

// --- idempotence --------------------------------------------------------------

func TestBuildRunWatcherReconcileIdempotentRepeatHasNoDuplicates(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 1)
	br := newTestBuildRun("br-idem", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	env := newT16TestEnv(t, nil, stream, br)

	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	first := drainFakeEvents(env.recorder)
	if len(first) == 0 {
		t.Fatalf("expected events from first reconcile")
	}
	firstCondition := registeredCondition(env.getBuildRun("br-idem"))

	env.clock.Advance(10 * time.Minute)
	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	if got := env.getStream(t16TestStream).Status.Tags[0].Items; len(got) != 1 {
		t.Fatalf("idempotent re-registration must not append history, got %d items", len(got))
	}
	secondCondition := registeredCondition(env.getBuildRun("br-idem"))
	if secondCondition["lastTransitionTime"] != firstCondition["lastTransitionTime"] {
		t.Fatalf("no-op reconcile must not re-stamp the condition: %v -> %v",
			firstCondition["lastTransitionTime"], secondCondition["lastTransitionTime"])
	}
	requireNoEvents(t, env)
}

func TestBuildRunWatcherReconcileSecondBuildRunSameTupleNoDuplicateImageChange(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 1)
	first := newTestBuildRun("br-1", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	env := newT16TestEnv(t, nil, stream, first)

	if _, err := env.reconcile(first); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
		t.Fatalf("expected 1 ImageChanged after first registration, got %d", n)
	}

	// A second BuildRun (e.g. rebuild producing identical content) registers
	// the same (stream, tag, digest): no duplicate Image Change on the stream,
	// but the second BuildRun still gets its own Registered audit trail.
	second := newTestBuildRun("br-2", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	if err := env.client.Create(context.Background(), second); err != nil {
		t.Fatalf("create second buildrun: %v", err)
	}
	env.clock.Advance(5 * time.Minute)
	if _, err := env.reconcile(second); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	if got := env.getStream(t16TestStream).Status.Tags[0].Items; len(got) != 1 {
		t.Fatalf("same tuple must not append stream history, got %d items", len(got))
	}
	requireRegisteredCondition(t, env.getBuildRun("br-2"), "True", "Succeeded")
	events = drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 0 {
		t.Fatalf("same tuple must not produce a second ImageChanged, got %v", events)
	}
	if n := countEventsWithReason(events, EventReasonBuildRunRegistered); n != 1 {
		t.Fatalf("second BuildRun gets its own BuildRunRegistered audit event, got %d: %v", n, events)
	}
}

func TestBuildRunWatcherReconcileNewDigestRecordsNewImageChange(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 1)
	br := newTestBuildRun("br-rebuild", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	env := newT16TestEnv(t, nil, stream, br)

	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	drainFakeEvents(env.recorder)

	// The BuildRun output moved to a new digest (status is controller-owned and
	// may be updated by the build system in place for retries).
	fresh := env.getBuildRun("br-rebuild")
	_ = unstructured.SetNestedField(fresh.Object, t16DigestB, "status", "output", "digest")
	if err := env.client.Status().Update(context.Background(), fresh); err != nil {
		t.Fatalf("update buildrun digest: %v", err)
	}
	env.clock.Advance(30 * time.Minute)
	if _, err := env.reconcile(br); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	items := env.getStream(t16TestStream).Status.Tags[0].Items
	if len(items) != 2 || items[0].Digest != t16DigestB {
		t.Fatalf("expected new digest prepended to history, got %+v", items)
	}
	cond := requireRegisteredCondition(t, env.getBuildRun("br-rebuild"), "True", "Succeeded")
	if msg, _ := cond["message"].(string); !strings.Contains(msg, t16DigestB) {
		t.Fatalf("condition message must reflect the new digest, got %q", msg)
	}
	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
		t.Fatalf("expected 1 ImageChanged for the new digest, got %d: %v", n, events)
	}
	if n := countEventsWithReason(events, EventReasonBuildRunRegistered); n != 1 {
		t.Fatalf("expected 1 BuildRunRegistered for the new digest, got %d: %v", n, events)
	}
}

// --- transient errors ---------------------------------------------------------

func TestBuildRunWatcherReconcileTransientErrorRequeues(t *testing.T) {
	stream := newT16TestStream(t16TestStream, 1)
	br := newTestBuildRun("br-flake", withTargetAnnotation(t16TestStream, t16TestTag),
		withSucceeded("True"), withOutput(t16DigestA, t16TestImage))
	failGetStreams := &interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*imageflowv1alpha1.ImageStream); ok {
				return errInjectedForTest
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
	env := newT16TestEnv(t, failGetStreams, stream, br)

	if _, err := env.reconcile(br); err == nil {
		t.Fatalf("transient stream read failure must requeue (returned error)")
	}
	requireNoRegisteredCondition(t, env.getBuildRun("br-flake"))
}

var errInjectedForTest = errors.New("injected transient failure")
