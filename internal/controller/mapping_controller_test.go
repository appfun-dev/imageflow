package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

// T4 unit tests: fake client + fake clock (seam S3) + interceptor assertions.
// They cover the acceptance surface that does not need a real API server:
// idempotent registration, ImageArtifact dedup, Image Change detection,
// bounded retention, failure taxonomy, created-by annotation preservation and
// TTL GC decisions. The envtest named cases live in
// test/integration/mapping_controller_test.go.

const (
	t4TestNS     = "catalog-test"
	t4TestStream = "payments-api"
	t4TestTag    = "stable"
	t4TestRepo   = "quay.io/example/payments-api"
	// t4CreatedByAnnotation is injected by the T3 mutating webhook at admission;
	// T4 controllers must never strip or rewrite it (they write status only).
	t4CreatedByAnnotation = "imageflow.appfun.dev/created-by"
)

var (
	t4TestT0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t4HexA    = strings.Repeat("a", 64)
	t4HexB    = strings.Repeat("b", 64)
	t4HexC    = strings.Repeat("c", 64)
	t4HexD    = strings.Repeat("d", 64)
	t4DigestA = "sha256:" + t4HexA
	t4DigestB = "sha256:" + t4HexB
	t4DigestC = "sha256:" + t4HexC
	t4DigestD = "sha256:" + t4HexD
	t4RefA    = t4TestRepo + "@" + t4DigestA
	t4RefB    = t4TestRepo + "@" + t4DigestB
	t4RefC    = t4TestRepo + "@" + t4DigestC
	t4RefD    = t4TestRepo + "@" + t4DigestD
)

// t4TestEnv bundles the fake collaborators of one reconciler under test.
type t4TestEnv struct {
	t        *testing.T
	client   client.Client
	recorder *record.FakeRecorder
	clock    *clock.Fake
	cfg      config.Config
	metrics  *mappingMetrics
	mapping  *MappingReconciler
}

func newT4TestEnv(t *testing.T, funcs *interceptor.Funcs, objs ...client.Object) *t4TestEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add clientgo scheme: %v", err)
	}
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add imageflow scheme: %v", err)
	}
	builder := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(
			&imageflowv1alpha1.ImageStreamMapping{},
			&imageflowv1alpha1.ImageStreamImport{},
			&imageflowv1alpha1.ImageStream{},
			&imageflowv1alpha1.ImageArtifact{},
		).
		WithObjects(objs...)
	if funcs != nil {
		builder = builder.WithInterceptorFuncs(*funcs)
	}
	env := &t4TestEnv{
		t:        t,
		client:   builder.Build(),
		recorder: record.NewFakeRecorder(256),
		clock:    clock.NewFake(t4TestT0),
		cfg: config.Config{
			MappingTTLSuccess:     time.Hour,
			MappingTTLFailure:     24 * time.Hour,
			StreamTagHistoryLimit: imageflowv1alpha1.StreamTagHistoryMaxItems,
		},
		metrics: newMappingMetrics(),
	}
	env.mapping = NewMappingReconciler(env.client, nil, env.recorder, env.clock, env.cfg, env.metrics)
	return env
}

func (e *t4TestEnv) newTaskGC(kind string) *taskGCReconciler {
	return &taskGCReconciler{
		client:   e.client,
		recorder: e.recorder,
		clock:    e.clock,
		config:   e.cfg,
		metrics:  e.metrics,
		kind:     kind,
	}
}

func (e *t4TestEnv) reconcileMapping(obj client.Object) (ctrl.Result, error) {
	e.t.Helper()
	return e.mapping.Reconcile(context.Background(), requestFor(obj))
}

func (e *t4TestEnv) reconcileGC(gc *taskGCReconciler, obj client.Object) (ctrl.Result, error) {
	e.t.Helper()
	return gc.Reconcile(context.Background(), requestFor(obj))
}

func (e *t4TestEnv) getMapping(name string) *imageflowv1alpha1.ImageStreamMapping {
	e.t.Helper()
	var m imageflowv1alpha1.ImageStreamMapping
	key := types.NamespacedName{Namespace: t4TestNS, Name: name}
	if err := e.client.Get(context.Background(), key, &m); err != nil {
		e.t.Fatalf("get mapping %q: %v", name, err)
	}
	return &m
}

func (e *t4TestEnv) getStream() *imageflowv1alpha1.ImageStream {
	e.t.Helper()
	var s imageflowv1alpha1.ImageStream
	key := types.NamespacedName{Namespace: t4TestNS, Name: t4TestStream}
	if err := e.client.Get(context.Background(), key, &s); err != nil {
		e.t.Fatalf("get stream %q: %v", t4TestStream, err)
	}
	return &s
}

func (e *t4TestEnv) getArtifact(name string) *imageflowv1alpha1.ImageArtifact {
	e.t.Helper()
	var a imageflowv1alpha1.ImageArtifact
	if err := e.client.Get(context.Background(), types.NamespacedName{Name: name}, &a); err != nil {
		e.t.Fatalf("get artifact %q: %v", name, err)
	}
	return &a
}

func requestFor(obj client.Object) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}}
}

func newTestStream(generation int64) *imageflowv1alpha1.ImageStream {
	return &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: t4TestNS, Name: t4TestStream, Generation: generation},
	}
}

func newTestMapping(name, image string) *imageflowv1alpha1.ImageStreamMapping {
	return &imageflowv1alpha1.ImageStreamMapping{
		ObjectMeta: metav1.ObjectMeta{Namespace: t4TestNS, Name: name},
		Spec: imageflowv1alpha1.ImageStreamMappingSpec{
			Stream: t4TestStream,
			Tag:    t4TestTag,
			Image:  image,
		},
	}
}

func newTerminalMapping(name string, succeeded bool, completedAt time.Time) *imageflowv1alpha1.ImageStreamMapping {
	m := newTestMapping(name, t4RefA)
	applyTerminalTaskStatus(&m.Status.TaskObjectStatus, succeeded, completedAt)
	return m
}

func newTerminalImport(name string, succeeded bool, completedAt time.Time) *imageflowv1alpha1.ImageStreamImport {
	task := &imageflowv1alpha1.ImageStreamImport{
		ObjectMeta: metav1.ObjectMeta{Namespace: t4TestNS, Name: name},
		Spec: imageflowv1alpha1.ImageStreamImportSpec{
			Stream: t4TestStream,
			Tag:    t4TestTag,
		},
	}
	applyTerminalTaskStatus(&task.Status.TaskObjectStatus, succeeded, completedAt)
	return task
}

func applyTerminalTaskStatus(st *imageflowv1alpha1.TaskObjectStatus, succeeded bool, completedAt time.Time) {
	reason := imageflowv1alpha1.ReasonFailed
	if succeeded {
		reason = imageflowv1alpha1.ReasonSucceeded
	}
	startedAt := metav1.NewTime(completedAt.Add(-time.Minute))
	done := metav1.NewTime(completedAt)
	st.StartedAt = &startedAt
	st.CompletedAt = &done
	st.Conditions = []metav1.Condition{{
		Type:               imageflowv1alpha1.ConditionTypeComplete,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            "terminal test state",
		LastTransitionTime: done,
	}}
}

func drainFakeEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// FakeRecorder strings look like "<type> <reason> <message>".
func countEventsWithReason(events []string, reason string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, " "+reason+" ") {
			n++
		}
	}
	return n
}

func findEventWithReason(events []string, reason string) string {
	for _, e := range events {
		if strings.Contains(e, " "+reason+" ") {
			return e
		}
	}
	return ""
}

func t4CounterValue(t *testing.T, vec *prometheus.CounterVec, label string) float64 {
	t.Helper()
	return testutil.ToFloat64(vec.WithLabelValues(label))
}

func t4HistogramCount(t *testing.T, vec *prometheus.HistogramVec, label string) uint64 {
	t.Helper()
	var pb dto.Metric
	if err := vec.WithLabelValues(label).(prometheus.Metric).Write(&pb); err != nil {
		t.Fatalf("write histogram metric: %v", err)
	}
	return pb.GetHistogram().GetSampleCount()
}

func completeCondition(m *imageflowv1alpha1.ImageStreamMapping) *metav1.Condition {
	for i := range m.Status.Conditions {
		if m.Status.Conditions[i].Type == imageflowv1alpha1.ConditionTypeComplete {
			return &m.Status.Conditions[i]
		}
	}
	return nil
}

// --- registration happy path ------------------------------------------------

func TestMappingReconcileRegistersDigestAndCompletesTask(t *testing.T) {
	stream := newTestStream(7)
	mapping := newTestMapping("task-1", t4RefA)
	env := newT4TestEnv(t, nil, stream, mapping)

	res, err := env.reconcileMapping(mapping)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Fatalf("expected no requeue, got %+v", res)
	}

	t.Run("stream status association", func(t *testing.T) { assertStreamAssociation(t, env, 7) })
	t.Run("content-addressed artifact created once", func(t *testing.T) { assertArtifactA(t, env) })
	t.Run("terminal task status", func(t *testing.T) { assertTerminalSuccessA(t, env, "task-1") })
	t.Run("audit events", func(t *testing.T) { assertFirstRegistrationEvents(t, env) })
	t.Run("metrics", func(t *testing.T) { assertSuccessMetrics(t, env) })
}

func assertStreamAssociation(t *testing.T, env *t4TestEnv, wantGeneration int64) {
	t.Helper()
	got := env.getStream()
	if len(got.Status.Tags) != 1 {
		t.Fatalf("expected 1 tag entry, got %d", len(got.Status.Tags))
	}
	entry := got.Status.Tags[0]
	if entry.Tag != t4TestTag || len(entry.Items) != 1 {
		t.Fatalf("unexpected tag entry: %+v", entry)
	}
	item := entry.Items[0]
	if item.Digest != t4DigestA || item.ImageReference != t4RefA {
		t.Fatalf("unexpected association: %+v", item)
	}
	if !item.Created.Time.Equal(t4TestT0) {
		t.Fatalf("created must come from the injected clock: %v", item.Created.Time)
	}
	if item.Generation != wantGeneration {
		t.Fatalf("expected recorded generation %d, got %d", wantGeneration, item.Generation)
	}
	if got.Status.ObservedGeneration != wantGeneration {
		t.Fatalf("expected observedGeneration %d, got %d", wantGeneration, got.Status.ObservedGeneration)
	}
}

func assertArtifactA(t *testing.T, env *t4TestEnv) {
	t.Helper()
	artifact := env.getArtifact("sha256-" + t4HexA)
	if artifact.Spec.Digest != t4DigestA || artifact.Spec.ImageReference != t4RefA {
		t.Fatalf("unexpected artifact spec: %+v", artifact.Spec)
	}
}

func assertTerminalSuccessA(t *testing.T, env *t4TestEnv, name string) {
	t.Helper()
	m := env.getMapping(name)
	cond := completeCondition(m)
	if cond == nil || cond.Status != metav1.ConditionTrue ||
		cond.Reason != imageflowv1alpha1.ReasonSucceeded {
		t.Fatalf("expected Complete=True/Succeeded, got %+v", cond)
	}
	if m.Status.StartedAt == nil || !m.Status.StartedAt.Time.Equal(t4TestT0) {
		t.Fatalf("startedAt must be the injected clock time: %+v", m.Status.StartedAt)
	}
	if m.Status.CompletedAt == nil || !m.Status.CompletedAt.Time.Equal(t4TestT0) {
		t.Fatalf("completedAt must be the injected clock time: %+v", m.Status.CompletedAt)
	}
	if !cond.LastTransitionTime.Time.Equal(t4TestT0) {
		t.Fatalf("condition timestamp must come from the injected clock: %v", cond.LastTransitionTime)
	}
	if m.Status.Digest != t4DigestA || m.Status.ArtifactRef != "sha256-"+t4HexA {
		t.Fatalf("unexpected result fields: digest=%q artifactRef=%q",
			m.Status.Digest, m.Status.ArtifactRef)
	}
}

func assertFirstRegistrationEvents(t *testing.T, env *t4TestEnv) {
	t.Helper()
	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
		t.Fatalf("expected 1 ImageChanged event, got %d: %v", n, events)
	}
	changed := findEventWithReason(events, EventReasonImageChanged)
	for _, want := range []string{"ImageStreamMapping", "task-1", t4DigestA, "previous: none"} {
		if !strings.Contains(changed, want) {
			t.Fatalf("ImageChanged event %q must mention %q", changed, want)
		}
	}
	if n := countEventsWithReason(events, EventReasonImageArtifactCreated); n != 1 {
		t.Fatalf("expected 1 ImageArtifactCreated event, got %d", n)
	}
	if n := countEventsWithReason(events, EventReasonMappingRegistered); n != 1 {
		t.Fatalf("expected 1 MappingRegistered event, got %d", n)
	}
}

func assertSuccessMetrics(t *testing.T, env *t4TestEnv) {
	t.Helper()
	if v := t4CounterValue(t, env.metrics.registrations, regOutcomeChanged); v != 1 {
		t.Fatalf("registrations{changed} = %v, want 1", v)
	}
	if n := t4HistogramCount(t, env.metrics.taskDuration, taskResultSuccess); n != 1 {
		t.Fatalf("task_duration{success} samples = %d, want 1", n)
	}
}

// --- idempotence: same (stream, tag, digest) --------------------------------

func TestMappingReconcileSameTupleIsNoopWithoutDuplicateImageChange(t *testing.T) {
	stream := newTestStream(3)
	first := newTestMapping("task-1", t4RefA)
	env := newT4TestEnv(t, nil, stream, first)

	if _, err := env.reconcileMapping(first); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	afterFirst := env.getStream().Status.Tags
	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
		t.Fatalf("expected 1 ImageChanged after first registration, got %d", n)
	}

	// Pipeline retry: a NEW task object registering the same tuple.
	env.clock.Advance(5 * time.Minute)
	retryTask := newTestMapping("task-retry", t4RefA)
	if err := env.client.Create(context.Background(), retryTask); err != nil {
		t.Fatalf("create retry mapping: %v", err)
	}
	if _, err := env.reconcileMapping(retryTask); err != nil {
		t.Fatalf("retry reconcile: %v", err)
	}

	// No-op success.
	m := env.getMapping("task-retry")
	cond := completeCondition(m)
	if cond == nil || cond.Reason != imageflowv1alpha1.ReasonSucceeded {
		t.Fatalf("retry must succeed, got %+v", cond)
	}
	if !strings.Contains(cond.Message, "No-op success") {
		t.Fatalf("retry message must report no-op, got %q", cond.Message)
	}
	if m.Status.Digest != t4DigestA || m.Status.ArtifactRef != "sha256-"+t4HexA {
		t.Fatalf("retry must still report digest/artifactRef: %+v", m.Status)
	}

	// Stream status untouched: same entries, original timestamps.
	afterRetry := env.getStream().Status.Tags
	if len(afterRetry) != len(afterFirst) || len(afterRetry[0].Items) != 1 {
		t.Fatalf("retry must not touch history: %+v", afterRetry)
	}
	if !afterRetry[0].Items[0].Created.Time.Equal(afterFirst[0].Items[0].Created.Time) {
		t.Fatalf("retry must not rewrite the association timestamp")
	}

	// No duplicate Image Change, no duplicate artifact creation.
	events = drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 0 {
		t.Fatalf("retry must not emit ImageChanged, got %d: %v", n, events)
	}
	if n := countEventsWithReason(events, EventReasonImageArtifactCreated); n != 0 {
		t.Fatalf("retry must not recreate the artifact, got %d events", n)
	}
	if n := countEventsWithReason(events, EventReasonMappingRegistered); n != 1 {
		t.Fatalf("retry still audits its own completion, got %d MappingRegistered", n)
	}
	if v := t4CounterValue(t, env.metrics.registrations, regOutcomeNoop); v != 1 {
		t.Fatalf("registrations{noop} = %v, want 1", v)
	}
	if v := t4CounterValue(t, env.metrics.registrations, regOutcomeChanged); v != 1 {
		t.Fatalf("registrations{changed} = %v, want 1 (only the first registration)", v)
	}
}

func TestMappingReconcileTerminalTaskIsEffectFree(t *testing.T) {
	var streamStatusWrites int
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
			obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*imageflowv1alpha1.ImageStream); ok {
				streamStatusWrites++
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	stream := newTestStream(1)
	mapping := newTestMapping("task-1", t4RefA)
	env := newT4TestEnv(t, &funcs, stream, mapping)

	if _, err := env.reconcileMapping(mapping); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	writesAfterFirst := streamStatusWrites
	events := drainFakeEvents(env.recorder)
	_ = events

	// Re-reconciling the terminal task must produce zero effects.
	for i := 0; i < 3; i++ {
		res, err := env.reconcileMapping(mapping)
		if err != nil {
			t.Fatalf("re-reconcile %d: %v", i, err)
		}
		if res != (ctrl.Result{}) {
			t.Fatalf("re-reconcile %d must not requeue: %+v", i, res)
		}
	}
	if streamStatusWrites != writesAfterFirst {
		t.Fatalf("terminal re-reconcile wrote stream status %d extra times",
			streamStatusWrites-writesAfterFirst)
	}
	if extra := drainFakeEvents(env.recorder); len(extra) != 0 {
		t.Fatalf("terminal re-reconcile emitted events: %v", extra)
	}
	if v := t4CounterValue(t, env.metrics.registrations, regOutcomeChanged); v != 1 {
		t.Fatalf("terminal re-reconcile double-counted metrics: %v", v)
	}
}

// --- Image Change detection --------------------------------------------------

func TestMappingReconcileDetectsRealImageChangeOnSameTag(t *testing.T) {
	stream := newTestStream(2)
	first := newTestMapping("task-1", t4RefA)
	env := newT4TestEnv(t, nil, stream, first)

	if _, err := env.reconcileMapping(first); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	_ = drainFakeEvents(env.recorder)

	env.clock.Advance(time.Minute)
	second := newTestMapping("task-2", t4RefB)
	if err := env.client.Create(context.Background(), second); err != nil {
		t.Fatalf("create second mapping: %v", err)
	}
	if _, err := env.reconcileMapping(second); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}

	got := env.getStream()
	if len(got.Status.Tags) != 1 || len(got.Status.Tags[0].Items) != 2 {
		t.Fatalf("expected history depth 2, got %+v", got.Status.Tags)
	}
	items := got.Status.Tags[0].Items
	if items[0].Digest != t4DigestB || items[1].Digest != t4DigestA {
		t.Fatalf("expected most-recent-first [B A], got [%s %s]", items[0].Digest, items[1].Digest)
	}
	if !items[0].Created.Time.Equal(t4TestT0.Add(time.Minute)) {
		t.Fatalf("new association must carry the advanced clock time: %v", items[0].Created.Time)
	}

	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
		t.Fatalf("expected exactly 1 new ImageChanged event, got %d: %v", n, events)
	}
	changed := findEventWithReason(events, EventReasonImageChanged)
	for _, want := range []string{t4DigestB, "previous: " + t4DigestA, "ImageStreamMapping", "task-2"} {
		if !strings.Contains(changed, want) {
			t.Fatalf("ImageChanged event %q must mention %q", changed, want)
		}
	}
	if v := t4CounterValue(t, env.metrics.registrations, regOutcomeChanged); v != 2 {
		t.Fatalf("registrations{changed} = %v, want 2", v)
	}
}

// --- bounded retention -------------------------------------------------------

func TestMappingReconcileEnforcesBoundedHistoryAndOverflowEvents(t *testing.T) {
	registerSequence := func(t *testing.T, limit int, refs ...string) *t4TestEnv {
		t.Helper()
		stream := newTestStream(1)
		env := newT4TestEnv(t, nil, stream)
		env.cfg.StreamTagHistoryLimit = limit
		env.mapping = NewMappingReconciler(env.client, nil, env.recorder, env.clock, env.cfg, env.metrics)
		for i, ref := range refs {
			env.clock.Advance(time.Minute)
			task := newTestMapping("task-"+string(rune('a'+i)), ref)
			if err := env.client.Create(context.Background(), task); err != nil {
				t.Fatalf("create task %d: %v", i, err)
			}
			if _, err := env.reconcileMapping(task); err != nil {
				t.Fatalf("reconcile task %d: %v", i, err)
			}
		}
		return env
	}
	digests := func(env *t4TestEnv) []string {
		items := env.getStream().Status.Tags[0].Items
		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, it.Digest)
		}
		return out
	}

	t.Run("depth 3 keeps most recent and drops overflow to events", func(t *testing.T) {
		env := registerSequence(t, 3, t4RefA, t4RefB, t4RefC, t4RefD)
		got := digests(env)
		want := []string{t4DigestD, t4DigestC, t4DigestB}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("history = %v, want %v", got, want)
		}
		events := drainFakeEvents(env.recorder)
		if n := countEventsWithReason(events, EventReasonTagHistoryOverflow); n != 1 {
			t.Fatalf("expected 1 overflow event, got %d: %v", n, events)
		}
		overflow := findEventWithReason(events, EventReasonTagHistoryOverflow)
		if !strings.Contains(overflow, t4DigestA) {
			t.Fatalf("overflow event must name the dropped digest %q: %q", t4DigestA, overflow)
		}
	})

	t.Run("configured limit below cap is honored", func(t *testing.T) {
		env := registerSequence(t, 2, t4RefA, t4RefB, t4RefC)
		got := digests(env)
		want := []string{t4DigestC, t4DigestB}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("history = %v, want %v", got, want)
		}
	})

	t.Run("configured limit above CRD cap is clamped to 3", func(t *testing.T) {
		env := registerSequence(t, 10, t4RefA, t4RefB, t4RefC, t4RefD)
		if got := digests(env); len(got) != imageflowv1alpha1.StreamTagHistoryMaxItems {
			t.Fatalf("history depth = %d, want clamped to %d", len(got),
				imageflowv1alpha1.StreamTagHistoryMaxItems)
		}
	})
}

// --- created-by annotation + main endpoint discipline ------------------------

func TestMappingReconcilePreservesAnnotationsAndNeverWritesMainEndpoints(t *testing.T) {
	mapping := newTestMapping("task-ann", t4RefB)
	mapping.Annotations = map[string]string{
		t4CreatedByAnnotation: "system:serviceaccount:ci:builder",
	}
	stream := newTestStream(1)
	artifact := &imageflowv1alpha1.ImageArtifact{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "sha256-" + t4HexB,
			Annotations: map[string]string{"notes.example.com/owner": "keep-me"},
		},
		Spec: imageflowv1alpha1.ImageArtifactSpec{Digest: t4DigestB, ImageReference: t4RefB},
	}
	funcs := interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			opts ...client.UpdateOption) error {
			switch obj.(type) {
			case *imageflowv1alpha1.ImageStreamMapping, *imageflowv1alpha1.ImageArtifact:
				return errors.New("controller must never write main endpoints of tasks or artifacts")
			}
			return cl.Update(ctx, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
			obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*imageflowv1alpha1.ImageArtifact); ok {
				return errors.New("controller must never write artifact status in v1")
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	env := newT4TestEnv(t, &funcs, mapping, stream, artifact)

	if _, err := env.reconcileMapping(mapping); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	m := env.getMapping("task-ann")
	if got := m.Annotations[t4CreatedByAnnotation]; got != "system:serviceaccount:ci:builder" {
		t.Fatalf("created-by annotation must be preserved, got %q", got)
	}
	cond := completeCondition(m)
	if cond == nil || cond.Reason != imageflowv1alpha1.ReasonSucceeded {
		t.Fatalf("expected success, got %+v", cond)
	}
	a := env.getArtifact("sha256-" + t4HexB)
	if got := a.Annotations["notes.example.com/owner"]; got != "keep-me" {
		t.Fatalf("user annotations on the artifact must be preserved, got %q", got)
	}
	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageArtifactCreated); n != 0 {
		t.Fatalf("existing artifact must be reused, got %d create events", n)
	}
}

func TestMappingReconcileHandlesArtifactAlreadyExistsRace(t *testing.T) {
	stream := newTestStream(1)
	mapping := newTestMapping("task-race", t4RefA)
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			opts ...client.CreateOption) error {
			if a, ok := obj.(*imageflowv1alpha1.ImageArtifact); ok {
				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: "imageflow.appfun.dev", Resource: "imageartifacts"},
					a.GetName())
			}
			return cl.Create(ctx, obj, opts...)
		},
	}
	env := newT4TestEnv(t, &funcs, stream, mapping)

	if _, err := env.reconcileMapping(mapping); err != nil {
		t.Fatalf("AlreadyExists on artifact create must be handled gracefully: %v", err)
	}
	m := env.getMapping("task-race")
	cond := completeCondition(m)
	if cond == nil || cond.Reason != imageflowv1alpha1.ReasonSucceeded {
		t.Fatalf("expected success despite create race, got %+v", cond)
	}
	if m.Status.ArtifactRef != "sha256-"+t4HexA {
		t.Fatalf("artifactRef must be reported, got %q", m.Status.ArtifactRef)
	}
	events := drainFakeEvents(env.recorder)
	if n := countEventsWithReason(events, EventReasonImageArtifactCreated); n != 0 {
		t.Fatalf("no create event when the create lost the race, got %d", n)
	}
	if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
		t.Fatalf("registration itself must still record the change, got %d", n)
	}
}

// --- failure taxonomy ---------------------------------------------------------

func TestMappingReconcileFailsTerminallyWhenStreamMissing(t *testing.T) {
	var artifactCreates int
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			opts ...client.CreateOption) error {
			if _, ok := obj.(*imageflowv1alpha1.ImageArtifact); ok {
				artifactCreates++
			}
			return cl.Create(ctx, obj, opts...)
		},
	}
	mapping := newTestMapping("task-nostream", t4RefA)
	env := newT4TestEnv(t, &funcs, mapping) // no stream on purpose

	res, err := env.reconcileMapping(mapping)
	if err != nil {
		t.Fatalf("terminal failure must not requeue with error: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Fatalf("terminal failure must not requeue: %+v", res)
	}
	if artifactCreates != 0 {
		t.Fatalf("failed registration must leave zero catalog side effects, artifact creates = %d",
			artifactCreates)
	}

	m := env.getMapping("task-nostream")
	cond := completeCondition(m)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != imageflowv1alpha1.ReasonFailed {
		t.Fatalf("expected Complete=True/Failed, got %+v", cond)
	}
	if !strings.HasPrefix(cond.Message, failCodeStreamNotFound+":") {
		t.Fatalf("failure message must carry the %q taxonomy code: %q", failCodeStreamNotFound, cond.Message)
	}
	if m.Status.Digest != t4DigestA {
		t.Fatalf("parsed digest must still be reported, got %q", m.Status.Digest)
	}
	if m.Status.ArtifactRef != "" {
		t.Fatalf("artifactRef must stay empty on failure, got %q", m.Status.ArtifactRef)
	}

	events := drainFakeEvents(env.recorder)
	failed := findEventWithReason(events, EventReasonMappingFailed)
	if !strings.HasPrefix(failed, corev1.EventTypeWarning) {
		t.Fatalf("MappingFailed must be a Warning event, got %q", failed)
	}
	if v := t4CounterValue(t, env.metrics.registrations, regOutcomeFailed); v != 1 {
		t.Fatalf("registrations{failed} = %v, want 1", v)
	}
	if n := t4HistogramCount(t, env.metrics.taskDuration, taskResultFailure); n != 1 {
		t.Fatalf("task_duration{failure} samples = %d, want 1", n)
	}
}

func TestMappingReconcileTransientErrorsRequeueAndDedupOnRetry(t *testing.T) {
	t.Run("internal error surfaces for requeue, retry converges with one Image Change", func(t *testing.T) {
		var streamWrites int
		funcs := interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if _, ok := obj.(*imageflowv1alpha1.ImageStream); ok {
					streamWrites++
					if streamWrites == 1 {
						return apierrors.NewInternalError(errors.New("etcd unavailable"))
					}
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}
		stream := newTestStream(1)
		mapping := newTestMapping("task-flaky", t4RefA)
		env := newT4TestEnv(t, &funcs, stream, mapping)

		if _, err := env.reconcileMapping(mapping); err == nil {
			t.Fatalf("transient stream write failure must surface as error for requeue")
		}
		if m := env.getMapping("task-flaky"); taskIsTerminal(&m.Status.TaskObjectStatus) {
			t.Fatalf("task must not be terminal after a transient failure")
		}
		if extra := drainFakeEvents(env.recorder); countEventsWithReason(extra, EventReasonImageChanged) != 0 {
			t.Fatalf("failed attempt must not emit ImageChanged: %v", extra)
		}

		// The requeue retry converges; persisted-state dedup keeps exactly one change.
		if _, err := env.reconcileMapping(mapping); err != nil {
			t.Fatalf("retry reconcile: %v", err)
		}
		events := drainFakeEvents(env.recorder)
		if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
			t.Fatalf("expected exactly 1 ImageChanged across attempts, got %d: %v", n, events)
		}
		if v := t4CounterValue(t, env.metrics.registrations, regOutcomeChanged); v != 1 {
			t.Fatalf("registrations{changed} = %v, want 1", v)
		}
	})

	t.Run("conflict on stream status is retried within one reconcile", func(t *testing.T) {
		var streamWrites int
		funcs := interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if _, ok := obj.(*imageflowv1alpha1.ImageStream); ok {
					streamWrites++
					if streamWrites == 1 {
						return apierrors.NewConflict(
							schema.GroupResource{Group: "imageflow.appfun.dev", Resource: "imagestreams"},
							t4TestStream, errors.New("concurrent registration"))
					}
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}
		stream := newTestStream(1)
		mapping := newTestMapping("task-conflict", t4RefA)
		env := newT4TestEnv(t, &funcs, stream, mapping)

		if _, err := env.reconcileMapping(mapping); err != nil {
			t.Fatalf("conflict must be retried internally, got %v", err)
		}
		if streamWrites != 2 {
			t.Fatalf("expected exactly one retry after conflict, stream writes = %d", streamWrites)
		}
		m := env.getMapping("task-conflict")
		if cond := completeCondition(m); cond == nil || cond.Reason != imageflowv1alpha1.ReasonSucceeded {
			t.Fatalf("expected success after conflict retry, got %+v", cond)
		}
		events := drainFakeEvents(env.recorder)
		if n := countEventsWithReason(events, EventReasonImageChanged); n != 1 {
			t.Fatalf("expected exactly 1 ImageChanged, got %d", n)
		}
	})
}

// --- pure logic pieces --------------------------------------------------------

func TestSplitDigestReference(t *testing.T) {
	repo, digest, err := SplitDigestReference(t4RefA)
	if err != nil {
		t.Fatalf("valid reference rejected: %v", err)
	}
	if repo != t4TestRepo || digest != t4DigestA {
		t.Fatalf("split = (%q, %q), want (%q, %q)", repo, digest, t4TestRepo, t4DigestA)
	}

	for _, bad := range []string{
		"quay.io/example/app:stable",                            // tag-only (ADR-004: rejected)
		"quay.io/example/app@sha256:xyz",                        // malformed digest
		"quay.io/example/app@sha256:" + strings.Repeat("A", 64), // uppercase hex
		"@sha256:" + t4HexA,                                     // empty repo
		"quay.io/example/app",                                   // bare repo
		"",                                                      // empty
	} {
		if _, _, err := SplitDigestReference(bad); err == nil {
			t.Fatalf("reference %q must be rejected", bad)
		}
	}
}

func TestArtifactNameForDigest(t *testing.T) {
	name, err := ArtifactNameForDigest(t4DigestA)
	if err != nil {
		t.Fatalf("valid digest rejected: %v", err)
	}
	if name != "sha256-"+t4HexA {
		t.Fatalf("name = %q, want sha256-<hex>", name)
	}
	if _, err := ArtifactNameForDigest("not-a-digest"); err == nil {
		t.Fatalf("invalid digest must be rejected")
	}
}

func TestHistoryLimitClamping(t *testing.T) {
	cases := []struct{ configured, want int }{
		{0, imageflowv1alpha1.StreamTagHistoryMaxItems},  // zero Config: documented default
		{-1, imageflowv1alpha1.StreamTagHistoryMaxItems}, // nonsense: clamp
		{1, 1},
		{2, 2},
		{3, 3},
		{10, imageflowv1alpha1.StreamTagHistoryMaxItems}, // above CRD cap: clamp (write would be rejected)
	}
	for _, c := range cases {
		if got := historyLimit(config.Config{StreamTagHistoryLimit: c.configured}); got != c.want {
			t.Fatalf("historyLimit(%d) = %d, want %d", c.configured, got, c.want)
		}
	}
}
