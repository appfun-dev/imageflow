package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"github.com/appfun-dev/imageflow/internal/registry"
)

// T7 unit tests: fake client + fake clock (seam S3) + a stub of the frozen S2
// registry seam. They cover the acceptance surface that does not need a real
// API server or a real registry: on-demand import registration through the
// shared T4 machinery, drift detection and its attribution, the diagnosable
// failure taxonomy (network / credential / manifest), the credential zero-leak
// boundary and the scheduled cadence decision. The envtest named cases live in
// test/integration/import_controller_test.go, the kind cases in
// test/e2e/import_e2e_test.go.

const (
	t7TestNS       = "catalog-import"
	t7TestStream   = "payments-api"
	t7TestTag      = "stable"
	t7TestRegistry = "quay.io"
	t7TestRepo     = "example/payments-api"
	// t7CanaryPassword must never appear in any user-visible surface.
	t7CanaryPassword = "t7-canary-super-secret-password"
	t7CanaryUser     = "imageflow+t7robot"
	t7PullSecretName = "quay-pull-secret"
)

var (
	t7TestT0 = time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)

	t7HexA     = strings.Repeat("1a", 32)
	t7HexB     = strings.Repeat("2b", 32)
	t7DigestA  = "sha256:" + t7HexA
	t7DigestB  = "sha256:" + t7HexB
	t7RefA     = t7TestRegistry + "/" + t7TestRepo + "@" + t7DigestA
	t7TagRef   = t7TestRegistry + "/" + t7TestRepo + ":" + t7TestTag
	t7HexRefA  = t7RefA
	t7Artifact = "sha256-" + t7HexA
)

// stubRegistry is a programmable stand-in for the frozen S2 seam. It records
// what the controller asked for (reference and credentials) so tests can assert
// the credential boundary without a network.
type stubRegistry struct {
	digest   string
	manifest *registry.Manifest
	err      error

	resolveCalls int
	fetchCalls   int
	listCalls    int
	refs         []registry.Reference
	creds        []registry.Credentials
}

func (s *stubRegistry) ResolveDigest(_ context.Context, ref registry.Reference, creds registry.Credentials) (string, error) {
	s.resolveCalls++
	s.refs = append(s.refs, ref)
	s.creds = append(s.creds, creds)
	if s.err != nil {
		return "", s.err
	}
	return s.digest, nil
}

func (s *stubRegistry) FetchManifest(_ context.Context, ref registry.Reference, creds registry.Credentials) (*registry.Manifest, error) {
	s.fetchCalls++
	s.refs = append(s.refs, ref)
	s.creds = append(s.creds, creds)
	if s.err != nil {
		return nil, s.err
	}
	return s.manifest, nil
}

func (s *stubRegistry) ListTags(_ context.Context, ref registry.Reference, creds registry.Credentials) ([]string, error) {
	s.listCalls++
	s.refs = append(s.refs, ref)
	s.creds = append(s.creds, creds)
	if s.err != nil {
		return nil, s.err
	}
	return []string{t7TestTag}, nil
}

// t7TestEnv bundles the fake collaborators of the T7 reconcilers under test.
type t7TestEnv struct {
	t         *testing.T
	client    client.Client
	recorder  *record.FakeRecorder
	clock     *clock.Fake
	cfg       config.Config
	metrics   *importMetrics
	images    *stubRegistry
	imports   *ImportReconciler
	scheduled *ScheduledImportReconciler
}

func newT7TestEnv(t *testing.T, funcs *interceptor.Funcs, objs ...client.Object) *t7TestEnv {
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
			&imageflowv1alpha1.ImageStreamImport{},
			&imageflowv1alpha1.ImageStream{},
			&imageflowv1alpha1.ImageArtifact{},
		).
		WithObjects(objs...)
	if funcs != nil {
		builder = builder.WithInterceptorFuncs(*funcs)
	}
	env := &t7TestEnv{
		t:        t,
		client:   builder.Build(),
		recorder: record.NewFakeRecorder(256),
		clock:    clock.NewFake(t7TestT0),
		cfg: config.Config{
			MappingTTLSuccess:        time.Hour,
			MappingTTLFailure:        24 * time.Hour,
			StreamTagHistoryLimit:    imageflowv1alpha1.StreamTagHistoryMaxItems,
			ScheduledDefaultInterval: 15 * time.Minute,
			ImportDefaultInterval:    15 * time.Minute,
		},
		metrics: newImportMetrics(),
		images:  &stubRegistry{digest: t7DigestA, manifest: &registry.Manifest{Digest: t7DigestA}},
	}
	env.imports = NewImportReconciler(env.client, env.client, env.recorder, env.clock,
		env.cfg, env.images, env.metrics)
	env.scheduled = NewScheduledImportReconciler(env.client, env.recorder, env.clock,
		env.cfg, env.metrics)
	return env
}

func t7ImportTask(name, from string) *imageflowv1alpha1.ImageStreamImport {
	return &imageflowv1alpha1.ImageStreamImport{
		ObjectMeta: metav1.ObjectMeta{Namespace: t7TestNS, Name: name},
		Spec: imageflowv1alpha1.ImageStreamImportSpec{
			Stream: t7TestStream,
			Tag:    t7TestTag,
			From: imageflowv1alpha1.ImageSourceReference{
				Kind: importSourceKindDockerImage,
				Name: from,
			},
		},
	}
}

func t7Stream(tags ...imageflowv1alpha1.TagReference) *imageflowv1alpha1.ImageStream {
	return &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: t7TestNS, Name: t7TestStream, UID: types.UID("stream-uid-t7"),
		},
		Spec: imageflowv1alpha1.ImageStreamSpec{Tags: tags},
	}
}

func t7ScheduledTag(name, from string, intervalSeconds int32) imageflowv1alpha1.TagReference {
	return imageflowv1alpha1.TagReference{
		Name: name,
		From: &imageflowv1alpha1.ImageSourceReference{Kind: importSourceKindDockerImage, Name: from},
		ImportPolicy: imageflowv1alpha1.TagImportPolicy{
			Scheduled:       ptrTrue(),
			IntervalSeconds: &intervalSeconds,
		},
	}
}

func ptrTrue() *bool { v := true; return &v }

// reconcileImport runs one reconcile of the import task and fails the test on a
// unexpected error.
func (e *t7TestEnv) reconcileImport(name string) ctrl.Result {
	e.t.Helper()
	res, err := e.imports.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: t7TestNS, Name: name},
	})
	if err != nil {
		e.t.Fatalf("reconcile import %s: %v", name, err)
	}
	return res
}

func (e *t7TestEnv) getImport(name string) *imageflowv1alpha1.ImageStreamImport {
	e.t.Helper()
	var task imageflowv1alpha1.ImageStreamImport
	if err := e.client.Get(context.Background(),
		types.NamespacedName{Namespace: t7TestNS, Name: name}, &task); err != nil {
		e.t.Fatalf("get import task %s: %v", name, err)
	}
	return &task
}

func (e *t7TestEnv) completeCondition(task *imageflowv1alpha1.ImageStreamImport) *metav1.Condition {
	e.t.Helper()
	cond := meta.FindStatusCondition(task.Status.Conditions, imageflowv1alpha1.ConditionTypeComplete)
	if cond == nil {
		e.t.Fatalf("task %s has no Complete condition: %+v", task.Name, task.Status)
	}
	return cond
}

func (e *t7TestEnv) events() []string {
	e.t.Helper()
	var out []string
	for {
		select {
		case ev := <-e.recorder.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func countEvents(events []string, reason string) int {
	n := 0
	for _, ev := range events {
		if strings.Contains(ev, " "+reason+" ") {
			n++
		}
	}
	return n
}

func (e *t7TestEnv) streamItems(tag string) []imageflowv1alpha1.TagEvent {
	e.t.Helper()
	var stream imageflowv1alpha1.ImageStream
	if err := e.client.Get(context.Background(),
		types.NamespacedName{Namespace: t7TestNS, Name: t7TestStream}, &stream); err != nil {
		e.t.Fatalf("get stream: %v", err)
	}
	for _, entry := range stream.Status.Tags {
		if entry.Tag == tag {
			return entry.Items
		}
	}
	return nil
}

func TestImportReconcileResolvesTagAndRegistersThroughSharedMachinery(t *testing.T) {
	env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}),
		t7ImportTask("import-1", t7TagRef))

	res := env.reconcileImport("import-1")
	if res != (ctrl.Result{}) {
		t.Fatalf("terminal task must not requeue, got %+v", res)
	}

	task := env.getImport("import-1")
	cond := env.completeCondition(task)
	if cond.Status != metav1.ConditionTrue || cond.Reason != imageflowv1alpha1.ReasonSucceeded {
		t.Fatalf("want Complete=True/Succeeded, got %s/%s", cond.Status, cond.Reason)
	}
	if task.Status.Digest != t7DigestA {
		t.Errorf("status.digest = %q, want %q", task.Status.Digest, t7DigestA)
	}
	if task.Status.ImageReference != t7RefA {
		t.Errorf("status.imageReference = %q, want %q", task.Status.ImageReference, t7RefA)
	}
	if task.Status.ArtifactRef != t7Artifact {
		t.Errorf("status.artifactRef = %q, want %q", task.Status.ArtifactRef, t7Artifact)
	}
	if task.Status.StartedAt == nil || task.Status.CompletedAt == nil {
		t.Error("startedAt and completedAt must both be stamped")
	}
	if !task.Status.CompletedAt.Time.Equal(env.clock.Now()) {
		t.Errorf("completedAt must come from the injected clock, got %v", task.Status.CompletedAt)
	}

	if env.images.resolveCalls != 1 || env.images.fetchCalls != 0 {
		t.Errorf("tag references resolve through ResolveDigest exactly once: resolve=%d fetch=%d",
			env.images.resolveCalls, env.images.fetchCalls)
	}
	if got := env.images.refs[0]; got.Registry != t7TestRegistry || got.Repository != t7TestRepo ||
		got.Tag != t7TestTag || got.Digest != "" {
		t.Errorf("unexpected reference handed to the S2 seam: %+v", got)
	}

	items := env.streamItems(t7TestTag)
	if len(items) != 1 || items[0].Digest != t7DigestA || items[0].ImageReference != t7RefA {
		t.Fatalf("stream association not recorded: %+v", items)
	}
	if !items[0].Created.Time.Equal(env.clock.Now()) {
		t.Errorf("association timestamp must come from the injected clock: %v", items[0].Created)
	}

	var artifact imageflowv1alpha1.ImageArtifact
	if err := env.client.Get(context.Background(),
		types.NamespacedName{Name: t7Artifact}, &artifact); err != nil {
		t.Fatalf("artifact %s not created: %v", t7Artifact, err)
	}
	if artifact.Spec.Digest != t7DigestA || artifact.Spec.ImageReference != t7RefA {
		t.Errorf("artifact spec = %+v", artifact.Spec)
	}

	events := env.events()
	if countEvents(events, EventReasonImportSucceeded) != 1 {
		t.Errorf("want one %s event, got %v", EventReasonImportSucceeded, events)
	}
	if countEvents(events, EventReasonImageChanged) != 1 {
		t.Errorf("want one shared %s event, got %v", EventReasonImageChanged, events)
	}
	changed := findEvent(events, EventReasonImageChanged)
	if !strings.Contains(changed, string(RegistrationSourceImport)) {
		t.Errorf("the Image Change must be attributed to the import source: %q", changed)
	}
	if strings.Contains(changed, string(RegistrationSourceMapping)) {
		t.Errorf("the Image Change must not claim the mapping source: %q", changed)
	}
	if countEvents(events, EventReasonTagDriftDetected) != 0 {
		t.Errorf("a first association is not drift: %v", events)
	}

	if got := testutil.ToFloat64(env.metrics.tasks.WithLabelValues(
		importResultSuccess, imageflowv1alpha1.ReasonSucceeded)); got != 1 {
		t.Errorf("tasks_total{success} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(env.metrics.registrations.WithLabelValues(importOutcomeChanged)); got != 1 {
		t.Errorf("registrations_total{changed} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(env.metrics.duration); got == 0 {
		t.Error("import duration histogram recorded nothing")
	}
}

func TestImportReconcileDigestReferenceVerifiesTheManifest(t *testing.T) {
	env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}),
		t7ImportTask("import-digest", t7HexRefA))

	env.reconcileImport("import-digest")

	if env.images.fetchCalls != 1 || env.images.resolveCalls != 0 {
		t.Fatalf("digest references verify through FetchManifest: fetch=%d resolve=%d",
			env.images.fetchCalls, env.images.resolveCalls)
	}
	if got := env.images.refs[0]; got.Digest != t7DigestA || got.Tag != "" {
		t.Errorf("FetchManifest must address the digest only, got %+v", got)
	}
	task := env.getImport("import-digest")
	if task.Status.Digest != t7DigestA {
		t.Errorf("status.digest = %q, want %q", task.Status.Digest, t7DigestA)
	}
	if cond := env.completeCondition(task); cond.Reason != imageflowv1alpha1.ReasonSucceeded {
		t.Errorf("reason = %s, want Succeeded (%s)", cond.Reason, cond.Message)
	}
}

func TestImportReconcileNoDriftIsNoopWithoutImageChange(t *testing.T) {
	env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}),
		t7ImportTask("import-1", t7TagRef), t7ImportTask("import-2", t7TagRef))

	env.reconcileImport("import-1")
	first := env.events()
	changedBefore := countEvents(first, EventReasonImageChanged)
	itemsBefore := env.streamItems(t7TestTag)

	env.clock.Advance(time.Minute)
	env.reconcileImport("import-2")

	task := env.getImport("import-2")
	cond := env.completeCondition(task)
	if cond.Reason != imageflowv1alpha1.ReasonSucceeded {
		t.Fatalf("re-import must still succeed: %s", cond.Message)
	}
	if !strings.Contains(cond.Message, "No drift") {
		t.Errorf("message must state the absence of drift, got %q", cond.Message)
	}
	items := env.streamItems(t7TestTag)
	if len(items) != len(itemsBefore) {
		t.Errorf("history must not grow without drift: %v -> %v", itemsBefore, items)
	}
	events := env.events()
	if got := countEvents(events, EventReasonImageChanged) + changedBefore; got != changedBefore {
		t.Errorf("no new Image Change expected, events: %v", events)
	}
	if countEvents(events, EventReasonTagDriftDetected) != 0 {
		t.Errorf("no drift event expected: %v", events)
	}
	if got := testutil.ToFloat64(env.metrics.registrations.WithLabelValues(importOutcomeNoop)); got != 1 {
		t.Errorf("registrations_total{noop} = %v, want 1", got)
	}
}

func TestImportReconcileDetectsTagDriftAndAttributesItToTheImportPath(t *testing.T) {
	env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}),
		t7ImportTask("import-a", t7TagRef), t7ImportTask("import-b", t7TagRef))

	env.reconcileImport("import-a")
	_ = env.events()

	env.images.digest = t7DigestB // the external tag moved: same tag, new digest
	env.clock.Advance(10 * time.Minute)
	env.reconcileImport("import-b")

	items := env.streamItems(t7TestTag)
	if len(items) != 2 || items[0].Digest != t7DigestB || items[1].Digest != t7DigestA {
		t.Fatalf("drift must be recorded most-recent-first: %+v", items)
	}
	events := env.events()
	drift := findEvent(events, EventReasonTagDriftDetected)
	if drift == "" {
		t.Fatalf("want a %s event, got %v", EventReasonTagDriftDetected, events)
	}
	for _, want := range []string{t7DigestA, t7DigestB, t7TestTag, t7TagRef} {
		if !strings.Contains(drift, want) {
			t.Errorf("drift event %q must mention %q", drift, want)
		}
	}
	if !strings.Contains(drift, "trigger: on-demand") {
		t.Errorf("drift event must name the trigger: %q", drift)
	}
	changed := findEvent(events, EventReasonImageChanged)
	if !strings.Contains(changed, string(RegistrationSourceImport)) ||
		!strings.Contains(changed, "previous: "+t7DigestA) {
		t.Errorf("shared Image Change event must attribute the import source: %q", changed)
	}
	task := env.getImport("import-b")
	if !strings.Contains(env.completeCondition(task).Message, "tag drift detected") {
		t.Errorf("task message must state the drift: %q", env.completeCondition(task).Message)
	}
}

func TestImportReconcileTerminalTaskIsEffectFree(t *testing.T) {
	env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}),
		t7ImportTask("import-1", t7TagRef))

	env.reconcileImport("import-1")
	before := env.getImport("import-1").ResourceVersion
	resolveBefore := env.images.resolveCalls
	_ = env.events()

	env.reconcileImport("import-1")

	if env.images.resolveCalls != resolveBefore {
		t.Error("a terminal task must not touch the registry again")
	}
	if got := env.getImport("import-1").ResourceVersion; got != before {
		t.Errorf("a terminal task must not be rewritten (%s -> %s)", before, got)
	}
	if events := env.events(); len(events) != 0 {
		t.Errorf("a terminal task must emit nothing, got %v", events)
	}
}

func TestImportReconcileFailureTaxonomy(t *testing.T) {
	connectionRefused := &url.Error{
		Op:  "Head",
		URL: "https://" + t7TestRegistry + "/v2/" + t7TestRepo + "/manifests/" + t7TestTag,
		Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
	}
	cases := []struct {
		name string
		err  error
		code string
	}{
		{"network unreachable", connectionRefused, importFailNetwork},
		{"registry timeout sentinel", fmt.Errorf("wrap: %w", registry.ErrTimeout), importFailTimeout},
		{"context deadline", context.DeadlineExceeded, importFailTimeout},
		{"net timeout", &net.DNSError{IsTimeout: true}, importFailTimeout},
		{"unauthorized", &registry.UnauthorizedError{
			Registry: t7TestRegistry, StatusCode: 401, Anonymous: true,
		}, importFailUnauthorized},
		{"manifest not found", &registry.NotFoundError{
			Registry: t7TestRegistry, Target: t7TagRef, Code: "MANIFEST_UNKNOWN",
		}, importFailManifestNotFound},
		{"digest mismatch", &registry.DigestMismatchError{
			Expected: t7DigestA, Actual: t7DigestB, Registry: t7TestRegistry,
		}, importFailDigestMismatch},
		{"allowlist denied", &registry.DeniedError{Registry: t7TestRegistry}, importFailRegistryDenied},
		{"invalid reference", &registry.InvalidReferenceError{Field: "tag", Reason: "boom"},
			failCodeInvalidImageReference},
		{"server error", &registry.HTTPError{
			Registry: t7TestRegistry, StatusCode: 503, Code: "UNAVAILABLE",
		}, importFailRegistry},
		{"protocol violation", errors.New("registry served a malformed header"), importFailRegistry},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}),
				t7ImportTask("import-fail", t7TagRef))
			env.images.err = tc.err

			res := env.reconcileImport("import-fail")
			if res != (ctrl.Result{}) {
				t.Fatalf("a classified registry failure is terminal, got %+v", res)
			}

			task := env.getImport("import-fail")
			cond := env.completeCondition(task)
			if cond.Status != metav1.ConditionTrue || cond.Reason != imageflowv1alpha1.ReasonFailed {
				t.Fatalf("want Complete=True/Failed, got %s/%s", cond.Status, cond.Reason)
			}
			if !strings.HasPrefix(cond.Message, tc.code+":") {
				t.Errorf("message must be prefixed with the diagnosable code %q, got %q",
					tc.code, cond.Message)
			}
			if task.Status.Digest != "" || task.Status.ArtifactRef != "" {
				t.Errorf("a failed import must not report results: %+v", task.Status)
			}
			events := env.events()
			failed := findEvent(events, EventReasonImportFailed)
			if failed == "" || !strings.Contains(failed, "Warning") {
				t.Errorf("want a Warning %s event, got %v", EventReasonImportFailed, events)
			}
			if got := testutil.ToFloat64(env.metrics.tasks.WithLabelValues(
				importResultFailure, tc.code)); got != 1 {
				t.Errorf("tasks_total{failure,%s} = %v, want 1", tc.code, got)
			}
		})
	}
}

func TestImportReconcileCancelledContextIsTransientNotTerminal(t *testing.T) {
	env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}),
		t7ImportTask("import-cancel", t7TagRef))
	env.images.err = context.Canceled

	res, err := env.imports.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: t7TestNS, Name: "import-cancel"},
	})
	if err == nil {
		t.Fatal("a cancelled import must surface as a reconcile error (requeue)")
	}
	if res != (ctrl.Result{}) {
		t.Errorf("requeue must come from the error, got %+v", res)
	}
	task := env.getImport("import-cancel")
	if taskIsTerminal(&task.Status.TaskObjectStatus) {
		t.Errorf("a cancelled import must stay open for retry: %+v", task.Status)
	}
	if task.Status.StartedAt == nil {
		t.Error("startedAt must survive the cancelled attempt")
	}
}

func TestImportReconcileStreamNotFoundIsTerminalAndDiagnosable(t *testing.T) {
	env := newT7TestEnv(t, nil, t7ImportTask("import-nostream", t7TagRef))

	env.reconcileImport("import-nostream")

	task := env.getImport("import-nostream")
	cond := env.completeCondition(task)
	if cond.Reason != imageflowv1alpha1.ReasonFailed {
		t.Fatalf("want Failed, got %s", cond.Reason)
	}
	if !strings.HasPrefix(cond.Message, failCodeStreamNotFound+":") {
		t.Errorf("message = %q, want the %s prefix", cond.Message, failCodeStreamNotFound)
	}
	if got := testutil.ToFloat64(env.metrics.tasks.WithLabelValues(
		importResultFailure, failCodeStreamNotFound)); got != 1 {
		t.Errorf("tasks_total{failure,%s} = %v, want 1", failCodeStreamNotFound, got)
	}
}

func TestImportReconcileRejectsMalformedSourceWithoutRegistryCall(t *testing.T) {
	cases := []struct {
		name string
		from string
		kind string
		code string
	}{
		{"missing registry host", "payments-api:stable", importSourceKindDockerImage,
			failCodeInvalidImageReference},
		{"garbage", "not an image reference", importSourceKindDockerImage,
			failCodeInvalidImageReference},
		{"unsupported source kind", t7TagRef, "ImageStreamImage", importFailUnsupportedSource},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := t7ImportTask("import-bad", tc.from)
			task.Spec.From.Kind = tc.kind
			env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}), task)

			env.reconcileImport("import-bad")

			cond := env.completeCondition(env.getImport("import-bad"))
			if !strings.HasPrefix(cond.Message, tc.code+":") {
				t.Errorf("message = %q, want the %s prefix", cond.Message, tc.code)
			}
			if env.images.resolveCalls+env.images.fetchCalls != 0 {
				t.Error("a rejected reference must not reach the registry seam")
			}
		})
	}
}

func TestImportPullSecretIsUsedAndNeverSurfaces(t *testing.T) {
	secret := t7DockerConfigSecret(t7CanaryUser, t7CanaryPassword, t7TestRegistry)
	env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}), secret)

	task := t7ImportTask("import-creds", t7TagRef)
	task.Spec.PullSecretRef = &imageflowv1alpha1.LocalSecretReference{Name: t7PullSecretName}
	if err := env.client.Create(context.Background(), task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	env.reconcileImport("import-creds")

	if len(env.images.creds) != 1 {
		t.Fatalf("want exactly one registry call, got %d", len(env.images.creds))
	}
	got := env.images.creds[0]
	if got.Username != t7CanaryUser || got.Password != t7CanaryPassword {
		t.Errorf("credentials were not handed to the S2 seam: %#v", got)
	}
	if !strings.Contains(fmt.Sprintf("%v", got), "[redacted]") {
		t.Errorf("credentials must self-redact in fmt output: %v", got)
	}

	completed := env.getImport("import-creds")
	if cond := env.completeCondition(completed); cond.Reason != imageflowv1alpha1.ReasonSucceeded {
		t.Fatalf("authenticated import failed: %s", cond.Message)
	}
	assertNoCredentialLeak(t, env.events(), completed, secret)
}

func TestImportPullSecretFailuresAreDiagnosableAndLeakFree(t *testing.T) {
	t.Run("secret missing", func(t *testing.T) {
		env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}))
		task := t7ImportTask("import-nosecret", t7TagRef)
		task.Spec.PullSecretRef = &imageflowv1alpha1.LocalSecretReference{Name: "does-not-exist"}
		if err := env.client.Create(context.Background(), task); err != nil {
			t.Fatalf("create task: %v", err)
		}

		env.reconcileImport("import-nosecret")

		cond := env.completeCondition(env.getImport("import-nosecret"))
		if !strings.HasPrefix(cond.Message, importFailPullSecretNotFound+":") {
			t.Errorf("message = %q, want the %s prefix", cond.Message, importFailPullSecretNotFound)
		}
		if env.images.resolveCalls != 0 {
			t.Error("a missing pull secret must stop before the registry call")
		}
	})

	t.Run("no entry for the registry", func(t *testing.T) {
		secret := t7DockerConfigSecret(t7CanaryUser, t7CanaryPassword, "other.example.com")
		env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}), secret)
		task := t7ImportTask("import-wronghost", t7TagRef)
		task.Spec.PullSecretRef = &imageflowv1alpha1.LocalSecretReference{Name: t7PullSecretName}
		if err := env.client.Create(context.Background(), task); err != nil {
			t.Fatalf("create task: %v", err)
		}

		env.reconcileImport("import-wronghost")

		completed := env.getImport("import-wronghost")
		cond := env.completeCondition(completed)
		if !strings.HasPrefix(cond.Message, importFailPullSecretInvalid+":") {
			t.Errorf("message = %q, want the %s prefix", cond.Message, importFailPullSecretInvalid)
		}
		if !strings.Contains(cond.Message, t7TestRegistry) {
			t.Errorf("message must name the registry it could not find credentials for: %q",
				cond.Message)
		}
		assertNoCredentialLeak(t, env.events(), completed, secret)
	})

	t.Run("registry rejects the credentials", func(t *testing.T) {
		secret := t7DockerConfigSecret(t7CanaryUser, t7CanaryPassword, t7TestRegistry)
		env := newT7TestEnv(t, nil, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}), secret)
		env.images.err = &registry.UnauthorizedError{
			Registry: t7TestRegistry, StatusCode: 401,
		}
		task := t7ImportTask("import-denied", t7TagRef)
		task.Spec.PullSecretRef = &imageflowv1alpha1.LocalSecretReference{Name: t7PullSecretName}
		if err := env.client.Create(context.Background(), task); err != nil {
			t.Fatalf("create task: %v", err)
		}

		env.reconcileImport("import-denied")

		completed := env.getImport("import-denied")
		cond := env.completeCondition(completed)
		if !strings.HasPrefix(cond.Message, importFailUnauthorized+":") {
			t.Errorf("message = %q, want the %s prefix", cond.Message, importFailUnauthorized)
		}
		assertNoCredentialLeak(t, env.events(), completed, secret)
	})
}

func TestImportTransientAPIErrorsRequeueWithoutTerminalStatus(t *testing.T) {
	boom := apierrors.NewInternalError(errors.New("simulated API outage"))
	env := newT7TestEnv(t, &interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
			obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if task, ok := obj.(*imageflowv1alpha1.ImageStreamImport); ok && sub == "status" &&
				meta.FindStatusCondition(task.Status.Conditions,
					imageflowv1alpha1.ConditionTypeComplete) != nil {
				return boom
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}, t7Stream(imageflowv1alpha1.TagReference{Name: t7TestTag}), t7ImportTask("import-flaky", t7TagRef))

	_, err := env.imports.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: t7TestNS, Name: "import-flaky"},
	})
	if err == nil {
		t.Fatal("a failing terminal status write must requeue")
	}
	task := env.getImport("import-flaky")
	if taskIsTerminal(&task.Status.TaskObjectStatus) {
		t.Errorf("the task must stay open after a transient write failure: %+v", task.Status)
	}
	// The catalog side effects landed; a retry must not duplicate them.
	if items := env.streamItems(t7TestTag); len(items) != 1 {
		t.Errorf("registration should have landed once, got %+v", items)
	}
}

// --- scheduled cadence -----------------------------------------------------

func TestEffectiveImportIntervalChain(t *testing.T) {
	tagWith := func(seconds *int32) imageflowv1alpha1.TagReference {
		return imageflowv1alpha1.TagReference{
			Name:         t7TestTag,
			ImportPolicy: imageflowv1alpha1.TagImportPolicy{IntervalSeconds: seconds},
		}
	}
	seconds := func(v int32) *int32 { return &v }

	if got := EffectiveImportInterval(tagWith(seconds(45)), config.Config{
		ScheduledDefaultInterval: time.Hour, ImportDefaultInterval: time.Minute,
	}); got != 45*time.Second {
		t.Errorf("explicit intervalSeconds must win, got %v", got)
	}
	if got := EffectiveImportInterval(tagWith(nil), config.Config{
		ScheduledDefaultInterval: 7 * time.Minute, ImportDefaultInterval: time.Minute,
	}); got != 7*time.Minute {
		t.Errorf("--scheduled-default-interval must apply, got %v", got)
	}
	if got := EffectiveImportInterval(tagWith(seconds(0)), config.Config{
		ImportDefaultInterval: 3 * time.Minute,
	}); got != 3*time.Minute {
		t.Errorf("--import-default-interval must be the second fallback, got %v", got)
	}
	if got := EffectiveImportInterval(tagWith(nil), config.Config{}); got != defaultScheduledImportInterval {
		t.Errorf("built-in default must apply for a zero config, got %v", got)
	}
}

func TestImportScheduleDecision(t *testing.T) {
	interval := 15 * time.Minute
	now := t7TestT0

	completed := func(ago time.Duration) imageflowv1alpha1.ImageStreamImport {
		at := metav1.NewTime(now.Add(-ago))
		created := metav1.NewTime(now.Add(-ago - time.Minute))
		task := imageflowv1alpha1.ImageStreamImport{
			ObjectMeta: metav1.ObjectMeta{Namespace: t7TestNS, Name: "t", CreationTimestamp: created},
		}
		task.Status.CompletedAt = &at
		task.Status.Conditions = []metav1.Condition{{
			Type: imageflowv1alpha1.ConditionTypeComplete, Status: metav1.ConditionTrue,
			Reason: imageflowv1alpha1.ReasonSucceeded, LastTransitionTime: at,
		}}
		return task
	}
	inFlight := func(ago time.Duration) imageflowv1alpha1.ImageStreamImport {
		return imageflowv1alpha1.ImageStreamImport{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: t7TestNS, Name: "running",
				CreationTimestamp: metav1.NewTime(now.Add(-ago)),
			},
		}
	}

	t.Run("no tasks yet: due immediately", func(t *testing.T) {
		due, wait, running := importScheduleDecision(nil, now, interval)
		if !due || wait != 0 || running {
			t.Fatalf("due=%v wait=%v inFlight=%v", due, wait, running)
		}
	})
	t.Run("inside the interval: not due", func(t *testing.T) {
		due, wait, running := importScheduleDecision(
			[]imageflowv1alpha1.ImageStreamImport{completed(5 * time.Minute)}, now, interval)
		if due || running {
			t.Fatalf("due=%v inFlight=%v", due, running)
		}
		if wait != 10*time.Minute {
			t.Errorf("wait = %v, want 10m", wait)
		}
	})
	t.Run("past the interval: due", func(t *testing.T) {
		due, wait, running := importScheduleDecision(
			[]imageflowv1alpha1.ImageStreamImport{completed(16 * time.Minute)}, now, interval)
		if !due || wait != 0 || running {
			t.Fatalf("due=%v wait=%v inFlight=%v", due, wait, running)
		}
	})
	t.Run("failed tasks anchor the retry cadence", func(t *testing.T) {
		failed := completed(3 * time.Minute)
		failed.Status.Conditions[0].Reason = imageflowv1alpha1.ReasonFailed
		due, wait, _ := importScheduleDecision(
			[]imageflowv1alpha1.ImageStreamImport{failed}, now, interval)
		if due || wait != 12*time.Minute {
			t.Fatalf("due=%v wait=%v, want a retry only after the full interval", due, wait)
		}
	})
	t.Run("in-flight tasks are never stacked", func(t *testing.T) {
		due, _, running := importScheduleDecision(
			[]imageflowv1alpha1.ImageStreamImport{inFlight(2 * time.Hour)}, now, interval)
		if due || !running {
			t.Fatalf("due=%v inFlight=%v", due, running)
		}
	})
	t.Run("the newest task anchors", func(t *testing.T) {
		due, wait, _ := importScheduleDecision([]imageflowv1alpha1.ImageStreamImport{
			completed(40 * time.Minute), completed(2 * time.Minute),
		}, now, interval)
		if due || wait != 13*time.Minute {
			t.Fatalf("due=%v wait=%v", due, wait)
		}
	})
}

func TestScheduledImportCreatesOwnedLabelledTaskAndRequeues(t *testing.T) {
	tag := t7ScheduledTag(t7TestTag, t7TagRef, 600)
	env := newT7TestEnv(t, nil, t7Stream(tag))

	res, err := env.scheduled.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: t7TestNS, Name: t7TestStream},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 10*time.Minute {
		t.Errorf("RequeueAfter = %v, want the tag interval 10m", res.RequeueAfter)
	}

	var list imageflowv1alpha1.ImageStreamImportList
	if err := env.client.List(context.Background(), &list, client.InNamespace(t7TestNS)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("want exactly one scheduled task, got %d", len(list.Items))
	}
	created := list.Items[0]
	if created.Labels[LabelImportSource] != LabelValueScheduledImport {
		t.Errorf("task label = %v, want %s=%s", created.Labels,
			LabelImportSource, LabelValueScheduledImport)
	}
	if created.Spec.Stream != t7TestStream || created.Spec.Tag != t7TestTag ||
		created.Spec.From.Name != t7TagRef {
		t.Errorf("task spec = %+v", created.Spec)
	}
	if created.Spec.PullSecretRef != nil {
		t.Errorf("scheduled imports are anonymous in v1, got %+v", created.Spec.PullSecretRef)
	}
	if created.Annotations[AnnotationScheduledInterval] != "10m0s" {
		t.Errorf("interval annotation = %q", created.Annotations[AnnotationScheduledInterval])
	}
	if len(created.OwnerReferences) != 1 {
		t.Fatalf("want a controlling ownerRef, got %+v", created.OwnerReferences)
	}
	owner := created.OwnerReferences[0]
	if owner.Kind != scheduledTaskOwnerKind || owner.Name != t7TestStream ||
		owner.UID != types.UID("stream-uid-t7") {
		t.Errorf("ownerRef = %+v", owner)
	}
	if owner.Controller == nil || !*owner.Controller {
		t.Error("ownerRef must be controlling (Owns() and cascade GC depend on it)")
	}
	if !strings.HasPrefix(created.Name, EncodeImageStreamTagName(t7TestStream, t7TestTag)) {
		t.Errorf("task name %q must derive from the encoded (stream, tag) index", created.Name)
	}

	events := env.events()
	if findEvent(events, EventReasonScheduledImportCreated) == "" {
		t.Errorf("want a %s event, got %v", EventReasonScheduledImportCreated, events)
	}
	if got := testutil.ToFloat64(env.metrics.scheduledCreations.WithLabelValues(
		scheduledCreateCreated)); got != 1 {
		t.Errorf("scheduled_creations_total{created} = %v, want 1", got)
	}
}

func TestScheduledImportIgnoresTagsWithoutPolicyOrSource(t *testing.T) {
	plain := imageflowv1alpha1.TagReference{Name: "plain"}
	notScheduled := imageflowv1alpha1.TagReference{
		Name:         "pushed",
		From:         &imageflowv1alpha1.ImageSourceReference{Name: t7TagRef},
		ImportPolicy: imageflowv1alpha1.TagImportPolicy{Scheduled: ptrFalse()},
	}
	noSource := imageflowv1alpha1.TagReference{
		Name:         "no-source",
		ImportPolicy: imageflowv1alpha1.TagImportPolicy{Scheduled: ptrTrue()},
	}
	env := newT7TestEnv(t, nil, t7Stream(plain, notScheduled, noSource))

	res, err := env.scheduled.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: t7TestNS, Name: t7TestStream},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("nothing to poll, got %+v", res)
	}
	var list imageflowv1alpha1.ImageStreamImportList
	if err := env.client.List(context.Background(), &list, client.InNamespace(t7TestNS)); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 0 {
		t.Errorf("no task may be created for unpollable tags, got %d", len(list.Items))
	}
}

func ptrFalse() *bool { v := false; return &v }

func TestScheduledImportDoesNotStackWhileATaskIsInFlight(t *testing.T) {
	tag := t7ScheduledTag(t7TestTag, t7TagRef, 600)
	env := newT7TestEnv(t, nil, t7Stream(tag))

	for i := range 2 {
		if _, err := env.scheduled.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: t7TestNS, Name: t7TestStream},
		}); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}

	var list imageflowv1alpha1.ImageStreamImportList
	if err := env.client.List(context.Background(), &list, client.InNamespace(t7TestNS)); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("an in-flight scheduled task must not be stacked, got %d tasks", len(list.Items))
	}
}

func TestScheduledImportRunsAgainAfterTheIntervalUnderTheInjectedClock(t *testing.T) {
	tag := t7ScheduledTag(t7TestTag, t7TagRef, 600)
	env := newT7TestEnv(t, nil, t7Stream(tag))
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: t7TestNS, Name: t7TestStream}}

	if _, err := env.scheduled.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	completeScheduledTasks(t, env)

	env.clock.Advance(9 * time.Minute)
	if _, err := env.scheduled.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile inside the interval: %v", err)
	}
	if got := countTasks(t, env); got != 1 {
		t.Fatalf("inside the interval no second task may appear, got %d", got)
	}

	env.clock.Advance(2 * time.Minute) // 11m since completion
	if _, err := env.scheduled.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile past the interval: %v", err)
	}
	if got := countTasks(t, env); got != 2 {
		t.Fatalf("past the interval a second task must appear, got %d", got)
	}
}

func TestScheduledImportReportsCreationFailures(t *testing.T) {
	tag := t7ScheduledTag(t7TestTag, t7TagRef, 600)
	boom := apierrors.NewInternalError(errors.New("simulated create outage"))
	env := newT7TestEnv(t, &interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			opts ...client.CreateOption) error {
			if _, ok := obj.(*imageflowv1alpha1.ImageStreamImport); ok {
				return boom
			}
			return cl.Create(ctx, obj, opts...)
		},
	}, t7Stream(tag))

	_, err := env.scheduled.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: t7TestNS, Name: t7TestStream},
	})
	if err == nil {
		t.Fatal("a creation failure must surface so the reconcile requeues")
	}
	events := env.events()
	if findEvent(events, EventReasonScheduledImportError) == "" {
		t.Errorf("want a %s event, got %v", EventReasonScheduledImportError, events)
	}
	if got := testutil.ToFloat64(env.metrics.scheduledCreations.WithLabelValues(
		scheduledCreateError)); got != 1 {
		t.Errorf("scheduled_creations_total{error} = %v, want 1", got)
	}
}

func TestScheduledTaskPrefixIsDNS1123Safe(t *testing.T) {
	cases := []struct{ stream, tag string }{
		{"payments-api", "stable"},
		{"payments-api", "UPPER_case.tag"},
		{"a", "_"},
		{strings.Repeat("s", 250), strings.Repeat("t", 128)},
	}
	valid := func(s string) bool {
		if s == "" || len(s) > mirrorNameMaxLen {
			return false
		}
		for _, r := range s {
			ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '.'
			if !ok {
				return false
			}
		}
		return true
	}
	for _, tc := range cases {
		prefix := scheduledTaskPrefix(tc.stream, tc.tag)
		if !valid(prefix) {
			t.Errorf("prefix %q is not DNS-1123 safe (stream %q tag %q)",
				prefix, tc.stream, tc.tag)
		}
	}
	// Two distinct tags of one stream must not collide on the same prefix.
	if scheduledTaskPrefix("payments-api", "stable") == scheduledTaskPrefix("payments-api", "canary") {
		t.Error("distinct tags must produce distinct task name prefixes")
	}
}

func TestSetupImportControllersSkipsWithoutImageFlowKinds(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add clientgo scheme: %v", err)
	}
	mgr := importGuardManager{scheme: scheme}

	// An allowlist entry that would fail NewClient proves the guard runs before
	// any client construction: the shared integration manager boots with a
	// clientgoscheme-only scheme and must not be aborted by this unit.
	err := SetupImportControllers(mgr, Options{Config: config.Config{
		RegistryAllowlist: []string{"not a host!"},
	}}, DefaultImportControllerNames(), ImportDeps{})
	if err != nil {
		t.Fatalf("registration must skip (not fail) without imageflow kinds: %v", err)
	}
}

// importGuardManager only needs to answer GetScheme for the guard test.
type importGuardManager struct {
	ctrl.Manager
	scheme *runtime.Scheme
}

func (m importGuardManager) GetScheme() *runtime.Scheme { return m.scheme }

// --- helpers ---------------------------------------------------------------

func t7DockerConfigSecret(user, password, host string) *corev1.Secret {
	auth := map[string]any{"auths": map[string]any{
		host: map[string]any{"username": user, "password": password, "auth": ""},
	}}
	payload, err := json.Marshal(auth)
	if err != nil {
		panic(err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: t7TestNS, Name: t7PullSecretName},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: payload},
	}
}

// assertNoCredentialLeak fails when credential material shows up in Events or
// in any of the given objects (serialized the way the API server would).
func assertNoCredentialLeak(t *testing.T, events []string, objs ...client.Object) {
	t.Helper()
	for _, ev := range events {
		if strings.Contains(ev, t7CanaryPassword) || strings.Contains(ev, t7CanaryUser+"\":") {
			t.Errorf("credential material leaked into an Event: %q", ev)
		}
	}
	for _, obj := range objs {
		if obj == nil {
			continue
		}
		if _, isSecret := obj.(*corev1.Secret); isSecret {
			continue // the secret itself obviously holds the material
		}
		raw, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("marshal %T: %v", obj, err)
		}
		if strings.Contains(string(raw), t7CanaryPassword) {
			t.Errorf("credential material leaked into %T: %s", obj, raw)
		}
	}
}

// completeScheduledTasks writes a terminal success status (with the injected
// clock) onto every scheduled task, standing in for the import controller.
func completeScheduledTasks(t *testing.T, env *t7TestEnv) {
	t.Helper()
	var list imageflowv1alpha1.ImageStreamImportList
	if err := env.client.List(context.Background(), &list, client.InNamespace(t7TestNS)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	for i := range list.Items {
		task := &list.Items[i]
		if taskIsTerminal(&task.Status.TaskObjectStatus) {
			continue
		}
		now := metav1.NewTime(env.clock.Now())
		task.Status.StartedAt = &now
		task.Status.CompletedAt = &now
		task.Status.Conditions = []metav1.Condition{{
			Type: imageflowv1alpha1.ConditionTypeComplete, Status: metav1.ConditionTrue,
			Reason: imageflowv1alpha1.ReasonSucceeded, Message: "completed by test",
			LastTransitionTime: now,
		}}
		if err := env.client.Status().Update(context.Background(), task); err != nil {
			t.Fatalf("complete task %s: %v", task.Name, err)
		}
	}
}

func countTasks(t *testing.T, env *t7TestEnv) int {
	t.Helper()
	var list imageflowv1alpha1.ImageStreamImportList
	if err := env.client.List(context.Background(), &list, client.InNamespace(t7TestNS)); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	return len(list.Items)
}

func findEvent(events []string, reason string) string {
	for _, ev := range events {
		if strings.Contains(ev, " "+reason+" ") {
			return ev
		}
	}
	return ""
}
