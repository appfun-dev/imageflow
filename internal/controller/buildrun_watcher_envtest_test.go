package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"github.com/prometheus/client_golang/prometheus"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
	"github.com/appfun-dev/imageflow/internal/delivery"
)

// T5 (ticket #18) envtest integration tests for the BuildRun Watcher (ADR-004
// Amended §4 CRD gate, ADR-011 §3-4 reconcile contract). Unlike the T3 unit
// tests (fake client), this suite runs the reconcile and the manager wiring
// against a real kube-apiserver (envtest):
//
//   - BuildRuns are consumed as unstructured objects through a minimal
//     Shipwright CRD fixture (test/testdata/thirdparty/shipwright.io_*.yaml);
//     no Shipwright Go types are imported anywhere (BC-003).
//   - Lane 1 drives NewBuildRunReconciler directly with the real client and a
//     fake recorder/clock: deterministic full-matrix coverage of the ticket
//     #18 acceptance criteria, including condition content, ImageStream
//     status, ImageArtifact creation and event content on every test.
//   - Lane 2 boots the production manager wiring (controller.Setup) so
//     informers, workqueue, real event recorder and the RESTMapper CRD gate
//     are exercised end to end.
//   - AGG-004 is verified by isolating the watcher (lane 1) and proving that
//     subscription/delivery state is bit-for-bit untouched by a registration.
//
// Envtest binaries are located via KUBEBUILDER_ASSETS, falling back to the
// repo-local bin/k8s/<version>-<os>-<arch>/ directory populated by
// `make envtest`; when neither exists every test here skips, mirroring the
// repo-wide envtest convention.
//
// Sandboxed-agent note (DSH/claude sandboxes on macOS): envtest reserves
// ports via flock files under os.UserCacheDir()/kubebuilder-envtest. When the
// sandbox denies file creation under ~/Library/Caches (open(2) → EPERM,
// surfaced as "failed to start the controlplane. retried 5 times: operation
// not permitted"), run the tests with HOME pointed at a writable directory,
// e.g. `HOME=/tmp/t5home`. The addr cacheDir is computed at package init, so
// this cannot be fixed from inside the test process.

// --- shared environment -----------------------------------------------------

var (
	brwSharedOnce sync.Once
	brwShared     *brwSharedEnv
	brwSharedErr  error
)

// brwSharedEnv is the lazily started environment that carries the ImageFlow
// CRDs plus the Shipwright Build/BuildRun fixtures.
type brwSharedEnv struct {
	env    *envtest.Environment
	cfg    *rest.Config
	scheme *runtime.Scheme
	client client.Client
}

// TestMain stops the shared envtest control plane after the run, but only if
// one of the tests in this file actually started it — unrelated package test
// runs neither boot nor tear down envtest.
func TestMain(m *testing.M) {
	code := m.Run()
	if brwShared != nil {
		if err := brwShared.env.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "buildrun watcher envtest: stop shared environment: %v\n", err)
		}
	}
	os.Exit(code)
}

// sharedBuildRunEnvtestEnv returns the shared environment (with Shipwright
// CRDs installed), starting it on first use. It skips the calling test when
// envtest binaries are unavailable.
func sharedBuildRunEnvtestEnv(t *testing.T) *brwSharedEnv {
	t.Helper()
	brwSharedOnce.Do(func() {
		assets := buildRunWatcherEnvtestAssets(t)
		if assets == "" {
			return
		}
		env, err := startBuildRunEnvtest(assets, true)
		if err != nil {
			brwSharedErr = err
			return
		}
		brwShared = env
	})
	if brwSharedErr != nil {
		t.Fatalf("start envtest environment: %v", brwSharedErr)
	}
	if brwShared == nil {
		t.Skip("envtest assets not found: set KUBEBUILDER_ASSETS or run `make envtest`")
	}
	return brwShared
}

// buildRunWatcherEnvtestAssets resolves the envtest binaries directory.
func buildRunWatcherEnvtestAssets(t *testing.T) string {
	t.Helper()
	if assets := os.Getenv("KUBEBUILDER_ASSETS"); assets != "" {
		return assets
	}
	// Repo-local fallback: bin/k8s/<version>-<os>-<arch>/ (Makefile envtest target).
	matches, err := filepath.Glob(filepath.Join("..", "..", "bin", "k8s", "*"))
	if err != nil || len(matches) == 0 {
		return ""
	}
	sort.Strings(matches)
	for i := len(matches) - 1; i >= 0; i-- {
		if st, statErr := os.Stat(filepath.Join(matches[i], "kube-apiserver")); statErr == nil && !st.IsDir() {
			abs, absErr := filepath.Abs(matches[i])
			if absErr != nil {
				t.Fatalf("resolve envtest assets path: %v", absErr)
			}
			return abs
		}
	}
	return ""
}

// startBuildRunEnvtest boots one control plane with the ImageFlow CRDs and,
// when withShipwright is true, the Shipwright Build/BuildRun fixtures.
func startBuildRunEnvtest(assets string, withShipwright bool) (*brwSharedEnv, error) {
	crdPaths := []string{filepath.Join("..", "..", "config", "crd", "bases")}
	if withShipwright {
		crdPaths = append(crdPaths, filepath.Join("..", "..", "test", "testdata", "thirdparty"))
	}
	env := &envtest.Environment{
		BinaryAssetsDirectory: assets,
		CRDDirectoryPaths:     crdPaths,
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		return nil, fmt.Errorf("start envtest: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		_ = env.Stop()
		return nil, fmt.Errorf("register client-go scheme: %w", err)
	}
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		_ = env.Stop()
		return nil, fmt.Errorf("register imageflow scheme: %w", err)
	}
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		_ = env.Stop()
		return nil, fmt.Errorf("create client: %w", err)
	}
	return &brwSharedEnv{env: env, cfg: cfg, scheme: scheme, client: cl}, nil
}

// --- object builders --------------------------------------------------------

// brwT0 is the deterministic lane-1 clock instant (whole seconds so it
// round-trips losslessly through the API server's RFC3339 timestamps).
var brwT0 = time.Date(2026, 2, 5, 12, 0, 0, 0, time.UTC)

// brwDigest builds a well-formed sha256:<64hex> digest from one hex character;
// each test picks a distinct character so the cluster-scoped ImageArtifact it
// creates never collides with another test's artifact in the shared env.
func brwDigest(hexChar string) string {
	return "sha256:" + strings.Repeat(hexChar, 64)
}

// brwCreateNamespace provisions an isolated namespace per test.
func brwCreateNamespace(t *testing.T, cl client.Client) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "brw-"}}
	if err := cl.Create(context.Background(), ns); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	t.Cleanup(func() {
		_ = cl.Delete(context.Background(), ns)
	})
	return ns.Name
}

// brwNewBuildRun builds an annotated-or-not Shipwright BuildRun skeleton
// (unstructured; BC-003). Status is applied separately: the API server strips
// status on create for CRDs with a status subresource.
func brwNewBuildRun(ns, name string) *unstructured.Unstructured {
	br := &unstructured.Unstructured{}
	br.SetGroupVersionKind(buildRunGVK)
	br.SetNamespace(ns)
	br.SetName(name)
	return br
}

func brwWithTargetAnnotation(br *unstructured.Unstructured, stream, tag string) *unstructured.Unstructured {
	br.SetAnnotations(map[string]string{
		buildRunTargetAnnotation: fmt.Sprintf(`{"stream":%q,"tag":%q}`, stream, tag),
	})
	return br
}

func brwWithRawAnnotation(br *unstructured.Unstructured, value string) *unstructured.Unstructured {
	br.SetAnnotations(map[string]string{buildRunTargetAnnotation: value})
	return br
}

// brwStatus builds the mutate function for brwCreateBuildRun carrying the
// Shipwright status fields the watcher contract reads: the Succeeded
// condition, .status.output.digest and .status.buildSpec.output.image.
func brwStatus(succeededConditionStatus, digest, statusBuildSpecImage string) func(map[string]interface{}) {
	return func(status map[string]interface{}) {
		_ = unstructured.SetNestedSlice(status, []interface{}{
			map[string]interface{}{
				"type":               "Succeeded",
				"status":             succeededConditionStatus,
				"reason":             "Completed",
				"lastTransitionTime": "2026-02-05T11:59:00Z",
			},
		}, "conditions")
		if digest != "" {
			_ = unstructured.SetNestedField(status, digest, "output", "digest")
		}
		if statusBuildSpecImage != "" {
			_ = unstructured.SetNestedField(status, statusBuildSpecImage, "buildSpec", "output", "image")
		}
	}
}

// brwCreateBuildRun creates the BuildRun (status stripped by the API server)
// and then applies mutateStatus through the status subresource, mirroring how
// Shipwright reports build outcomes.
func brwCreateBuildRun(t *testing.T, cl client.Client, br *unstructured.Unstructured, mutateStatus func(map[string]interface{})) {
	t.Helper()
	ctx := context.Background()
	var status map[string]interface{}
	if mutateStatus != nil {
		status = map[string]interface{}{}
		mutateStatus(status)
	}
	delete(br.Object, "status")
	if err := cl.Create(ctx, br); err != nil {
		t.Fatalf("create BuildRun %s/%s: %v", br.GetNamespace(), br.GetName(), err)
	}
	if status != nil {
		br.Object["status"] = status
		if err := cl.Status().Update(ctx, br); err != nil {
			t.Fatalf("set BuildRun %s/%s status: %v", br.GetNamespace(), br.GetName(), err)
		}
	}
}

// brwNewStream builds a catalog ImageStream with the given declared tags.
func brwNewStream(ns, name string, tags ...string) *imageflowv1alpha1.ImageStream {
	stream := &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
	}
	for _, tag := range tags {
		stream.Spec.Tags = append(stream.Spec.Tags, imageflowv1alpha1.TagReference{Name: tag})
	}
	return stream
}

func brwCreate(t *testing.T, cl client.Client, obj client.Object, what string) {
	t.Helper()
	if err := cl.Create(context.Background(), obj); err != nil {
		t.Fatalf("create %s: %v", what, err)
	}
}

// --- read-back helpers ------------------------------------------------------

func brwGetBuildRun(t *testing.T, cl client.Client, ns, name string) *unstructured.Unstructured {
	t.Helper()
	br := &unstructured.Unstructured{}
	br.SetGroupVersionKind(buildRunGVK)
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, br); err != nil {
		t.Fatalf("get BuildRun %s/%s: %v", ns, name, err)
	}
	return br
}

// brwRegisteredCondition extracts the watcher's Registered condition from a
// BuildRun, or nil when absent.
func brwRegisteredCondition(t *testing.T, br *unstructured.Unstructured) map[string]interface{} {
	t.Helper()
	conds, found, err := unstructured.NestedSlice(br.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if ok && m["type"] == buildRunRegisteredConditionType {
			return m
		}
	}
	return nil
}

// brwRequireCondition asserts status/reason and message content of a
// Registered condition — the ticket #18 requirement that every test asserts
// the BuildRun status condition explicitly.
func brwRequireCondition(t *testing.T, cond map[string]interface{}, wantStatus, wantReason string, wantMessageParts ...string) {
	t.Helper()
	if cond == nil {
		t.Fatalf("expected Registered condition (status=%s reason=%s), found none", wantStatus, wantReason)
	}
	if got := cond["status"]; got != wantStatus {
		t.Errorf("Registered condition status = %v, want %s (condition: %v)", got, wantStatus, cond)
	}
	if got := cond["reason"]; got != wantReason {
		t.Errorf("Registered condition reason = %v, want %s (condition: %v)", got, wantReason, cond)
	}
	message, _ := cond["message"].(string)
	for _, part := range wantMessageParts {
		if !strings.Contains(message, part) {
			t.Errorf("Registered condition message %q does not contain %q", message, part)
		}
	}
	if ltt, _ := cond["lastTransitionTime"].(string); ltt == "" {
		t.Errorf("Registered condition lastTransitionTime must be set (condition: %v)", cond)
	}
}

// brwRequireNoRegisteredCondition proves a BuildRun was left unprocessed: no
// Registered condition may appear (ticket #18: failed BuildRuns are not
// processed, no Condition change).
func brwRequireNoRegisteredCondition(t *testing.T, br *unstructured.Unstructured) {
	t.Helper()
	if cond := brwRegisteredCondition(t, br); cond != nil {
		t.Fatalf("expected no Registered condition, found: %v", cond)
	}
}

func brwGetStream(t *testing.T, cl client.Client, ns, name string) *imageflowv1alpha1.ImageStream {
	t.Helper()
	stream := &imageflowv1alpha1.ImageStream{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, stream); err != nil {
		t.Fatalf("get ImageStream %s/%s: %v", ns, name, err)
	}
	return stream
}

// brwRequireStreamAssociation asserts the (tag -> digest, reference)
// association on ImageStream.status — the ticket #18 requirement that every
// test asserts ImageStream.status.
//
//nolint:unparam // tag is parameterized for future test cases
func brwRequireStreamAssociation(t *testing.T, stream *imageflowv1alpha1.ImageStream, tag, digest, ref string, wantCreated time.Time) {
	t.Helper()
	var tagList *imageflowv1alpha1.NamedTagEventList
	for i := range stream.Status.Tags {
		if stream.Status.Tags[i].Tag == tag {
			tagList = &stream.Status.Tags[i]
			break
		}
	}
	if tagList == nil {
		t.Fatalf("ImageStream %s/%s status has no tag %q entry (status: %+v)", stream.Namespace, stream.Name, tag, stream.Status)
	}
	if len(tagList.Items) != 1 {
		t.Fatalf("ImageStream tag %q history length = %d, want exactly 1 item (items: %+v)", tag, len(tagList.Items), tagList.Items)
	}
	item := tagList.Items[0]
	if item.Digest != digest {
		t.Errorf("ImageStream tag %q item digest = %s, want %s", tag, item.Digest, digest)
	}
	if item.ImageReference != ref {
		t.Errorf("ImageStream tag %q item imageReference = %s, want %s", tag, item.ImageReference, ref)
	}
	if !item.Created.Time.Equal(wantCreated) {
		t.Errorf("ImageStream tag %q item created = %s, want %s", tag, item.Created.Time, wantCreated)
	}
}

// brwRequireStreamEmpty asserts the watcher left no association behind.
func brwRequireStreamEmpty(t *testing.T, stream *imageflowv1alpha1.ImageStream) {
	t.Helper()
	if len(stream.Status.Tags) != 0 {
		t.Fatalf("ImageStream %s/%s status.tags = %+v, want empty (no registration)", stream.Namespace, stream.Name, stream.Status.Tags)
	}
}

func brwArtifactNameForDigest(t *testing.T, digest string) string {
	t.Helper()
	name, err := ArtifactNameForDigest(digest)
	if err != nil {
		t.Fatalf("derive artifact name for %s: %v", digest, err)
	}
	return name
}

// brwRequireArtifact asserts the content-addressed ImageArtifact exists with
// the registered digest and reference.
func brwRequireArtifact(t *testing.T, cl client.Client, digest, ref string) {
	t.Helper()
	artifact := &imageflowv1alpha1.ImageArtifact{}
	key := types.NamespacedName{Name: brwArtifactNameForDigest(t, digest)}
	if err := cl.Get(context.Background(), key, artifact); err != nil {
		t.Fatalf("get ImageArtifact %s: %v", key.Name, err)
	}
	if artifact.Spec.Digest != digest {
		t.Errorf("ImageArtifact %s spec.digest = %s, want %s", key.Name, artifact.Spec.Digest, digest)
	}
	if artifact.Spec.ImageReference != ref {
		t.Errorf("ImageArtifact %s spec.imageReference = %s, want %s", key.Name, artifact.Spec.ImageReference, ref)
	}
}

// brwRequireNoArtifact asserts no ImageArtifact was created for the digest.
func brwRequireNoArtifact(t *testing.T, cl client.Client, digest string) {
	t.Helper()
	artifact := &imageflowv1alpha1.ImageArtifact{}
	key := types.NamespacedName{Name: brwArtifactNameForDigest(t, digest)}
	err := cl.Get(context.Background(), key, artifact)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no ImageArtifact %s, get err = %v", key.Name, err)
	}
}

// --- event helpers (lane 1: FakeRecorder) -----------------------------------

// brwRequireEvent asserts exactly one event with the given reason exists and
// carries the expected type prefix and message parts — the ticket #18
// requirement that every test asserts Event content.
func brwRequireEvent(t *testing.T, events []string, reason, wantTypePrefix string, wantMessageParts ...string) {
	t.Helper()
	if got := countEventsWithReason(events, reason); got != 1 {
		t.Fatalf("expected exactly 1 %s event, got %d (events: %v)", reason, got, events)
	}
	event := findEventWithReason(events, reason)
	if !strings.HasPrefix(event, wantTypePrefix+" ") {
		t.Errorf("event %q does not start with %q", event, wantTypePrefix)
	}
	for _, part := range wantMessageParts {
		if !strings.Contains(event, part) {
			t.Errorf("event %q does not contain %q", event, part)
		}
	}
}

func brwRequireEventCount(t *testing.T, events []string, want int) {
	t.Helper()
	if len(events) != want {
		t.Fatalf("expected %d events, got %d: %v", want, len(events), events)
	}
}

// --- eventually/consistently helpers (lane 2) --------------------------------

// brwEventually polls cond until it returns nil or the timeout expires,
// reporting the last failure.
func brwEventually(t *testing.T, what string, cond func() error) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(context.Background(), 25*time.Millisecond, 15*time.Second, true,
		func(ctx context.Context) (bool, error) {
			last = cond()
			return last == nil, nil
		})
	if err != nil {
		t.Fatalf("timed out waiting for %s: %v", what, last)
	}
}

// brwConsistently asserts cond stays true for the whole window.
func brwConsistently(t *testing.T, d time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if err := cond(); err != nil {
			t.Fatalf("%s violated during %s window: %v", what, d, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// --- lane 1 fixture: hand-built reconciler on the real client ----------------

// brwLane1 wires the watcher reconciler directly against the envtest client
// with a fake recorder and clock (deterministic, no other controllers running).
type brwLane1 struct {
	cl         client.Client
	recorder   *record.FakeRecorder
	reconciler *BuildRunReconciler
}

func newBRWLane1(t *testing.T) *brwLane1 {
	t.Helper()
	env := sharedBuildRunEnvtestEnv(t)
	rec := record.NewFakeRecorder(100)
	reconciler := NewBuildRunReconciler(
		env.client, env.client, rec, clock.NewFake(brwT0),
		config.Config{StreamTagHistoryLimit: 3},
	)
	return &brwLane1{cl: env.client, recorder: rec, reconciler: reconciler}
}

func (l *brwLane1) reconcile(t *testing.T, br *unstructured.Unstructured) {
	t.Helper()
	res, err := l.reconciler.Reconcile(context.Background(), requestFor(br))
	if err != nil {
		t.Fatalf("reconcile BuildRun %s/%s: %v", br.GetNamespace(), br.GetName(), err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("reconcile asked for requeue: %+v (failures must be terminal)", res)
	}
}

func (l *brwLane1) drainEvents() []string { return drainFakeEvents(l.recorder) }

// --- lane 1 tests: full ticket #18 matrix -----------------------------------

// Ticket matrix: Succeeded + digest non-empty + valid annotation →
// ImageStream.status updated + ImageArtifact created + Registered=True.
func TestBuildRunWatcherEnvtest_SucceededRegistersIntoCatalog(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	const stream, tag = "payments-api", "stable"
	brwCreate(t, lane.cl, brwNewStream(ns, stream, tag), "ImageStream")

	digest := brwDigest("a")
	ref := "quay.io/example/payments-api@" + digest
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-happy"), stream, tag)
	brwCreateBuildRun(t, lane.cl, br, brwStatus("True", digest, "quay.io/example/payments-api:latest"))

	lane.reconcile(t, br)

	// BuildRun status condition.
	persisted := brwGetBuildRun(t, lane.cl, ns, "br-happy")
	cond := brwRegisteredCondition(t, persisted)
	brwRequireCondition(t, cond, "True", buildRunReasonSucceeded,
		"Registered "+ref+" onto "+stream+":"+tag)
	if got, _ := cond["observedGeneration"].(int64); got != persisted.GetGeneration() {
		t.Errorf("Registered condition observedGeneration = %d, want %d", got, persisted.GetGeneration())
	}
	if got, _ := cond["lastTransitionTime"].(string); got != brwT0.Format(time.RFC3339) {
		t.Errorf("Registered condition lastTransitionTime = %s, want %s (injected clock)", got, brwT0.Format(time.RFC3339))
	}

	// ImageStream.status + ImageArtifact.
	brwRequireStreamAssociation(t, brwGetStream(t, lane.cl, ns, stream), tag, digest, ref, brwT0)
	brwRequireArtifact(t, lane.cl, digest, ref)

	// Events: artifact creation, Image Change, success audit — content checked.
	events := lane.drainEvents()
	brwRequireEventCount(t, events, 3)
	brwRequireEvent(t, events, EventReasonImageArtifactCreated, "Normal",
		"Created cluster-scoped ImageArtifact for "+digest,
		"source: BuildRun "+ns+"/br-happy")
	brwRequireEvent(t, events, EventReasonImageChanged, "Normal",
		`Tag "stable" now references `+digest,
		"(previous: none)",
		"source: BuildRun "+ns+"/br-happy")
	brwRequireEvent(t, events, EventReasonBuildRunRegistered, "Normal",
		"Registered "+ref+" onto "+stream+":"+tag)
}

// Ticket matrix: Succeeded + digest empty → Registered=False/MissingDigest
// Condition + Warning Event.
func TestBuildRunWatcherEnvtest_SucceededEmptyDigestStampsMissingDigest(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	const stream, tag = "payments-api", "stable"
	brwCreate(t, lane.cl, brwNewStream(ns, stream, tag), "ImageStream")

	digest := brwDigest("b")
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-nodigest"), stream, tag)
	brwCreateBuildRun(t, lane.cl, br, brwStatus("True", "", "quay.io/example/payments-api:latest"))

	lane.reconcile(t, br)

	// BuildRun status condition (ADR-011: explicit failure, no silent skip).
	cond := brwRegisteredCondition(t, brwGetBuildRun(t, lane.cl, ns, "br-nodigest"))
	brwRequireCondition(t, cond, "False", buildRunReasonMissingDigest,
		".status.output.digest is empty",
		"build strategy may not provide system results")

	// Nothing touched the catalog.
	brwRequireStreamEmpty(t, brwGetStream(t, lane.cl, ns, stream))
	brwRequireNoArtifact(t, lane.cl, digest)

	// Warning event with the failure-code prefix.
	events := lane.drainEvents()
	brwRequireEventCount(t, events, 1)
	brwRequireEvent(t, events, EventReasonBuildRunFailed, "Warning",
		buildRunReasonMissingDigest+": ",
		".status.output.digest is empty")
}

// Ticket matrix: Failed BuildRun → not processed (no Condition change).
func TestBuildRunWatcherEnvtest_FailedBuildRunIsIgnored(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	const stream, tag = "payments-api", "stable"
	brwCreate(t, lane.cl, brwNewStream(ns, stream, tag), "ImageStream")

	digest := brwDigest("c")
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-failed"), stream, tag)
	// Even a digest must not tempt the watcher: Succeeded=False is terminal-ignore.
	brwCreateBuildRun(t, lane.cl, br, brwStatus("False", digest, "quay.io/example/payments-api:latest"))

	lane.reconcile(t, br)

	persisted := brwGetBuildRun(t, lane.cl, ns, "br-failed")
	brwRequireNoRegisteredCondition(t, persisted)
	brwRequireStreamEmpty(t, brwGetStream(t, lane.cl, ns, stream))
	brwRequireNoArtifact(t, lane.cl, digest)
	brwRequireEventCount(t, lane.drainEvents(), 0)
}

// Ticket matrix: same (stream, tag, digest) repeated → no-op, no duplicate
// Event. The real API server makes the idempotence observable as unchanged
// resourceVersions: a true no-op writes nothing.
func TestBuildRunWatcherEnvtest_RepeatedRegistrationIsNoop(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	const stream, tag = "payments-api", "stable"
	brwCreate(t, lane.cl, brwNewStream(ns, stream, tag), "ImageStream")

	digest := brwDigest("d")
	ref := "quay.io/example/payments-api@" + digest
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-repeat"), stream, tag)
	brwCreateBuildRun(t, lane.cl, br, brwStatus("True", digest, "quay.io/example/payments-api:latest"))

	// First pass: registers, stamps condition, emits the three events.
	lane.reconcile(t, br)
	brwRequireEventCount(t, lane.drainEvents(), 3)
	brBefore := brwGetBuildRun(t, lane.cl, ns, "br-repeat")
	streamBefore := brwGetStream(t, lane.cl, ns, stream)
	condBefore := brwRegisteredCondition(t, brBefore)
	if condBefore == nil {
		t.Fatal("first reconcile must stamp the Registered condition")
	}

	// Second pass: identical persisted state → complete no-op.
	lane.reconcile(t, br)

	brAfter := brwGetBuildRun(t, lane.cl, ns, "br-repeat")
	if brAfter.GetResourceVersion() != brBefore.GetResourceVersion() {
		t.Errorf("BuildRun resourceVersion changed on idempotent re-reconcile (%s → %s): condition was re-stamped",
			brBefore.GetResourceVersion(), brAfter.GetResourceVersion())
	}
	streamAfter := brwGetStream(t, lane.cl, ns, stream)
	if streamAfter.GetResourceVersion() != streamBefore.GetResourceVersion() {
		t.Errorf("ImageStream resourceVersion changed on idempotent re-reconcile (%s → %s): status was rewritten",
			streamBefore.GetResourceVersion(), streamAfter.GetResourceVersion())
	}
	brwRequireStreamAssociation(t, streamAfter, tag, digest, ref, brwT0)
	brwRequireEventCount(t, lane.drainEvents(), 0)
}

// Ticket matrix: target stream not found → Registered=False/StreamNotFound;
// and ST-004 persisted recovery: once the stream exists, the same BuildRun
// recovers to Registered=True.
func TestBuildRunWatcherEnvtest_StreamNotFoundStampsConditionAndRecovers(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	const stream, tag = "payments-api", "stable"
	digest := brwDigest("0")
	ref := "quay.io/example/payments-api@" + digest
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-nostream"), stream, tag)
	brwCreateBuildRun(t, lane.cl, br, brwStatus("True", digest, "quay.io/example/payments-api:latest"))

	// Stream missing: terminal failure with explicit Condition.
	lane.reconcile(t, br)

	cond := brwRegisteredCondition(t, brwGetBuildRun(t, lane.cl, ns, "br-nostream"))
	brwRequireCondition(t, cond, "False", buildRunReasonStreamNotFound,
		`ImageStream "payments-api" not found in namespace "`+ns+`"`,
		"create the stream first or fix annotation",
		buildRunTargetAnnotation)
	brwRequireNoArtifact(t, lane.cl, digest)

	events := lane.drainEvents()
	brwRequireEventCount(t, events, 1)
	brwRequireEvent(t, events, EventReasonBuildRunFailed, "Warning",
		buildRunReasonStreamNotFound+": ",
		`ImageStream "payments-api" not found`)

	// Recovery: create the stream and re-reconcile the same BuildRun.
	brwCreate(t, lane.cl, brwNewStream(ns, stream, tag), "ImageStream")
	lane.reconcile(t, br)

	cond = brwRegisteredCondition(t, brwGetBuildRun(t, lane.cl, ns, "br-nostream"))
	brwRequireCondition(t, cond, "True", buildRunReasonSucceeded,
		"Registered "+ref+" onto "+stream+":"+tag)
	brwRequireStreamAssociation(t, brwGetStream(t, lane.cl, ns, stream), tag, digest, ref, brwT0)
	brwRequireArtifact(t, lane.cl, digest, ref)

	events = lane.drainEvents()
	brwRequireEventCount(t, events, 3)
	brwRequireEvent(t, events, EventReasonBuildRunRegistered, "Normal",
		"Registered "+ref+" onto "+stream+":"+tag)
	brwRequireEvent(t, events, EventReasonImageChanged, "Normal",
		`Tag "stable" now references `+digest)
}

// Ticket matrix: valid annotation JSON whose stream/tag point at a
// nonexistent stream → explicit Condition. The annotation parses cleanly
// (extra fields tolerated), so the reason must be StreamNotFound, not
// InvalidTarget.
func TestBuildRunWatcherEnvtest_ValidTargetJSONPointingAtMissingStreamStampsStreamNotFound(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	digest := brwDigest("1")
	br := brwWithRawAnnotation(brwNewBuildRun(ns, "br-ghost"),
		`{"stream":"ghost","tag":"edge","comment":"extra fields are still valid JSON"}`)
	brwCreateBuildRun(t, lane.cl, br, brwStatus("True", digest, "quay.io/example/ghost:latest"))

	lane.reconcile(t, br)

	cond := brwRegisteredCondition(t, brwGetBuildRun(t, lane.cl, ns, "br-ghost"))
	brwRequireCondition(t, cond, "False", buildRunReasonStreamNotFound,
		`ImageStream "ghost" not found in namespace "`+ns+`"`)

	events := lane.drainEvents()
	brwRequireEventCount(t, events, 1)
	brwRequireEvent(t, events, EventReasonBuildRunFailed, "Warning",
		buildRunReasonStreamNotFound+": ")
}

// ADR-011 taxonomy completeness: malformed annotation JSON, and valid JSON
// with an empty field, both stamp Registered=False/InvalidTarget (webhooks
// can be bypassed or disabled, so the watcher fails loudly itself).
func TestBuildRunWatcherEnvtest_InvalidTargetAnnotationStampsInvalidTarget(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	digest := brwDigest("2")
	cases := []struct {
		name      string
		raw       string
		wantParts []string
	}{
		{"br-badjson", `not-json`, []string{"is not valid JSON"}},
		{"br-notag", `{"stream":"payments-api"}`, []string{"must set non-empty stream and tag"}},
		{"br-nostream", `{"tag":"stable"}`, []string{"must set non-empty stream and tag"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := brwWithRawAnnotation(brwNewBuildRun(ns, tc.name), tc.raw)
			brwCreateBuildRun(t, lane.cl, br, brwStatus("True", digest, "quay.io/example/payments-api:latest"))

			lane.reconcile(t, br)

			cond := brwRegisteredCondition(t, brwGetBuildRun(t, lane.cl, ns, tc.name))
			wantParts := append([]string{}, tc.wantParts...)
			brwRequireCondition(t, cond, "False", buildRunReasonInvalidTarget, wantParts...)

			events := lane.drainEvents()
			brwRequireEventCount(t, events, 1)
			brwRequireEvent(t, events, EventReasonBuildRunFailed, "Warning",
				append([]string{buildRunReasonInvalidTarget + ": "}, tc.wantParts...)...)
		})
	}

	// Nothing reached the catalog from any of the invalid BuildRuns.
	brwRequireNoArtifact(t, lane.cl, digest)
}

// ADR-011 taxonomy completeness: digest present but no image reference
// anywhere (.status.buildSpec.output.image and .spec.output.image both empty)
// → Registered=False/MissingImageReference.
func TestBuildRunWatcherEnvtest_MissingImageReferenceStampsCondition(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	const stream, tag = "payments-api", "stable"
	brwCreate(t, lane.cl, brwNewStream(ns, stream, tag), "ImageStream")

	digest := brwDigest("3")
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-noimage"), stream, tag)
	brwCreateBuildRun(t, lane.cl, br, brwStatus("True", digest, ""))

	lane.reconcile(t, br)

	cond := brwRegisteredCondition(t, brwGetBuildRun(t, lane.cl, ns, "br-noimage"))
	brwRequireCondition(t, cond, "False", buildRunReasonMissingImageReference,
		"cannot derive the image repository", digest)

	brwRequireStreamEmpty(t, brwGetStream(t, lane.cl, ns, stream))
	brwRequireNoArtifact(t, lane.cl, digest)

	events := lane.drainEvents()
	brwRequireEventCount(t, events, 1)
	brwRequireEvent(t, events, EventReasonBuildRunFailed, "Warning",
		buildRunReasonMissingImageReference+": ")
}

// AGG-004: the watcher may only write ImageStream/ImageArtifact aggregates
// through the shared CatalogRegistration; it must not directly mutate
// subscription, delivery, or observation state. With the watcher running in
// isolation (lane 1), a full registration must leave the ImageSubscription
// bit-for-bit identical.
func TestBuildRunWatcherEnvtest_DoesNotTouchSubscriptionState(t *testing.T) {
	lane := newBRWLane1(t)
	ns := brwCreateNamespace(t, lane.cl)

	const stream, tag = "payments-api", "stable"
	brwCreate(t, lane.cl, brwNewStream(ns, stream, tag), "ImageStream")

	subscription := &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "agg-guard"},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      "ImageStreamTag",
				Name:      stream + ":" + tag,
				Namespace: ns,
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{{
				TargetRef: imageflowv1alpha1.TargetReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Namespace:  ns,
					Name:       "payments-api",
				},
				ContainerNames: []string{"payments-api"},
			}},
		},
	}
	brwCreate(t, lane.cl, subscription, "ImageSubscription")
	before := &imageflowv1alpha1.ImageSubscription{}
	if err := lane.cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "agg-guard"}, before); err != nil {
		t.Fatalf("get ImageSubscription: %v", err)
	}

	digest := brwDigest("4")
	ref := "quay.io/example/payments-api@" + digest
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-agg"), stream, tag)
	brwCreateBuildRun(t, lane.cl, br, brwStatus("True", digest, "quay.io/example/payments-api:latest"))

	lane.reconcile(t, br)

	// The registration itself happened (the test would be vacuous otherwise).
	brwRequireCondition(t,
		brwRegisteredCondition(t, brwGetBuildRun(t, lane.cl, ns, "br-agg")),
		"True", buildRunReasonSucceeded, "Registered "+ref+" onto "+stream+":"+tag)
	brwRequireStreamAssociation(t, brwGetStream(t, lane.cl, ns, stream), tag, digest, ref, brwT0)
	brwRequireArtifact(t, lane.cl, digest, ref)

	after := &imageflowv1alpha1.ImageSubscription{}
	if err := lane.cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "agg-guard"}, after); err != nil {
		t.Fatalf("get ImageSubscription after reconcile: %v", err)
	}
	if after.GetResourceVersion() != before.GetResourceVersion() {
		t.Errorf("AGG-004 violation: ImageSubscription resourceVersion changed (%s → %s)",
			before.GetResourceVersion(), after.GetResourceVersion())
	}
	if !reflect.DeepEqual(after.Spec, before.Spec) {
		t.Errorf("AGG-004 violation: ImageSubscription spec changed: %+v → %+v", before.Spec, after.Spec)
	}
	if !reflect.DeepEqual(after.Status, before.Status) {
		t.Errorf("AGG-004 violation: ImageSubscription status changed: %+v → %+v", before.Status, after.Status)
	}
}

// --- lane 2: production manager wiring ---------------------------------------

// brwFreshMetricsRegistry swaps controller-runtime's process-global metrics
// registry for an empty one. Production runs Setup exactly once per process,
// but this suite exercises Setup in several lanes of the same test binary;
// the registry client (internal/registry) hard-registers its collectors per
// construction and would fail the second Setup with "duplicate metrics
// collector registration attempted". A fresh registry per lane keeps each
// Setup hermetic without touching production wiring.
func brwFreshMetricsRegistry(t *testing.T) {
	t.Helper()
	ctrlmetrics.Registry = prometheus.NewRegistry()
}

// brwResetProcessGlobals restores the process-global slots that a production
// Setup fills, so later tests in this binary keep their documented
// preconditions (path_adapter_test.go pins the PathAdapter slot empty).
func brwResetProcessGlobals(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { delivery.RegisterPathAdapter(nil) })
}

// brwStartManager builds a manager through the production Setup wiring and
// starts it. SkipNameValidation is enabled because this test binary also
// builds the production controller names on fake managers (unit tests), and
// controller-runtime validates names process-globally.
//
//nolint:unparam // enableWatcher varies across call sites; Manager return used by some callers
func brwStartManager(t *testing.T, env *brwSharedEnv, enableWatcher bool) ctrl.Manager {
	t.Helper()
	brwFreshMetricsRegistry(t)
	brwResetProcessGlobals(t)
	mgr, err := ctrl.NewManager(env.cfg, ctrl.Options{
		Scheme:                 env.scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		Controller:             ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	if err := Setup(mgr, Options{
		Clock:  clock.Real{},
		Config: config.Config{EnableBuildRunWatcher: enableWatcher, StreamTagHistoryLimit: 3},
	}); err != nil {
		t.Fatalf("controller.Setup: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("manager cache did not sync")
	}
	return mgr
}

// brwListEvents returns all kube Events (the suite's objects are unique per
// test, so no namespace scoping is needed).
func brwListEvents(t *testing.T, cl client.Client) []corev1.Event {
	t.Helper()
	list := &corev1.EventList{}
	if err := cl.List(context.Background(), list); err != nil {
		t.Fatalf("list events: %v", err)
	}
	return list.Items
}

func brwFilterEvents(items []corev1.Event, involvedName, reason string) []corev1.Event {
	var out []corev1.Event
	for _, e := range items {
		if e.InvolvedObject.Name == involvedName && e.Reason == reason {
			out = append(out, e)
		}
	}
	return out
}

// Ticket matrix, manager-driven: the production wiring (controller.Setup with
// the feature flag on) discovers the Shipwright CRDs through the RESTMapper,
// starts the informer/workqueue, registers the build output into the catalog
// and records a real kube Event — then stays idempotent when the BuildRun is
// re-queued by an unrelated update.
func TestBuildRunWatcherEnvtest_ManagerDrivenEndToEnd(t *testing.T) {
	env := sharedBuildRunEnvtestEnv(t)
	brwStartManager(t, env, true)
	cl := env.client
	ns := brwCreateNamespace(t, cl)

	const stream, tag = "checkout", "edge"
	brwCreate(t, cl, brwNewStream(ns, stream, tag), "ImageStream")

	digest := brwDigest("5")
	repo := "registry.local:5000/team/checkout"
	ref := repo + "@" + digest
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-e2e"), stream, tag)
	// Exercise the .spec.output.image fallback (status.buildSpec absent) and a
	// registry port, both through the real informer.
	if err := unstructured.SetNestedField(br.Object, repo+":latest", "spec", "output", "image"); err != nil {
		t.Fatalf("set spec.output.image: %v", err)
	}
	brwCreateBuildRun(t, cl, br, brwStatus("True", digest, ""))

	// Registered condition on the BuildRun (stamped via the status subresource).
	brwEventually(t, "Registered=True/Succeeded condition", func() error {
		cond := brwRegisteredCondition(t, brwGetBuildRun(t, cl, ns, "br-e2e"))
		if cond == nil {
			return fmt.Errorf("no Registered condition yet")
		}
		if cond["status"] != "True" || cond["reason"] != buildRunReasonSucceeded {
			return fmt.Errorf("Registered condition = %v", cond)
		}
		if msg, _ := cond["message"].(string); !strings.Contains(msg, "Registered "+ref+" onto "+stream+":"+tag) {
			return fmt.Errorf("condition message = %q", msg)
		}
		return nil
	})

	// ImageStream.status association and content-addressed artifact.
	brwEventually(t, "ImageStream status association", func() error {
		s := brwGetStream(t, cl, ns, stream)
		for _, tl := range s.Status.Tags {
			if tl.Tag == tag && len(tl.Items) == 1 && tl.Items[0].Digest == digest && tl.Items[0].ImageReference == ref {
				return nil
			}
		}
		return fmt.Errorf("status.tags = %+v", s.Status.Tags)
	})
	brwEventually(t, "ImageArtifact", func() error {
		artifact := &imageflowv1alpha1.ImageArtifact{}
		key := types.NamespacedName{Name: brwArtifactNameForDigest(t, digest)}
		if err := cl.Get(context.Background(), key, artifact); err != nil {
			return err
		}
		if artifact.Spec.Digest != digest || artifact.Spec.ImageReference != ref {
			return fmt.Errorf("artifact spec = %+v", artifact.Spec)
		}
		return nil
	})

	// Real kube Events recorded through the manager's event broadcaster.
	brwEventually(t, "BuildRunRegistered kube Event", func() error {
		matches := brwFilterEvents(brwListEvents(t, cl), "br-e2e", EventReasonBuildRunRegistered)
		if len(matches) != 1 {
			return fmt.Errorf("BuildRunRegistered events = %d", len(matches))
		}
		e := matches[0]
		if e.Type != corev1.EventTypeNormal {
			return fmt.Errorf("event type = %s", e.Type)
		}
		if !strings.Contains(e.Message, "Registered "+ref+" onto "+stream+":"+tag) {
			return fmt.Errorf("event message = %q", e.Message)
		}
		return nil
	})
	brwEventually(t, "ImageChanged kube Event on the stream", func() error {
		matches := brwFilterEvents(brwListEvents(t, cl), stream, EventReasonImageChanged)
		if len(matches) != 1 {
			return fmt.Errorf("ImageChanged events = %d", len(matches))
		}
		if !strings.Contains(matches[0].Message, "source: BuildRun "+ns+"/br-e2e") {
			return fmt.Errorf("event message = %q", matches[0].Message)
		}
		return nil
	})

	// Idempotence: an unrelated update re-queues the BuildRun; no duplicate
	// audit event and no duplicate history entry may appear.
	brwEventually(t, "label touch propagated", func() error {
		current := brwGetBuildRun(t, cl, ns, "br-e2e")
		labels := current.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["appfun.dev/requeue"] = "yes"
		current.SetLabels(labels)
		if err := cl.Update(context.Background(), current); err != nil {
			return fmt.Errorf("touch BuildRun: %w", err)
		}
		return nil
	})
	brwConsistently(t, 2*time.Second, "idempotent steady state", func() error {
		if n := len(brwFilterEvents(brwListEvents(t, cl), "br-e2e", EventReasonBuildRunRegistered)); n != 1 {
			return fmt.Errorf("BuildRunRegistered events = %d, want exactly 1", n)
		}
		s := brwGetStream(t, cl, ns, stream)
		for _, tl := range s.Status.Tags {
			if tl.Tag == tag && len(tl.Items) != 1 {
				return fmt.Errorf("tag %q history length = %d, want 1", tag, len(tl.Items))
			}
		}
		return nil
	})
}

// Flag-off acceptance (ADR-004 §4): with --enable-buildrun-watcher=false the
// watcher is not registered at all — zero informers — so even a perfectly
// annotated, succeeded BuildRun is never processed.
func TestBuildRunWatcherEnvtest_FlagOffRegistersNothing(t *testing.T) {
	env := sharedBuildRunEnvtestEnv(t)
	mgr := brwStartManager(t, env, false)
	cl := env.client
	ns := brwCreateNamespace(t, cl)

	const stream, tag = "payments-api", "stable"
	brwCreate(t, cl, brwNewStream(ns, stream, tag), "ImageStream")

	digest := brwDigest("6")
	br := brwWithTargetAnnotation(brwNewBuildRun(ns, "br-flagoff"), stream, tag)
	brwCreateBuildRun(t, cl, br, brwStatus("True", digest, "quay.io/example/payments-api:latest"))

	// Sanity: the manager is up and serves the watched type via its cache even
	// though no informer was registered for it (direct API read instead).
	_ = mgr

	brwConsistently(t, 3*time.Second, "flag-off quiescence", func() error {
		if cond := brwRegisteredCondition(t, brwGetBuildRun(t, cl, ns, "br-flagoff")); cond != nil {
			return fmt.Errorf("Registered condition appeared with the watcher disabled: %v", cond)
		}
		if s := brwGetStream(t, cl, ns, stream); len(s.Status.Tags) != 0 {
			return fmt.Errorf("stream status mutated with the watcher disabled: %+v", s.Status.Tags)
		}
		artifact := &imageflowv1alpha1.ImageArtifact{}
		key := types.NamespacedName{Name: brwArtifactNameForDigest(t, digest)}
		if err := cl.Get(context.Background(), key, artifact); !apierrors.IsNotFound(err) {
			return fmt.Errorf("artifact lookup err = %v, want NotFound", err)
		}
		return nil
	})
}

// CRD gate (ADR-004 §4): flag=true with the Shipwright CRDs absent must fail
// manager setup with a clear error; flag=false on the same cluster is fine.
func TestBuildRunWatcherEnvtest_MissingShipwrightCRDFailsSetup(t *testing.T) {
	assets := buildRunWatcherEnvtestAssets(t)
	if assets == "" {
		t.Skip("envtest assets not found: set KUBEBUILDER_ASSETS or run `make envtest`")
	}
	// This environment deliberately excludes the Shipwright fixtures.
	env, err := startBuildRunEnvtest(assets, false)
	if err != nil {
		t.Fatalf("start envtest without Shipwright CRDs: %v", err)
	}
	t.Cleanup(func() {
		if err := env.env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})

	newManager := func(t *testing.T) ctrl.Manager {
		t.Helper()
		mgr, err := ctrl.NewManager(env.cfg, ctrl.Options{
			Scheme:                 env.scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			LeaderElection:         false,
			Controller:             ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
		})
		if err != nil {
			t.Fatalf("create manager: %v", err)
		}
		return mgr
	}

	// flag=true + CRD missing → hard startup failure naming the missing CRD.
	brwFreshMetricsRegistry(t)
	brwResetProcessGlobals(t)
	err = Setup(newManager(t), Options{
		Clock:  clock.Real{},
		Config: config.Config{EnableBuildRunWatcher: true, StreamTagHistoryLimit: 3},
	})
	if err == nil {
		t.Fatal("expected Setup error when flag=true and the Shipwright Build CRD is missing")
	}
	for _, part := range []string{"shipwright.io", "Build", "--enable-buildrun-watcher"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("Setup error %q does not mention %q", err, part)
		}
	}

	// flag=false on the same CRD-less cluster → Setup succeeds and the gate is
	// never consulted.
	brwFreshMetricsRegistry(t)
	if err := Setup(newManager(t), Options{
		Clock:  clock.Real{},
		Config: config.Config{EnableBuildRunWatcher: false, StreamTagHistoryLimit: 3},
	}); err != nil {
		t.Fatalf("expected nil Setup error when flag=false, got: %v", err)
	}
}
