package webhook

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	crwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// T4 (ticket #17) envtest suite: the BuildRun target-annotation admission
// contract (ADR-011 §2) verified end-to-end against a real API server —
// ValidatingWebhookConfiguration routing (vbuildrun.kb.io) → the handler
// registered through the production seam (Register) → the rejection message
// surfaced to the client. buildrun_validate_test.go covers the handler in
// isolation; this suite covers the wiring.
//
// BC-003: BuildRun objects are built as unstructured; the Shipwright Go types
// are never imported. The API server learns the kind from the minimal CRD
// stand-in in test/testdata/thirdparty (same convention as the EdgeService
// fixture used by the imageFields suites).
//
// The environment starts once per package run in TestMain and only when
// KUBEBUILDER_ASSETS is set (`make test` provides it via setup-envtest);
// without assets every test here skips, matching test/integration.

var (
	envtestClient client.Client
	envtestNS     string
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		// Unit-only run: the envtest cases below skip themselves.
		os.Exit(m.Run())
	}
	shutdown, err := startBuildRunAdmissionEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "buildrun admission envtest startup: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	shutdown()
	os.Exit(code)
}

// startBuildRunAdmissionEnv boots envtest with the generated webhook
// configurations (config/webhook) and serves the handlers through the same
// Register seam production uses, on the envtest-issued serving certificate —
// the webhook-half of the test/integration/webhook_test.go pattern, scoped to
// what the BuildRun admission contract needs (no ImageFlow CRDs: the suite
// only ever creates BuildRuns and namespaces).
func startBuildRunAdmissionEnv() (func(), error) {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	env := &envtest.Environment{
		BinaryAssetsDirectory: os.Getenv("KUBEBUILDER_ASSETS"),
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "test", "testdata", "thirdparty"),
		},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			Paths: []string{filepath.Join("..", "..", "config", "webhook")},
		},
	}
	cfg, err := env.Start()
	if err != nil {
		return nil, fmt.Errorf("start envtest: %w", err)
	}
	fail := func(err error) (func(), error) {
		_ = env.Stop()
		return nil, err
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return fail(fmt.Errorf("add clientgo scheme: %w", err))
	}
	if err := imageflowv1alpha1.AddToScheme(scheme); err != nil {
		return fail(fmt.Errorf("add imageflow scheme: %w", err))
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: crwebhook.NewServer(crwebhook.Options{
			Host:    env.WebhookInstallOptions.LocalServingHost,
			Port:    env.WebhookInstallOptions.LocalServingPort,
			CertDir: env.WebhookInstallOptions.LocalServingCertDir,
		}),
	})
	if err != nil {
		return fail(fmt.Errorf("new manager: %w", err))
	}
	Register(mgr)

	mgrCtx, mgrCancel := context.WithCancel(context.Background())
	mgrErr := make(chan error, 1)
	go func() { mgrErr <- mgr.Start(mgrCtx) }()
	started := mgr.GetWebhookServer().StartedChecker()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := started(nil); err == nil {
			break
		}
		select {
		case err := <-mgrErr:
			mgrCancel()
			return fail(fmt.Errorf("manager exited before the webhook server started: %w", err))
		default:
		}
		if time.Now().After(deadline) {
			mgrCancel()
			return fail(fmt.Errorf("webhook server did not start within 20s"))
		}
		time.Sleep(100 * time.Millisecond)
	}

	envtestClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		mgrCancel()
		return fail(fmt.Errorf("new client: %w", err))
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "buildrun-admission-"}}
	if err := envtestClient.Create(context.Background(), ns); err != nil {
		mgrCancel()
		return fail(fmt.Errorf("create namespace: %w", err))
	}
	envtestNS = ns.Name

	return func() {
		mgrCancel()
		if err := env.Stop(); err != nil {
			fmt.Fprintf(os.Stderr, "stop envtest: %v\n", err)
		}
	}, nil
}

func requireBuildRunAdmissionEnv(t *testing.T) {
	t.Helper()
	if envtestClient == nil {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test` (setup-envtest)")
	}
}

// envtestBuildRun builds a Shipwright BuildRun carrying only what the
// admission contract reads: apiVersion/kind + metadata (BC-003).
func envtestBuildRun(name string, annotations map[string]string) *unstructured.Unstructured {
	br := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "shipwright.io/v1beta1",
		"kind":       "BuildRun",
		"metadata": map[string]interface{}{
			"namespace": envtestNS,
			"name":      name,
		},
	}}
	br.SetAnnotations(annotations)
	return br
}

// expectBuildRunDenied pins the full client-visible rejection shape: routed
// through the BuildRun webhook (registration proof), surfaced as Invalid
// (422) like every other ImageFlow validator, and carrying the contract
// keyword so users can tell which constraint fired.
func expectBuildRunDenied(t *testing.T, err error, wantKeyword string) {
	t.Helper()
	if err == nil {
		t.Fatal("request must be denied")
	}
	if !strings.Contains(err.Error(), "vbuildrun.kb.io") {
		t.Fatalf("rejection must come from the BuildRun webhook (vbuildrun.kb.io), got: %v", err)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("rejection must surface as Invalid (422), got: %v", err)
	}
	if !strings.Contains(err.Error(), wantKeyword) {
		t.Fatalf("rejection message must contain %q (it must name the failing constraint), got: %v",
			wantKeyword, err)
	}
}

// TestBuildRunAdmissionMatrix is the ticket #17 acceptance matrix: every
// named case asserts the admission outcome AND the rejection-message keyword
// through a real API server round trip.
func TestBuildRunAdmissionMatrix(t *testing.T) {
	requireBuildRunAdmissionEnv(t)
	ctx := context.Background()

	t.Run("valid stream and tag annotation is allowed", func(t *testing.T) {
		br := envtestBuildRun("br-valid", map[string]string{
			BuildRunTargetAnnotation: `{"stream":"my-stream","tag":"latest"}`,
		})
		if err := envtestClient.Create(ctx, br); err != nil {
			t.Fatalf("valid annotation must be admitted: %v", err)
		}
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(br.GroupVersionKind())
		if err := envtestClient.Get(ctx, client.ObjectKeyFromObject(br), got); err != nil {
			t.Fatalf("admitted BuildRun must persist: %v", err)
		}
	})

	t.Run("BuildRun without the annotation is allowed", func(t *testing.T) {
		br := envtestBuildRun("br-no-annotation", nil)
		if err := envtestClient.Create(ctx, br); err != nil {
			t.Fatalf("the watcher only consumes annotated BuildRuns; an unannotated one must pass: %v", err)
		}
	})

	t.Run("unrelated annotations are allowed", func(t *testing.T) {
		br := envtestBuildRun("br-unrelated", map[string]string{
			"shipwright.io/buildRun.name": "kaniko-build",
		})
		if err := envtestClient.Create(ctx, br); err != nil {
			t.Fatalf("unrelated annotations must not be validated: %v", err)
		}
	})

	rejected := []struct {
		name        string
		annotation  string
		wantKeyword string
	}{
		{"invalid JSON syntax", `{"stream":"my-stream",`, "invalid JSON"},
		{"JSON array instead of object", `["my-stream","latest"]`, "must be JSON object"},
		{"JSON string instead of object", `"my-stream:latest"`, "must be JSON object"},
		{"missing stream field", `{"tag":"latest"}`, "stream"},
		{"missing tag field", `{"stream":"my-stream"}`, "tag"},
		{"stream value violates RFC 1123", `{"stream":"My-Stream","tag":"latest"}`, "RFC 1123"},
		{"tag value violates RFC 1123", `{"stream":"my-stream","tag":"st@ble"}`, "RFC 1123"},
	}
	for _, tc := range rejected {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			br := envtestBuildRun("br-"+strings.ReplaceAll(strings.ToLower(tc.name), " ", "-"),
				map[string]string{BuildRunTargetAnnotation: tc.annotation})
			err := envtestClient.Create(ctx, br)
			expectBuildRunDenied(t, err, tc.wantKeyword)

			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(br.GroupVersionKind())
			getErr := envtestClient.Get(ctx, client.ObjectKeyFromObject(br), got)
			if !apierrors.IsNotFound(getErr) {
				t.Fatalf("a rejected create must not persist the object, got: %v", getErr)
			}
		})
	}
}

// TestBuildRunAdmissionUpdateRouting proves the webhook configuration covers
// the update verb too (kubebuilder marker verbs=create;update): an object
// admitted cleanly must still be protected from an update that corrupts the
// annotation.
func TestBuildRunAdmissionUpdateRouting(t *testing.T) {
	requireBuildRunAdmissionEnv(t)
	ctx := context.Background()

	br := envtestBuildRun("br-update", map[string]string{
		BuildRunTargetAnnotation: `{"stream":"my-stream","tag":"latest"}`,
	})
	if err := envtestClient.Create(ctx, br); err != nil {
		t.Fatalf("create with a valid annotation must be admitted: %v", err)
	}

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(br.GroupVersionKind())
	if err := envtestClient.Get(ctx, client.ObjectKeyFromObject(br), got); err != nil {
		t.Fatalf("get stored BuildRun: %v", err)
	}
	got.SetAnnotations(map[string]string{BuildRunTargetAnnotation: `{"tag":"latest"}`})
	err := envtestClient.Update(ctx, got)
	expectBuildRunDenied(t, err, "stream")

	after := &unstructured.Unstructured{}
	after.SetGroupVersionKind(br.GroupVersionKind())
	if err := envtestClient.Get(ctx, client.ObjectKeyFromObject(br), after); err != nil {
		t.Fatalf("stored BuildRun must survive a rejected update: %v", err)
	}
	if v := after.GetAnnotations()[BuildRunTargetAnnotation]; v != `{"stream":"my-stream","tag":"latest"}` {
		t.Fatalf("a rejected update must leave the stored annotation untouched, got %q", v)
	}
}
