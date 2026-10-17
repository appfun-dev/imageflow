package controller

import (
	"flag"
	"testing"

	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// TestWebhookRegistrarSkipsWithoutCertificates pins the enablement gate: a
// process that was not configured with --webhook-cert-path must never touch
// the manager's webhook server, because registering a handler starts the
// server, and a certificate-less webhook server fails to start (taking the
// whole manager down).
//
// The fakeManager embeds a nil ctrl.Manager: any attempt to use it (e.g.
// GetWebhookServer) would panic, which is exactly the behavior this gate must
// prevent. The enabled path is exercised end-to-end by
// test/integration/webhook_test.go, which calls webhook.Register against a
// real manager with envtest-issued certificates.
func TestWebhookRegistrarSkipsWithoutCertificates(t *testing.T) {
	registrar := webhookRegistrar{}
	opts := Options{Clock: clock.Real{}, Config: config.Config{}}

	t.Run("flag not bound at all", func(t *testing.T) {
		if flag.Lookup(webhookCertPathFlag) != nil {
			t.Skip("flag already bound in this process; covered by the empty-value case")
		}
		if err := registrar.Register(&fakeManager{}, opts); err != nil {
			t.Fatalf("Register() = %v, want nil", err)
		}
	})

	t.Run("flag bound but empty", func(t *testing.T) {
		fs := flag.CommandLine
		if fs.Lookup(webhookCertPathFlag) == nil {
			fs.String(webhookCertPathFlag, "", "test binding")
		}
		if err := registrar.Register(&fakeManager{}, opts); err != nil {
			t.Fatalf("Register() = %v, want nil", err)
		}
	})
}

func TestWebhookCertificatesConfigured(t *testing.T) {
	fs := flag.CommandLine
	if fs.Lookup(webhookCertPathFlag) == nil {
		fs.String(webhookCertPathFlag, "", "test binding")
	}
	f := fs.Lookup(webhookCertPathFlag)

	if err := f.Value.Set(""); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if webhookCertificatesConfigured() {
		t.Fatal("empty cert path must count as not configured")
	}
	if err := f.Value.Set("/tmp/k8s-webhook-server/serving-certs"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !webhookCertificatesConfigured() {
		t.Fatal("non-empty cert path must count as configured")
	}
	// Restore the neutral state for any later test in this package.
	if err := f.Value.Set(""); err != nil {
		t.Fatalf("Set: %v", err)
	}
}
