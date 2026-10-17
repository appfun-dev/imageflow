package controller

import (
	"flag"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/appfun-dev/imageflow/internal/webhook"
)

// webhookCertPathFlag is the process flag (bound in cmd/main.go) that points
// at the directory holding the webhook TLS serving certificate. It is the
// single source of truth for whether this process serves admission webhooks.
const webhookCertPathFlag = "webhook-cert-path"

func init() {
	Registrars = append(Registrars, webhookRegistrar{})
}

// webhookRegistrar installs the admission webhook handlers on the manager's
// single webhook server (ADR-001: one controller-manager process, one webhook
// service). It uses the same independent registration seam as the controllers
// — a new file appending a Registrar via init() — so the webhook surface stays
// a self-contained unit.
//
// Enablement gate: handlers are registered only when the process was
// configured with --webhook-cert-path (non-empty). Rationale:
//
//   - registering a handler touches mgr.GetWebhookServer(), which adds the
//     server to the manager runnables; a webhook server without TLS
//     certificates fails to start (certwatcher rejects the missing files) and
//     would take the whole manager down — so a certificate-less process
//     (e.g. `make run` for local controller development) must never register;
//   - the deployment's webhook configurations are fail-closed
//     (failurePolicy=Fail); they must never point at a server that cannot
//     serve, so admission and certificates are enabled together or not at all.
//
// The gate reads the flag surface of cmd/main.go instead of manager options so
// this ticket needs no edits to shared files (internal/manager/run.go,
// internal/controller/setup.go). Embedders that construct manager.Run
// programmatically with certificates but without the standard flag set call
// webhook.Register(mgr) themselves — the integration suite in
// test/integration/webhook_test.go exercises exactly that seam.
//
// Without admission registered, CRD-level CEL (ticket T2) still guards the
// mirror and ImageArtifact write boundary (spec decision 6: that protection
// must not depend on webhook availability).
type webhookRegistrar struct{}

func (webhookRegistrar) Register(mgr ctrl.Manager, _ Options) error {
	if !webhookCertificatesConfigured() {
		ctrl.Log.WithName("setup").Info(
			"skipping admission webhook registration: --webhook-cert-path is not set",
		)
		return nil
	}
	webhook.Register(mgr)
	return nil
}

// webhookCertificatesConfigured reports whether the process flag surface
// carries a non-empty --webhook-cert-path. A missing flag (programmatic
// embedders that never bound it) counts as not configured.
func webhookCertificatesConfigured() bool {
	f := flag.Lookup(webhookCertPathFlag)
	return f != nil && f.Value.String() != ""
}
