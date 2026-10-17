// Package webhook implements the ImageFlow admission webhook service: the
// single mutating + validating surface served inside the controller-manager
// process (ADR-001, spec issue #1 decision 6, ticket T3).
//
// Division of enforcement (frozen with the T2 catalog model):
//
//   - CRD-level CEL (ticket T2) guards the mirror (ImageStreamTag/ImageTag) and
//     ImageArtifact write boundary independently of webhook availability;
//   - this package owns everything CEL deliberately does not check: the
//     created-by identity annotation (ADR-003), importPolicy.intervalSeconds
//     defaulting (ADR-002 R8), the ImageStream name "--" ban (ADR-002 mirror
//     name encoding), the SourceRef "<stream>:<tag>" logical shape, the
//     containerNames/imageFields mutual exclusion, the imageFields grammar and
//     path duplicate/overlap rejection (ADR-005).
//
// The imageFields grammar parser lives in internal/imagefields as a pure logic
// library shared with the subscription controller admission re-validation (T8)
// and the runtime generic image field resolver (T10).
//
// T8 addition: the subscription validating handler also carries the
// admission-time half of the SubjectAccessReview authorization gate
// (subscription_authorize.go, ADR-003 timings "创建 admission" and "targets
// 变更"): the shape rules above are untouched, always run first and keep sole
// ownership of admission rejection; the authorization findings surface as
// admission warnings, while the authoritative Forbidden condition and the
// zero-write guarantee are enforced controller-side (internal/controller
// subscription-authz).
//
// The imageFields grammar is deliberately NOT claimed to be compatible with
// the OpenShift fieldPath annotation syntax (ADR-005 §1, ticket T3 acceptance
// criteria); the frozen grammar is documented in internal/imagefields.
package webhook

import (
	ctrl "sigs.k8s.io/controller-runtime"
	crwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// Webhook paths served by the single ImageFlow webhook server. They follow the
// kubebuilder path convention /<mutate|validate>-<group>-<version>-<plural> so
// the generated manifests (make manifests) and the runtime registration stay
// in sync.
const (
	// PathMutateImageSubscription serves created-by injection (ADR-003).
	PathMutateImageSubscription = "/mutate-imageflow-appfun-dev-v1alpha1-imagesubscription"
	// PathValidateImageSubscription serves the subscription shape rules.
	PathValidateImageSubscription = "/validate-imageflow-appfun-dev-v1alpha1-imagesubscription"
	// PathMutateImageStream serves importPolicy defaulting (ADR-002 R8).
	PathMutateImageStream = "/mutate-imageflow-appfun-dev-v1alpha1-imagestream"
	// PathValidateImageStream serves the stream name "--" ban (ADR-002).
	PathValidateImageStream = "/validate-imageflow-appfun-dev-v1alpha1-imagestream"
)

// Register installs all ImageFlow admission handlers on the manager's single
// webhook server (ADR-001: one webhook service per manager process). The
// manager only calls this when it is configured with webhook TLS certificates
// (see the webhook registrar in internal/controller); registering handlers on
// a server that cannot serve TLS would leave fail-closed webhook
// configurations pointing at a dead endpoint.
func Register(mgr ctrl.Manager) {
	scheme := mgr.GetScheme()
	server := mgr.GetWebhookServer()
	server.Register(PathMutateImageSubscription,
		&crwebhook.Admission{Handler: &subscriptionMutator{decoder: admission.NewDecoder(scheme)}})
	server.Register(PathValidateImageSubscription,
		admission.WithCustomValidator(scheme, &imageflowv1alpha1.ImageSubscription{},
			&subscriptionValidator{authorizer: newSubscriptionAuthorizer(mgr)}))
	server.Register(PathMutateImageStream,
		admission.WithCustomDefaulter(scheme, &imageflowv1alpha1.ImageStream{}, &imageStreamDefaulter{}))
	server.Register(PathValidateImageStream,
		admission.WithCustomValidator(scheme, &imageflowv1alpha1.ImageStream{}, &imageStreamValidator{}))
}
