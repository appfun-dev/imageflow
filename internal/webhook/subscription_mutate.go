package webhook

import (
	"context"
	"encoding/json"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// +kubebuilder:webhook:path=/mutate-imageflow-appfun-dev-v1alpha1-imagesubscription,mutating=true,failurePolicy=fail,sideEffects=None,groups=imageflow.appfun.dev,resources=imagesubscriptions,verbs=create;update,versions=v1alpha1,name=mimagesubscription.kb.io,admissionReviewVersions=v1,timeoutSeconds=10

// subscriptionMutator injects the imageflow.appfun.dev/created-by annotation
// with the authenticated requester identity (ADR-003). It is a raw
// admission.Handler — not an admission.CustomDefaulter — because defaulters do
// not see the AdmissionRequest and therefore cannot access req.UserInfo.
//
// Identity semantics:
//   - CREATE: the authenticated requester always wins; a user-supplied
//     created-by value is an untrusted identity claim and gets overwritten.
//     Without an authenticated username nothing is injected and any claimed
//     annotation is stripped.
//   - UPDATE: the annotation names the CREATOR and is immutable for the
//     lifetime of the object — every update is forced back to the stored
//     value, even one that deletes it. Objects that predate the webhook (no
//     stored annotation) get the updater's identity injected once, so the T8
//     authorization gate always has a subject.
type subscriptionMutator struct {
	decoder admission.Decoder
}

var _ admission.Handler = &subscriptionMutator{}

// Handle implements admission.Handler.
func (m *subscriptionMutator) Handle(_ context.Context, req admission.Request) admission.Response {
	sub := &imageflowv1alpha1.ImageSubscription{}
	if err := m.decoder.Decode(req, sub); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	switch req.Operation {
	case admissionv1.Create:
		enforceCreatedBy(sub, createdByFromRequest(req), "")
	case admissionv1.Update:
		old := &imageflowv1alpha1.ImageSubscription{}
		if err := m.decoder.DecodeRaw(req.OldObject, old); err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
		enforceCreatedBy(sub, createdByFromRequest(req), old.GetAnnotations()[CreatedByAnnotation])
	default:
		// The webhook is only configured for create/update; anything else
		// passes through untouched.
		return admission.Allowed("")
	}

	mutated, err := json.Marshal(sub)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, mutated)
}

// enforceCreatedBy writes the trusted created-by value onto the object:
// kept (the stored creator) wins over the current requester, which in turn
// wins over anything the writer claimed. An empty result strips the
// annotation so an untrusted claim never survives admission.
func enforceCreatedBy(sub *imageflowv1alpha1.ImageSubscription, requester, kept string) {
	value := kept
	if value == "" {
		value = requester
	}
	annotations := sub.GetAnnotations()
	if value == "" {
		delete(annotations, CreatedByAnnotation)
		sub.SetAnnotations(annotations)
		return
	}
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[CreatedByAnnotation] = value
	sub.SetAnnotations(annotations)
}

// createdByFromRequest encodes the authenticated admission identity.
func createdByFromRequest(req admission.Request) string {
	return CreatedByValue(req.UserInfo)
}
