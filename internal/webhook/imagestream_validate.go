package webhook

import (
	"context"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// imageStreamGK identifies the ImageStream kind in Invalid errors.
var imageStreamGK = schema.GroupKind{
	Group: imageflowv1alpha1.GroupVersion.Group,
	Kind:  "ImageStream",
}

// +kubebuilder:webhook:path=/validate-imageflow-appfun-dev-v1alpha1-imagestream,mutating=false,failurePolicy=fail,sideEffects=None,groups=imageflow.appfun.dev,resources=imagestreams,verbs=create;update,versions=v1alpha1,name=vimagestream.kb.io,admissionReviewVersions=v1,timeoutSeconds=10

// imageStreamValidator enforces the stream naming rule that admission must
// guarantee for the mirror name encoding (ADR-002, ticket T2 handoff item 1 —
// deliberately NOT a CEL rule so this webhook-owned case stays testable):
// an ImageStream name must not contain "--", because mirror objects are named
// "<stream>--<tag>" / "<stream>--sha256-<hex>" and split back on the FIRST
// "--" — a split that is only injective when stream names never contain it.
type imageStreamValidator struct{}

var _ admission.CustomValidator = &imageStreamValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *imageStreamValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, validateImageStreamShape(obj.(*imageflowv1alpha1.ImageStream))
}

// ValidateUpdate implements admission.CustomValidator. metadata.name is
// immutable in Kubernetes, so the update path re-checks defensively and keeps
// the rule enforced no matter how the object is rewritten.
func (v *imageStreamValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	return nil, validateImageStreamShape(newObj.(*imageflowv1alpha1.ImageStream))
}

// ValidateDelete implements admission.CustomValidator; deletes always pass.
func (v *imageStreamValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// validateImageStreamShape rejects stream names containing the mirror-name
// separator "--".
func validateImageStreamShape(stream *imageflowv1alpha1.ImageStream) error {
	if !strings.Contains(stream.Name, streamNameSeparator) {
		return nil
	}
	return apierrors.NewInvalid(imageStreamGK, stream.Name, field.ErrorList{
		field.Invalid(field.NewPath("metadata", "name"), stream.Name,
			`must not contain "--": mirror objects are named <stream>--<tag> / <stream>--sha256-<hex>`+
				` and split back on the first "--", which is only injective when stream names never contain it (ADR-002)`),
	})
}
