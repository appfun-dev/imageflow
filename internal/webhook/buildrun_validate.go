package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// +kubebuilder:webhook:path=/validate-shipwright-io-v1beta1-buildrun,mutating=false,failurePolicy=fail,sideEffects=None,groups=shipwright.io,resources=buildruns,verbs=create;update,versions=v1beta1,name=vbuildrun.kb.io,admissionReviewVersions=v1,timeoutSeconds=10

// BuildRunTargetAnnotation locks a Build/BuildRun to its ImageFlow
// registration target (ADR-011 §2). The value is a JSON object
// {"stream":"<rfc1123-name>","tag":"<rfc1123-name>"}; this webhook rejects
// malformed values at write time instead of letting the watcher silently
// ignore them at reconcile time.
const BuildRunTargetAnnotation = "buildrun.imageflow.appfun.dev/target"

// buildRunGK identifies the Shipwright BuildRun kind in Invalid errors.
// Shipwright types are deliberately not imported into this module: ImageFlow
// only reads BuildRun metadata, so the handler decodes into unstructured.
var buildRunGK = schema.GroupKind{Group: "shipwright.io", Kind: "BuildRun"}

// buildRunTarget is the frozen annotation schema (ADR-011 §2). Unknown fields
// are tolerated so future contract extensions stay forward-compatible.
type buildRunTarget struct {
	Stream string `json:"stream"`
	Tag    string `json:"tag"`
}

// buildRunValidator enforces the BuildRunTargetAnnotation contract on
// BuildRun create/update. It is a raw admission.Handler decoding into
// unstructured because the Shipwright scheme is not registered in this
// manager — only metadata.annotations is consulted.
type buildRunValidator struct {
	decoder admission.Decoder
}

var _ admission.Handler = &buildRunValidator{}

// Handle implements admission.Handler.
func (v *buildRunValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	obj := &unstructured.Unstructured{}
	if err := v.decoder.Decode(req, obj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := validateBuildRunTarget(obj.GetName(), obj.GetAnnotations()); err != nil {
		return invalidResponse(err)
	}
	return admission.Allowed("")
}

// validateBuildRunTarget checks the target annotation when present: valid
// JSON object, required stream/tag fields, both RFC 1123 subdomains (the same
// grammar ImageStream object names already follow). A BuildRun without the
// annotation is untouched — the watcher only consumes annotated objects.
func validateBuildRunTarget(name string, annotations map[string]string) error {
	value, ok := annotations[BuildRunTargetAnnotation]
	if !ok {
		return nil
	}
	p := field.NewPath("metadata", "annotations").Key(BuildRunTargetAnnotation)

	var target buildRunTarget
	if err := json.Unmarshal([]byte(value), &target); err != nil {
		// The message must name the failing constraint (ADR-011 §2): a syntax
		// error means the value is not JSON at all, a type error means it is
		// JSON but not the required object.
		detail := "must be JSON object of form {\"stream\":\"<name>\",\"tag\":\"<name>\"} (ADR-011)"
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			detail = "invalid JSON: " + detail
		}
		return apierrors.NewInvalid(buildRunGK, name, field.ErrorList{
			field.Invalid(p, value, fmt.Sprintf("%s: %v", detail, err)),
		})
	}

	var errs field.ErrorList
	if target.Stream == "" {
		errs = append(errs, field.Required(p.Child("stream"),
			`the target annotation requires both "stream" and "tag" (ADR-011)`))
	} else {
		for _, msg := range validation.IsDNS1123Subdomain(target.Stream) {
			errs = append(errs, field.Invalid(p.Child("stream"), target.Stream,
				"stream is not a valid ImageStream name (RFC 1123 subdomain): "+msg))
		}
	}
	if target.Tag == "" {
		errs = append(errs, field.Required(p.Child("tag"),
			`the target annotation requires both "stream" and "tag" (ADR-011)`))
	} else {
		for _, msg := range validation.IsDNS1123Subdomain(target.Tag) {
			errs = append(errs, field.Invalid(p.Child("tag"), target.Tag,
				"tag is not a valid name (RFC 1123 subdomain): "+msg))
		}
	}
	if len(errs) > 0 {
		return apierrors.NewInvalid(buildRunGK, name, errs)
	}
	return nil
}

// invalidResponse surfaces the rejection with the same 422 Invalid status the
// ImageFlow CustomValidator handlers produce, keeping admission failure
// semantics uniform across the webhook service.
func invalidResponse(err error) admission.Response {
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.Response{AdmissionResponse: admissionv1.AdmissionResponse{
		Allowed: false,
		Result:  &statusErr.ErrStatus,
	}}
}
