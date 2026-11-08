package webhook

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/imagefields"
)

// sourceKindImageStreamTag is the only subscription source kind of v1
// (ADR-002 R3, spec decision: v1 仅 kind=ImageStreamTag).
const sourceKindImageStreamTag = "ImageStreamTag"

// streamNameSeparator is the mirror-name separator banned inside ImageStream
// names so the "<stream>--<tag>" / "<stream>--sha256-<hex>" encoding stays
// injective when split on its first occurrence (ADR-002, ticket T3).
const streamNameSeparator = "--"

// ociTagPattern is the tag grammar frozen on TagReference.Name by the T2 CRD
// shape; the SourceRef "<stream>:<tag>" tag part must match it so the logical
// form always maps to a declarable stream tag.
var ociTagPattern = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`)

// desiredImagePatterns maps each imageReferenceFormat to the reference grammar
// the delivery controller is contractually bound to write (ADR-012 §6, spec
// decision 3). The kubebuilder pattern on DesiredImage was removed in T1
// because one regex cannot express all three formats; this webhook enforces
// the format selected by spec.imageReferenceFormat instead. Because
// DesiredImage lives in the status subresource, the webhook rules below also
// match imagesubscriptions/status so the check fires where the value is
// actually written.
var desiredImagePatterns = map[imageflowv1alpha1.ImageReferenceFormat]*regexp.Regexp{
	imageflowv1alpha1.ImageReferenceFormatDigest:       regexp.MustCompile(`^[^@\s]+@sha256:[0-9a-f]{64}$`),
	imageflowv1alpha1.ImageReferenceFormatTag:          regexp.MustCompile(`^[^@\s]+:[^@\s]+$`),
	imageflowv1alpha1.ImageReferenceFormatDigestWithTag: regexp.MustCompile(`^[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}$`),
}

// effectiveImageReferenceFormat resolves the format used for validation: an
// empty value means Digest (mutating webhook defaulting, ADR-012 §3), so
// subscriptions predating v1.2 keep validating against the v1 digest-only
// shape.
func effectiveImageReferenceFormat(format imageflowv1alpha1.ImageReferenceFormat) imageflowv1alpha1.ImageReferenceFormat {
	if format == "" {
		return imageflowv1alpha1.ImageReferenceFormatDigest
	}
	return format
}

// builtinWorkloadKinds is the first-batch built-in adapter set (ADR-006).
// Targets referencing one of these apps/v1 Kinds must locate images by
// containerNames — required, non-empty, no default guessing (spec decision 6).
var builtinWorkloadKinds = map[string]bool{
	"Deployment":  true,
	"StatefulSet": true,
	"DaemonSet":   true,
}

// subscriptionGK identifies the ImageSubscription kind in Invalid errors.
var subscriptionGK = schema.GroupKind{
	Group: imageflowv1alpha1.GroupVersion.Group,
	Kind:  "ImageSubscription",
}

// +kubebuilder:webhook:path=/validate-imageflow-appfun-dev-v1alpha1-imagesubscription,mutating=false,failurePolicy=fail,sideEffects=None,groups=imageflow.appfun.dev,resources=imagesubscriptions;imagesubscriptions/status,verbs=create;update,versions=v1alpha1,name=vimagesubscription.kb.io,admissionReviewVersions=v1,timeoutSeconds=10

// subscriptionValidator enforces the subscription shape rules that spec
// decision 6 assigns to the validating webhook — deliberately NOT to CRD-level
// CEL (T2 handoff): the SourceRef logical form, the containerNames/imageFields
// mutual exclusion, the imageFields grammar and path duplicate/overlap
// rejection.
//
// T8 addition (additive, shape semantics unchanged): when an authorizer is
// configured, the admission-time SubjectAccessReview gate (ADR-003) runs
// AFTER the shape validation passed — create admission checks every target,
// update admission re-checks whenever spec.targets changed. A zero-value
// validator (no authorizer) behaves exactly like the original T3 shape-only
// validator.
type subscriptionValidator struct {
	authorizer *subscriptionAuthorizer
}

var _ admission.CustomValidator = &subscriptionValidator{}

// ValidateCreate implements admission.CustomValidator: shape first (T3),
// then the T8 admission-time authorization of the created-by identity against
// every target.
func (v *subscriptionValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	sub := obj.(*imageflowv1alpha1.ImageSubscription)
	if err := validateSubscriptionShape(sub); err != nil {
		return nil, err
	}
	if v.authorizer == nil {
		return nil, nil
	}
	return v.authorizer.authorizeCreate(ctx, sub)
}

// ValidateUpdate implements admission.CustomValidator. The full shape is
// re-validated on every update; the old object adds no exempt state. Format
// downgrade protection (T3, #24) rejects switching imageReferenceFormat from
// Tag/DigestWithTag back to Digest. The T8 authorization re-check runs only
// when spec.targets changed (ADR-003 "targets 变更" timing).
func (v *subscriptionValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	newSub := newObj.(*imageflowv1alpha1.ImageSubscription)
	if err := validateSubscriptionShape(newSub); err != nil {
		return nil, err
	}
	oldSub := oldObj.(*imageflowv1alpha1.ImageSubscription)
	if err := validateFormatTransition(oldSub, newSub); err != nil {
		return nil, err
	}
	if v.authorizer == nil {
		return nil, nil
	}
	return v.authorizer.authorizeUpdate(ctx, oldSub, newSub)
}

// ValidateDelete implements admission.CustomValidator; deletes always pass.
func (v *subscriptionValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// validateSubscriptionShape aggregates every shape violation of one
// subscription into a single Invalid error, so a misconfiguration is rejected
// as a whole with a complete report instead of one error at a time.
func validateSubscriptionShape(sub *imageflowv1alpha1.ImageSubscription) error {
	var errs field.ErrorList
	errs = append(errs, validateSourceRef(field.NewPath("spec", "source"), sub.Spec.Source)...)
	targetsPath := field.NewPath("spec", "targets")
	for i := range sub.Spec.Targets {
		errs = append(errs, validateTarget(targetsPath.Index(i), &sub.Spec.Targets[i])...)
	}
	errs = append(errs, validateImageReferenceFormat(field.NewPath("spec", "imageReferenceFormat"),
		sub.Spec.ImageReferenceFormat)...)
	errs = append(errs, validateDesiredImages(field.NewPath("status", "targets"),
		sub.Status.Targets, sub.Spec.ImageReferenceFormat)...)
	if len(errs) > 0 {
		return apierrors.NewInvalid(subscriptionGK, sub.Name, errs)
	}
	return nil
}

// validateImageReferenceFormat is the secondary enum defense required by spec
// decision 5: the kubebuilder enum on the field is the primary check, but the
// webhook re-verifies because it also guards the status subresource and
// objects written before the enum existed. Empty stays valid here — it means
// "not yet defaulted" and validates as Digest everywhere else.
func validateImageReferenceFormat(p *field.Path, format imageflowv1alpha1.ImageReferenceFormat) field.ErrorList {
	if format == "" {
		return nil
	}
	if _, ok := desiredImagePatterns[format]; !ok {
		return field.ErrorList{field.NotSupported(p, format, []string{
			string(imageflowv1alpha1.ImageReferenceFormatDigest),
			string(imageflowv1alpha1.ImageReferenceFormatTag),
			string(imageflowv1alpha1.ImageReferenceFormatDigestWithTag),
		})}
	}
	return nil
}

// validateDesiredImages checks every reported desiredImage against the
// reference grammar of the selected imageReferenceFormat (ADR-012 §6). An
// empty desiredImage means "nothing delivered yet" and always passes. An
// unknown format was already reported by validateImageReferenceFormat, so no
// pattern check runs on top of it.
func validateDesiredImages(p *field.Path, targets []imageflowv1alpha1.TargetDeliveryStatus,
	format imageflowv1alpha1.ImageReferenceFormat) field.ErrorList {
	format = effectiveImageReferenceFormat(format)
	pattern, ok := desiredImagePatterns[format]
	if !ok {
		return nil
	}
	var errs field.ErrorList
	for i := range targets {
		desired := targets[i].DesiredImage
		if desired == "" {
			continue
		}
		if !pattern.MatchString(desired) {
			errs = append(errs, field.Invalid(p.Index(i).Child("desiredImage"), desired,
				fmt.Sprintf("must match %s for imageReferenceFormat=%s: %s",
					pattern.String(), format, "the reference grammar of the selected format (ADR-012 §6)")))
		}
	}
	return errs
}

// validateFormatTransition implements the format downgrade protection (T3,
// #24): once a subscription writes Tag or DigestWithTag references, switching
// back to Digest would silently rewrite every target image field to a
// different reference shape, so the downgrade is rejected. Upgrades and the
// lateral Tag → DigestWithTag move stay allowed. Status-subresource updates
// carry an unchanged spec, so they never trip this check.
func validateFormatTransition(oldSub, newSub *imageflowv1alpha1.ImageSubscription) error {
	oldFormat := effectiveImageReferenceFormat(oldSub.Spec.ImageReferenceFormat)
	newFormat := effectiveImageReferenceFormat(newSub.Spec.ImageReferenceFormat)
	if newFormat != imageflowv1alpha1.ImageReferenceFormatDigest ||
		oldFormat == imageflowv1alpha1.ImageReferenceFormatDigest {
		return nil
	}
	return apierrors.NewInvalid(subscriptionGK, newSub.Name, field.ErrorList{
		field.Invalid(field.NewPath("spec", "imageReferenceFormat"), newSub.Spec.ImageReferenceFormat,
			fmt.Sprintf("format downgrade from %s to Digest is forbidden: it would silently rewrite"+
				" every target image field back to the digest-only shape (T3, #24); recreate the"+
				" subscription if a downgrade is really intended", oldFormat)),
	})
}

// validateSourceRef checks the logical "<stream>:<tag>" form (ADR-002): users
// never write encoded mirror object names, and the referenced stream tag must
// be declarable on an ImageStream (stream name grammar without "--", OCI tag
// grammar).
func validateSourceRef(p *field.Path, src imageflowv1alpha1.SubscriptionSourceRef) field.ErrorList {
	var errs field.ErrorList
	if src.Kind != sourceKindImageStreamTag {
		errs = append(errs, field.NotSupported(p.Child("kind"), src.Kind, []string{sourceKindImageStreamTag}))
	}
	if strings.Count(src.Name, ":") != 1 {
		return append(errs, field.Invalid(p.Child("name"), src.Name,
			`must be the logical "<stream>:<tag>" form with exactly one ":"`+
				` (users never write encoded mirror object names, ADR-002)`))
	}
	stream, tag, _ := strings.Cut(src.Name, ":")
	if strings.Contains(stream, streamNameSeparator) {
		errs = append(errs, field.Invalid(p.Child("name"), src.Name,
			`stream part must not contain "--": mirror names are split on the first "--" and that split is only injective when stream names never contain it (ADR-002)`))
	}
	for _, msg := range validation.IsDNS1123Subdomain(stream) {
		errs = append(errs, field.Invalid(p.Child("name"), src.Name,
			"stream part is not a valid ImageStream name (RFC 1123 subdomain): "+msg))
	}
	if !ociTagPattern.MatchString(tag) {
		errs = append(errs, field.Invalid(p.Child("name"), src.Name,
			"tag part must match the OCI tag grammar "+ociTagPattern.String()))
	}
	return errs
}

// validateTarget enforces the per-target locator contract: exactly one of
// containerNames / imageFields (mutual exclusion, no default guessing), the
// built-in Kind binding to containerNames (ADR-006 + spec decision 6), and the
// full grammar/duplicate/overlap validation of imageFields (ADR-005).
func validateTarget(p *field.Path, t *imageflowv1alpha1.SubscriptionTarget) field.ErrorList {
	var errs field.ErrorList
	hasContainers := len(t.ContainerNames) > 0
	hasFields := len(t.ImageFields) > 0

	switch {
	case hasContainers && hasFields:
		errs = append(errs, field.Forbidden(p,
			"containerNames and imageFields are mutually exclusive: built-in Kinds locate images by"+
				" container name, third-party Kinds by restricted imageFields paths (ADR-005/ADR-006);"+
				" the whole configuration is rejected"))
	case !hasContainers && !hasFields:
		errs = append(errs, field.Required(p,
			"exactly one locator is required: containerNames (built-in Kinds) or imageFields"+
				" (third-party Kinds); ImageFlow never guesses a default (spec decision 6)"))
	}

	if isBuiltinWorkload(t.TargetRef) && hasFields {
		errs = append(errs, field.Forbidden(p.Child("imageFields"),
			fmt.Sprintf("built-in Kind %s must locate images by containerNames (ADR-006);"+
				" imageFields is the locator for third-party Kinds", t.TargetRef.Kind)))
	}

	errs = append(errs, validateContainerNames(p.Child("containerNames"), t.ContainerNames)...)
	errs = append(errs, validateImageFields(p.Child("imageFields"), t.ImageFields)...)
	return errs
}

// isBuiltinWorkload reports whether the target references the first-batch
// built-in adapter set (apps/v1 Deployment/StatefulSet/DaemonSet, ADR-006).
// Group and version must both match: a third-party CRD may legitimately reuse
// a Kind name and then follows the generic imageFields path.
func isBuiltinWorkload(ref imageflowv1alpha1.TargetReference) bool {
	group, version, found := strings.Cut(ref.APIVersion, "/")
	if !found || group != "apps" || version != "v1" {
		return false
	}
	return builtinWorkloadKinds[ref.Kind]
}

// validateContainerNames checks that entries are real Kubernetes container
// names (DNS-1123 labels) and unique — a target must never declare the same
// container twice.
func validateContainerNames(p *field.Path, names []string) field.ErrorList {
	var errs field.ErrorList
	seen := make(map[string]struct{}, len(names))
	for i, name := range names {
		if msgs := validation.IsDNS1123Label(name); len(msgs) > 0 {
			errs = append(errs, field.Invalid(p.Index(i), name,
				"container names are Kubernetes container names (DNS-1123 label): "+msgs[0]))
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			errs = append(errs, field.Duplicate(p.Index(i), name))
			continue
		}
		seen[name] = struct{}{}
	}
	return errs
}

// validateImageFields runs the frozen ADR-005 grammar and the set-level
// duplicate/overlap detection of internal/imagefields, mapping violations to
// field-precise errors.
func validateImageFields(p *field.Path, paths []string) field.ErrorList {
	var errs field.ErrorList
	_, violations := imagefields.ValidateSet(paths)
	for _, v := range violations {
		switch v.Kind {
		case imagefields.ViolationGrammar:
			errs = append(errs, field.Invalid(p.Index(v.Index), v.Path, v.Reason))
		case imagefields.ViolationDuplicate:
			errs = append(errs, field.Invalid(p.Index(v.Other), v.OtherPath,
				fmt.Sprintf("duplicates imageFields[%d]: the same image field must not be declared twice in one target", v.Index)))
		case imagefields.ViolationOverlap:
			errs = append(errs, field.Invalid(p.Index(v.Other), v.OtherPath,
				fmt.Sprintf("overlaps imageFields[%d] (%q): one path addresses a prefix of the other,"+
					" so both writes would touch the same field; the whole configuration is rejected (ADR-005)", v.Index, v.Path)))
		}
	}
	return errs
}
