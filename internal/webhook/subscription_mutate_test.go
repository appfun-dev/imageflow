package webhook

import (
	"context"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// T2 (issue #23, ADR-012 decision 3) unit suite: the mutating webhook defaults
// spec.imageReferenceFormat to Digest when omitted, on create AND on update,
// and never overwrites an explicit value. Patch-level assertions follow the
// createdByAfterPatches pattern in webhook_test.go.

const imageReferenceFormatPatchPath = "/spec/imageReferenceFormat"

// formatAfterPatches extracts the imageReferenceFormat state the response
// patches would produce: (value, true) when a patch touches the field,
// ("", false) when the response leaves it untouched.
func formatAfterPatches(t *testing.T, resp admission.Response) (string, bool) {
	t.Helper()
	if !resp.Allowed {
		t.Fatalf("response not allowed: %+v", resp.Result)
	}
	for _, op := range resp.Patches {
		if op.Path != imageReferenceFormatPatchPath {
			continue
		}
		value, ok := op.Value.(string)
		if !ok {
			t.Fatalf("format patch value is not a string: %+v", op.Value)
		}
		return value, true
	}
	return "", false
}

func TestMutateImageReferenceFormatDefaultsOnCreate(t *testing.T) {
	sub := testSubscription() // no imageReferenceFormat set
	req := admissionRequest(admissionv1.Create, sub, testUserAlice)

	resp := newMutator(t).Handle(context.Background(), req)
	got, present := formatAfterPatches(t, resp)
	if !present || got != string(imageflowv1alpha1.ImageReferenceFormatDigest) {
		t.Fatalf("format after create patches = (%q, %v), want (%q, true); patches %+v",
			got, present, imageflowv1alpha1.ImageReferenceFormatDigest, resp.Patches)
	}
}

func TestMutateImageReferenceFormatDefaultsOnUpdate(t *testing.T) {
	// A subscription predating v1.2 (stored without the field) gets defaulted
	// on its next update — the zero-manual-migration promise of ADR-012 §3.
	stored := testSubscription()
	updated := stored.DeepCopy()
	req := admissionRequest(admissionv1.Update, updated, testUserAlice)
	req.OldObject = runtime.RawExtension{Raw: mustMarshal(t, stored)}

	resp := newMutator(t).Handle(context.Background(), req)
	got, present := formatAfterPatches(t, resp)
	if !present || got != string(imageflowv1alpha1.ImageReferenceFormatDigest) {
		t.Fatalf("format after update patches = (%q, %v), want (%q, true); patches %+v",
			got, present, imageflowv1alpha1.ImageReferenceFormatDigest, resp.Patches)
	}
}

func TestMutateImageReferenceFormatKeepsExplicitValues(t *testing.T) {
	for _, format := range []imageflowv1alpha1.ImageReferenceFormat{
		imageflowv1alpha1.ImageReferenceFormatTag,
		imageflowv1alpha1.ImageReferenceFormatDigestWithTag,
	} {
		t.Run("create keeps "+string(format), func(t *testing.T) {
			sub := testSubscription()
			sub.Spec.ImageReferenceFormat = format
			req := admissionRequest(admissionv1.Create, sub, testUserAlice)

			resp := newMutator(t).Handle(context.Background(), req)
			if got, touched := formatAfterPatches(t, resp); touched {
				t.Fatalf("explicit format %q must not be patched, got patch to %q", format, got)
			}
		})

		t.Run("update keeps "+string(format), func(t *testing.T) {
			stored := testSubscription()
			stored.Spec.ImageReferenceFormat = format
			updated := stored.DeepCopy()
			req := admissionRequest(admissionv1.Update, updated, testUserAlice)
			req.OldObject = runtime.RawExtension{Raw: mustMarshal(t, stored)}

			resp := newMutator(t).Handle(context.Background(), req)
			if got, touched := formatAfterPatches(t, resp); touched {
				t.Fatalf("explicit format %q must not be patched, got patch to %q", format, got)
			}
		})
	}
}
