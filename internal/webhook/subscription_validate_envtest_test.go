package webhook

import (
	"context"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// T3 (#24) envtest suite: the imageReferenceFormat-conditional desiredImage
// validation and the format downgrade protection verified end-to-end against
// a real API server — ValidatingWebhookConfiguration routing
// (vimagesubscription.kb.io, including the imagesubscriptions/status rule)
// → the validator registered through the production seam (Register) → the
// rejection message surfaced to the client. webhook_test.go covers the
// validator in isolation; this suite covers the wiring.
//
// The environment is shared with the BuildRun admission suite (started once
// in TestMain, only when KUBEBUILDER_ASSETS is set).

const (
	envtestDigestRef = "registry.example.com/payments/api@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	envtestTagRef = "registry.example.com/payments/api:stable"
	envtestCompositeRef = envtestTagRef + "@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

// envtestSubscription builds an admissible ImageSubscription in the shared
// envtest namespace with the given imageReferenceFormat.
func envtestValidateSubscription(name string, format imageflowv1alpha1.ImageReferenceFormat) *imageflowv1alpha1.ImageSubscription {
	return &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: envtestNS, Name: name},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      sourceKindImageStreamTag,
				Name:      "payments-api:stable",
				Namespace: "imageflow-system",
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{{
				TargetRef: imageflowv1alpha1.TargetReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Namespace:  envtestNS,
					Name:       "payments-api",
				},
				ContainerNames: []string{"payments-api"},
			}},
			ImageReferenceFormat: format,
		},
	}
}

// setDesiredImage re-reads the subscription and writes the given desiredImage
// through the status subresource — the path the delivery controller uses and
// the one the imagesubscriptions/status webhook rule guards.
func setDesiredImage(t *testing.T, name, desiredImage string) error {
	t.Helper()
	sub := &imageflowv1alpha1.ImageSubscription{}
	if err := envtestClient.Get(context.Background(),
		client.ObjectKey{Namespace: envtestNS, Name: name}, sub); err != nil {
		t.Fatalf("get subscription: %v", err)
	}
	sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{{
		TargetRef:    sub.Spec.Targets[0].TargetRef,
		DesiredImage: desiredImage,
	}}
	return envtestClient.Status().Update(context.Background(), sub)
}

// expectSubscriptionDenied pins the client-visible rejection shape: routed
// through the subscription webhook (vimagesubscription.kb.io), surfaced as
// Invalid (422), and carrying the contract keyword.
func expectSubscriptionDenied(t *testing.T, err error, wantKeyword string) {
	t.Helper()
	if err == nil {
		t.Fatal("request must be denied")
	}
	if !strings.Contains(err.Error(), "vimagesubscription.kb.io") {
		t.Fatalf("rejection must come from the subscription webhook (vimagesubscription.kb.io), got: %v", err)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("rejection must surface as Invalid (422), got: %v", err)
	}
	if !strings.Contains(err.Error(), wantKeyword) {
		t.Fatalf("rejection message must contain %q (it must name the failing constraint), got: %v",
			wantKeyword, err)
	}
}

func TestValidateSubscriptionFormatAdmission(t *testing.T) {
	requireBuildRunAdmissionEnv(t)
	ctx := context.Background()

	t.Run("create with each format is admitted", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			format imageflowv1alpha1.ImageReferenceFormat
		}{
			{"sub-fmt-digest", imageflowv1alpha1.ImageReferenceFormatDigest},
			{"sub-fmt-tag", imageflowv1alpha1.ImageReferenceFormatTag},
			{"sub-fmt-composite", imageflowv1alpha1.ImageReferenceFormatDigestWithTag},
			{"sub-fmt-unset", ""},
		} {
			sub := envtestValidateSubscription(tc.name, tc.format)
			if err := envtestClient.Create(ctx, sub); err != nil {
				t.Fatalf("create with format %q must be admitted: %v", tc.format, err)
			}
		}
	})

	t.Run("status desiredImage matching the selected format is admitted", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			desiredImage string
		}{
			{"sub-fmt-digest", envtestDigestRef},
			{"sub-fmt-tag", envtestTagRef},
			{"sub-fmt-composite", envtestCompositeRef},
		} {
			if err := setDesiredImage(t, tc.name, tc.desiredImage); err != nil {
				t.Fatalf("status update with matching desiredImage must be admitted: %v", err)
			}
		}
	})

	t.Run("status desiredImage violating the Digest grammar is denied", func(t *testing.T) {
		expectSubscriptionDenied(t,
			setDesiredImage(t, "sub-fmt-digest", envtestTagRef), "imageReferenceFormat=Digest")
	})

	t.Run("status desiredImage violating the Tag grammar is denied", func(t *testing.T) {
		expectSubscriptionDenied(t,
			setDesiredImage(t, "sub-fmt-tag", envtestDigestRef), "imageReferenceFormat=Tag")
	})

	t.Run("status desiredImage violating the DigestWithTag grammar is denied", func(t *testing.T) {
		expectSubscriptionDenied(t,
			setDesiredImage(t, "sub-fmt-composite", envtestTagRef), "imageReferenceFormat=DigestWithTag")
	})

	t.Run("unset format validates desiredImage as Digest", func(t *testing.T) {
		expectSubscriptionDenied(t,
			setDesiredImage(t, "sub-fmt-unset", envtestTagRef), "imageReferenceFormat=Digest")
		if err := setDesiredImage(t, "sub-fmt-unset", envtestDigestRef); err != nil {
			t.Fatalf("digest reference under unset format must be admitted: %v", err)
		}
	})

	t.Run("format downgrade from Tag to Digest is denied", func(t *testing.T) {
		sub := &imageflowv1alpha1.ImageSubscription{}
		if err := envtestClient.Get(ctx, client.ObjectKey{Namespace: envtestNS, Name: "sub-fmt-tag"}, sub); err != nil {
			t.Fatalf("get subscription: %v", err)
		}
		sub.Spec.ImageReferenceFormat = imageflowv1alpha1.ImageReferenceFormatDigest
		expectSubscriptionDenied(t, envtestClient.Update(ctx, sub), "downgrade")
	})

	t.Run("format upgrade from Digest to Tag is admitted", func(t *testing.T) {
		sub := &imageflowv1alpha1.ImageSubscription{}
		if err := envtestClient.Get(ctx, client.ObjectKey{Namespace: envtestNS, Name: "sub-fmt-digest"}, sub); err != nil {
			t.Fatalf("get subscription: %v", err)
		}
		sub.Spec.ImageReferenceFormat = imageflowv1alpha1.ImageReferenceFormatTag
		if err := envtestClient.Update(ctx, sub); err != nil {
			t.Fatalf("format upgrade must be admitted: %v", err)
		}
	})

	t.Run("status update after the upgrade is validated against the new format", func(t *testing.T) {
		expectSubscriptionDenied(t,
			setDesiredImage(t, "sub-fmt-digest", envtestDigestRef), "imageReferenceFormat=Tag")
	})
}
