package webhook

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// T2 (issue #23, ADR-012 decision 3) envtest suite: imageReferenceFormat
// defaulting verified end-to-end against a real API server — the generated
// MutatingWebhookConfiguration (mimagesubscription.kb.io) routes create and
// update through the production Register seam, and the stored object carries
// the defaulted value. Runs against the shared package TestMain environment
// and skips without KUBEBUILDER_ASSETS like the BuildRun suite.

// envtestSubscription builds a valid subscription in the shared envtest
// namespace. The shape must pass the validating webhook so the mutating
// outcome is what persists.
func envtestSubscription(name string) *imageflowv1alpha1.ImageSubscription {
	sub := testSubscription()
	sub.Namespace = envtestNS
	sub.Name = name
	return sub
}

func getSubscriptionFormat(t *testing.T, key client.ObjectKey) imageflowv1alpha1.ImageReferenceFormat {
	t.Helper()
	got := &imageflowv1alpha1.ImageSubscription{}
	if err := envtestClient.Get(context.Background(), key, got); err != nil {
		t.Fatalf("get stored subscription: %v", err)
	}
	return got.Spec.ImageReferenceFormat
}

func TestMutateImageReferenceFormatDefaulting(t *testing.T) {
	requireBuildRunAdmissionEnv(t)
	ctx := context.Background()

	t.Run("create without format persists Digest", func(t *testing.T) {
		sub := envtestSubscription("fmt-default-create")
		if err := envtestClient.Create(ctx, sub); err != nil {
			t.Fatalf("create without imageReferenceFormat must be admitted: %v", err)
		}
		if got := getSubscriptionFormat(t, client.ObjectKeyFromObject(sub)); got != imageflowv1alpha1.ImageReferenceFormatDigest {
			t.Fatalf("stored format = %q, want %q (mutating defaulting)", got, imageflowv1alpha1.ImageReferenceFormatDigest)
		}
	})

	t.Run("update with cleared format re-defaults to Digest", func(t *testing.T) {
		// Simulate a pre-v1.2 object: create without format (mutating webhook
		// defaults to Digest), then clear the field and update. The re-default
		// path (Digest→Digest) must be admitted — this is the zero-manual-migration
		// guarantee for objects predating v1.2. Note: Tag→cleared is correctly
		// rejected as a downgrade by T3's validateFormatTransition.
		sub := envtestSubscription("fmt-default-update")
		// Create without explicit format — mutating webhook defaults to Digest
		if err := envtestClient.Create(ctx, sub); err != nil {
			t.Fatalf("create: %v", err)
		}
		stored := &imageflowv1alpha1.ImageSubscription{}
		if err := envtestClient.Get(ctx, client.ObjectKeyFromObject(sub), stored); err != nil {
			t.Fatalf("get: %v", err)
		}
		if stored.Spec.ImageReferenceFormat != imageflowv1alpha1.ImageReferenceFormatDigest {
			t.Fatalf("initial format = %q, want Digest (mutating default)", stored.Spec.ImageReferenceFormat)
		}
		// A client writing the object back without the field (omitempty drops
		// the empty string) must be re-defaulted to Digest.
		stored.Spec.ImageReferenceFormat = ""
		if err := envtestClient.Update(ctx, stored); err != nil {
			t.Fatalf("update without imageReferenceFormat must be admitted: %v", err)
		}
		if got := getSubscriptionFormat(t, client.ObjectKeyFromObject(sub)); got != imageflowv1alpha1.ImageReferenceFormatDigest {
			t.Fatalf("stored format after update = %q, want %q", got, imageflowv1alpha1.ImageReferenceFormatDigest)
		}
	})

	t.Run("explicit values survive create and update", func(t *testing.T) {
		sub := envtestSubscription("fmt-explicit-tag")
		sub.Spec.ImageReferenceFormat = imageflowv1alpha1.ImageReferenceFormatTag
		if err := envtestClient.Create(ctx, sub); err != nil {
			t.Fatalf("create with explicit Tag must be admitted: %v", err)
		}
		key := client.ObjectKeyFromObject(sub)
		if got := getSubscriptionFormat(t, key); got != imageflowv1alpha1.ImageReferenceFormatTag {
			t.Fatalf("stored format = %q, explicit Tag must not be defaulted", got)
		}

		stored := &imageflowv1alpha1.ImageSubscription{}
		if err := envtestClient.Get(ctx, key, stored); err != nil {
			t.Fatalf("get: %v", err)
		}
		stored.Spec.ImageReferenceFormat = imageflowv1alpha1.ImageReferenceFormatDigestWithTag
		if err := envtestClient.Update(ctx, stored); err != nil {
			t.Fatalf("update with explicit DigestWithTag must be admitted: %v", err)
		}
		if got := getSubscriptionFormat(t, key); got != imageflowv1alpha1.ImageReferenceFormatDigestWithTag {
			t.Fatalf("stored format after update = %q, explicit DigestWithTag must not be defaulted", got)
		}
	})
}
