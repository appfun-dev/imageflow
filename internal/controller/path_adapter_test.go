package controller

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/appfun-dev/imageflow/internal/delivery"
)

// TestGenericPathAdapterRegistrationRoutesImageFields proves the T10
// wiring: after the Registrar runs, the delivery engine classifies an
// arbitrary third-party GVK located by imageFields as supported and hands
// it the generic resolver — without any package init registering the slot
// (TestPathAdapterSlotIsTheT10Seam in internal/delivery pins it empty at
// process start, and TestSubscriptionDeliveryUnsupportedKindsAreHonest in
// this package pins honest Unsupported without registration).
func TestGenericPathAdapterRegistrationRoutesImageFields(t *testing.T) {
	if delivery.LookupPathAdapter() != nil {
		t.Fatal("the PathAdapter slot must start empty: only Register may fill it")
	}
	t.Cleanup(func() { delivery.RegisterPathAdapter(nil) })

	if err := (pathAdapterRegistrar{}).Register(nil, Options{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	adapter := delivery.LookupPathAdapter()
	if adapter == nil {
		t.Fatal("Register must install the generic resolver into the single slot")
	}
	if _, ok := adapter.(*delivery.PathResolver); !ok {
		t.Fatalf("registered adapter = %T, want *delivery.PathResolver", adapter)
	}

	gvk := schema.GroupVersionKind{
		Group: "apps.thirdparty.test", Version: "v1alpha1", Kind: "EdgeService",
	}
	kind, adapterOut, class, ok := delivery.Classify(gvk, true)
	if !ok || adapterOut == nil || class != 0 || kind != nil {
		t.Fatalf("Classify(third-party, paths) = (%v, %v, %v, %v), want the generic adapter",
			kind, adapterOut, class, ok)
	}

	// Registering again is last-wins and must not corrupt the slot.
	if err := (pathAdapterRegistrar{}).Register(nil, Options{}); err != nil {
		t.Fatalf("re-Register: %v", err)
	}
	if delivery.LookupPathAdapter() == nil {
		t.Fatal("re-Register must keep the slot filled")
	}
}

// TestGenericPathAdapterRegistrationIsIdempotentAndSurvivesNilClear pins
// the cleanup contract used by tests: nil clears the slot, so a later
// Register call can reinstall the production resolver.
func TestGenericPathAdapterRegistrationIsIdempotentAndSurvivesNilClear(t *testing.T) {
	t.Cleanup(func() { delivery.RegisterPathAdapter(nil) })
	delivery.RegisterPathAdapter(nil)
	if delivery.LookupPathAdapter() != nil {
		t.Fatal("nil must clear the slot")
	}
	if err := (pathAdapterRegistrar{}).Register(nil, Options{}); err != nil {
		t.Fatalf("Register after clear: %v", err)
	}
	if delivery.LookupPathAdapter() == nil {
		t.Fatal("Register must refill the cleared slot")
	}
}
