package controller

import (
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/appfun-dev/imageflow/internal/delivery"
)

// T10 — generic image field resolver wiring (ADR-005).
//
// The delivery engine routes every imageFields-located target through the
// single PathAdapter slot in internal/delivery. The built-in adapter is
// registered from package init for apps/v1 workloads; the generic resolver
// for third-party kinds must NOT be: the pinned honesty test
// (TestSubscriptionDeliveryUnsupportedKindsAreHonest) proves that a build
// without the resolver reports honest Unsupported instead of guessing.
// Registering it from this Registrar's Register path keeps the unit lane
// honest while every manager booted through controller.Setup (production
// and full-manager integration lanes) gets the resolver for its lifetime.
//
// There is deliberately no controller and no watch here: the delivery
// engine's reconcile covers third-party targets, waiting states ride the
// 30s requeue and InvalidImageField results are deterministic (T9 seam,
// frozen). A fixed kind set cannot be watched for open-ended third-party
// CRDs anyway; adding discovery-driven watches is out of T10 scope.

const pathAdapterRegistrarName = "generic-image-field-resolver"

type pathAdapterRegistrar struct{}

func init() {
	Registrars = append(Registrars, pathAdapterRegistrar{})
}

// Register installs the generic resolver into the delivery PathAdapter
// slot. It ignores mgr on purpose: the resolver is a pure function of the
// live object handed to it by the delivery engine, and the registration
// contract smoke test (setup_test.go) calls every Registrar with a fake
// manager that panics on any method call.
func (pathAdapterRegistrar) Register(_ ctrl.Manager, _ Options) error {
	delivery.RegisterPathAdapter(delivery.NewPathResolver())
	ctrl.Log.WithName("setup").Info("registered the generic image field resolver (T10)",
		"slot", pathAdapterRegistrarName, "pathAdapter", "delivery.PathResolver")
	return nil
}
