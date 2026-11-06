package controller

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
)

// BuildRun Watcher (T1, ADR-004 Amended + ADR-011): feature-gated optional
// controller that watches Shipwright BuildRun objects and auto-registers build
// outputs into the ImageFlow catalog. This file provides the infrastructure
// skeleton: config flag, conditional registration, and CRD existence gate.
// Reconcile logic lands in a follow-up ticket.

const (
	// shipwrightBuildCRDGroup is the API group for Shipwright Build resources.
	shipwrightBuildCRDGroup = "shipwright.io"
	// shipwrightBuildCRDVersion is the API version checked at startup.
	shipwrightBuildCRDVersion = "v1beta1"
	// shipwrightBuildCRDKind is the kind whose presence gates watcher startup.
	shipwrightBuildCRDKind = "Build"
)

// shipwrightBuildGVK is the GroupVersionKind used for CRD existence detection.
var shipwrightBuildGVK = schema.GroupVersionKind{
	Group:   shipwrightBuildCRDGroup,
	Version: shipwrightBuildCRDVersion,
	Kind:    shipwrightBuildCRDKind,
}

type buildRunWatcherRegistrar struct{}

func init() {
	Registrars = append(Registrars, &buildRunWatcherRegistrar{})
}

// Register conditionally registers the BuildRun watcher controller.
// - If EnableBuildRunWatcher is false, registration is skipped entirely (no informer created).
// - If true, the Shipwright Build CRD must exist; otherwise an error is returned
//   to prevent manager startup with a misconfigured cluster.
func (buildRunWatcherRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	if !opts.Config.EnableBuildRunWatcher {
		return nil
	}

	// CRD existence gate: verify shipwright.io/v1beta1 Build is available.
	mapper := mgr.GetRESTMapper()
	if mapper == nil {
		return fmt.Errorf("buildrun watcher: RESTMapper not available on manager")
	}

	_, err := mapper.RESTMapping(shipwrightBuildGVK.GroupKind(), shipwrightBuildGVK.Version)
	if err != nil {
		return fmt.Errorf(
			"buildrun watcher: --enable-buildrun-watcher=true but %s/%s %s CRD not found in cluster: %w",
			shipwrightBuildCRDGroup, shipwrightBuildCRDVersion, shipwrightBuildCRDKind, err,
		)
	}

	// TODO(T-next): register the actual BuildRun reconciler here once implemented.
	// For T1, successful registration means the gate passed and the controller
	// infrastructure is ready. The reconcile loop lands in a follow-up ticket.
	return nil
}
