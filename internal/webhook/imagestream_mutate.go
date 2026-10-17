package webhook

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// DefaultImportIntervalSeconds is the scheduled-import default frozen by
// ADR-002 R8 / ADR-004 (900s). The CRD schema carries the same default
// (+kubebuilder:default=900 on TagImportPolicy.IntervalSeconds) and structural
// defaulting runs before mutating admission, so this webhook defaulting is the
// defensive second layer that guarantees the ADR value even if the CRD marker
// ever drifts.
const DefaultImportIntervalSeconds int32 = 900

// +kubebuilder:webhook:path=/mutate-imageflow-appfun-dev-v1alpha1-imagestream,mutating=true,failurePolicy=fail,sideEffects=None,groups=imageflow.appfun.dev,resources=imagestreams,verbs=create;update,versions=v1alpha1,name=mimagestream.kb.io,admissionReviewVersions=v1,timeoutSeconds=10

// imageStreamDefaulter fills importPolicy.intervalSeconds with the frozen
// ADR-002 R8 default when it is missing.
type imageStreamDefaulter struct{}

var _ admission.CustomDefaulter = &imageStreamDefaulter{}

// Default implements admission.CustomDefaulter.
func (d *imageStreamDefaulter) Default(_ context.Context, obj runtime.Object) error {
	stream := obj.(*imageflowv1alpha1.ImageStream)
	for i := range stream.Spec.Tags {
		policy := &stream.Spec.Tags[i].ImportPolicy
		if policy.IntervalSeconds == nil {
			policy.IntervalSeconds = ptr.To(DefaultImportIntervalSeconds)
		}
	}
	return nil
}
