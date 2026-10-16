/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SubscriptionTarget declares one workload whose image fields follow the
// subscription source. Locator fields are mutually exclusive: built-in Kinds
// (Deployment/StatefulSet/DaemonSet, ADR-006) locate images by containerNames;
// third-party CRDs locate them by restricted imageFields paths (ADR-005 grammar).
//
// Division of enforcement (spec decision 6): the mutual exclusion of
// containerNames/imageFields, the imageFields path grammar, path overlap and
// duplicate detection are validated by the T3 validating webhook — deliberately
// NOT by CRD-level CEL here, so webhook-owned named cases stay testable. The
// structural shape below only guarantees both locators exist and are well-formed
// strings.
type SubscriptionTarget struct {
	// TargetRef locates the workload object.
	// +kubebuilder:validation:Required
	TargetRef TargetReference `json:"targetRef"`

	// ContainerNames locates image fields of built-in workload Kinds by container
	// name (pod template containers). Required non-empty for built-in Kinds; no
	// default guessing is performed (spec decision 6). Mutually exclusive with
	// imageFields (webhook-enforced).
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	ContainerNames []string `json:"containerNames,omitempty"`

	// ImageFields locates image fields of third-party CRDs by restricted path
	// (ADR-005 grammar, e.g. .spec.runtime.containers[name=edge].image). Full
	// grammar, overlap and duplicate validation is webhook-enforced (T3).
	// Mutually exclusive with containerNames.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	ImageFields []string `json:"imageFields,omitempty"`

	// StatusPolicyRef optionally references the CUE status policy ConfigMap used
	// to evaluate rollout/health for this target (ADR-005 §4). Without an
	// evaluable policy, rollout/health report Unknown — never success.
	// +optional
	StatusPolicyRef *StatusPolicyRef `json:"statusPolicyRef,omitempty"`
}

// ImageSubscriptionSpec declares source + targets (+ optional per-target status
// policy). Users own the spec; the controller owns the status.
type ImageSubscriptionSpec struct {
	// Source of the image changes: a catalog stream tag in logical
	// "<stream>:<tag>" form (ADR-002). Cross-namespace sources are allowed;
	// authorization follows ADR-003 (SubjectAccessReview of the subscription
	// creator against every target).
	// +kubebuilder:validation:Required
	Source SubscriptionSourceRef `json:"source"`

	// Targets are the workloads whose image fields follow the source. One source
	// may drive many targets across namespaces (user story 22).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Targets []SubscriptionTarget `json:"targets"`
}

// TargetDeliveryStatus is the per-target Outcome Stages report frozen by ADR-005
// §3: imageApplied must never be interpreted as rollout success; rollout and health
// are independent dimensions.
type TargetDeliveryStatus struct {
	// TargetRef echoes the spec target this status belongs to.
	// +kubebuilder:validation:Required
	TargetRef TargetReference `json:"targetRef"`

	// DesiredImage is the digest reference <repo>@sha256:<64hex> resolved from the
	// source. Digest references only (ADR-005 §2).
	// +optional
	// +kubebuilder:validation:Pattern=`^[^@\s]+@sha256:[0-9a-f]{64}$`
	DesiredImage string `json:"desiredImage,omitempty"`

	// ObservedImage is the image value currently observed in the target field(s).
	// Before the first apply it may be any reference (tag or digest), so no
	// pattern is enforced here.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	ObservedImage string `json:"observedImage,omitempty"`

	// ImageApplied reports whether the observed field value equals DesiredImage.
	// true does NOT imply rollout success (ADR-005 §3).
	// +optional
	ImageApplied bool `json:"imageApplied"`

	// RolloutStatus is the rollout dimension for this target, including the
	// Superseded/Conflict terminal states (ADR-008).
	// +optional
	RolloutStatus RolloutStatus `json:"rolloutStatus,omitempty"`

	// HealthStatus is the health dimension for this target; Unknown without an
	// evaluable status policy (ADR-005 §5).
	// +optional
	HealthStatus HealthStatus `json:"healthStatus,omitempty"`

	// Reason is a stable machine-readable reason (e.g. Forbidden, Conflict,
	// Unsupported, WaitingForRollout).
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable explanation.
	// +optional
	// +kubebuilder:validation:MaxLength=32768
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the subscription generation processed for this target.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastTransitionTime is when the target last changed rollout/health state.
	// +optional
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`

	// RetryCount is the number of consecutive failed apply/observe attempts for
	// the current image change.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RetryCount int32 `json:"retryCount,omitempty"`

	// NextRetryTime is when the controller will next retry a failed attempt.
	// +optional
	NextRetryTime *metav1.Time `json:"nextRetryTime,omitempty"`
}

// DeliveryResult records one image change delivered to one target (design §12.9,
// GLOSSARY "Delivery Result"). ImageSubscription.status.recentDeliveries keeps the
// most recent SubscriptionDeliveryMaxItems (5) entries (ADR-008); the full history
// lives in kube Events and the cluster audit pipeline.
type DeliveryResult struct {
	// Source is the subscription source at delivery time.
	// +kubebuilder:validation:Required
	Source SubscriptionSourceRef `json:"source"`

	// OldImage is the previously delivered digest reference, empty for the first
	// delivery to this target.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	OldImage string `json:"oldImage,omitempty"`

	// NewImage is the delivered digest reference <repo>@sha256:<64hex>.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[^@\s]+@sha256:[0-9a-f]{64}$`
	NewImage string `json:"newImage"`

	// TargetRef is the workload this delivery went to.
	// +kubebuilder:validation:Required
	TargetRef TargetReference `json:"targetRef"`

	// ImageApplied reports whether the field write succeeded.
	// +optional
	ImageApplied bool `json:"imageApplied"`

	// RolloutResult is the rollout outcome at record time, including Superseded
	// (latest-wins, ADR-008) and Conflict terminal states.
	// +optional
	RolloutResult RolloutStatus `json:"rolloutResult,omitempty"`

	// HealthResult is the health outcome at record time.
	// +optional
	HealthResult HealthStatus `json:"healthResult,omitempty"`

	// Reason is a stable machine-readable reason.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Reason string `json:"reason,omitempty"`

	// ObservedAt is when this delivery outcome was observed.
	// +kubebuilder:validation:Required
	ObservedAt metav1.Time `json:"observedAt"`
}

// ImageSubscriptionStatus is the controller-owned observation surface.
type ImageSubscriptionStatus struct {
	// ObservedGeneration is the subscription generation last processed.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions of the whole subscription, keyed by type.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// RolloutStatus is the aggregate worst-state across targets
	// (Failed > Unknown > Progressing > Succeeded, ADR-008). No cross-target
	// rollback exists.
	// +optional
	RolloutStatus RolloutStatus `json:"rolloutStatus,omitempty"`

	// Targets carries per-target Outcome Stages, one entry per spec target.
	// +optional
	// +listType=atomic
	Targets []TargetDeliveryStatus `json:"targets,omitempty"`

	// RecentDeliveries keeps the most recent delivery results, bounded to
	// SubscriptionDeliveryMaxItems (5) at CRD level (ADR-008); overflow falls to
	// kube Events and audit logs.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=5
	RecentDeliveries []DeliveryResult `json:"recentDeliveries,omitempty"`
}

// ImageSubscription declares that the image fields of N workload targets follow a
// catalog stream tag (ADR-002 R3, ADR-003, ADR-005, ADR-008). The value written to
// targets is always a digest reference; delivery is latest-wins with per-field SSA
// exclusivity; per-target outcomes are reported independently and aggregated to
// the worst state.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.source.name`
// +kubebuilder:printcolumn:name="Rollout",type=string,JSONPath=`.status.rolloutStatus`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ImageSubscription struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageSubscriptionSpec   `json:"spec,omitempty"`
	Status ImageSubscriptionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ImageSubscriptionList contains a list of ImageSubscription.
type ImageSubscriptionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageSubscription `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImageSubscription{}, &ImageSubscriptionList{})
}
