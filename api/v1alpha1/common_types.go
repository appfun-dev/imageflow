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

// CRD-level hard caps for bounded status arrays (ADR-008, spec issue #1 decision 8:
// "status 数组有界 3/5/5"). The effective retention at runtime comes from the
// controller flags in internal/config (--stream-tag-history-limit,
// --subscription-delivery-limit, --mirror-event-limit); the values below are the
// structural shape guards baked into the CRD schemas so a misconfigured flag can
// never grow an object past the etcd single-object red line (~1.5MB). Raising a
// flag above these caps is rejected by the API server on status write and would
// require an API revision.
const (
	// StreamTagHistoryMaxItems caps ImageStream.status.tags[].items (per-tag history depth).
	StreamTagHistoryMaxItems = 3
	// SubscriptionDeliveryMaxItems caps ImageSubscription.status.recentDeliveries.
	SubscriptionDeliveryMaxItems = 5
	// MirrorEventMaxItems caps ImageStreamTag/ImageTag .status.events.
	MirrorEventMaxItems = 5
	// MirrorConsumerMaxItems caps ImageStreamTag/ImageTag .status.consumers. ADR-002 R4
	// requires consumer references to be bounded but names no number; this cap is a
	// structural etcd guard only, not a retention policy.
	MirrorConsumerMaxItems = 50
)

// Regex source fragments reused by kubebuilder validation markers. The markers must
// carry literal values, so these constants exist for Go-side reuse (webhooks,
// registry client, tests) and are kept in sync with the markers by the manifest
// unit tests.
const (
	// DigestPattern matches a bare image digest, e.g. sha256:<64 lowercase hex chars>.
	DigestPattern = `^sha256:[0-9a-f]{64}$`
	// DigestReferencePattern matches a full digest reference <repo>@sha256:<64hex>.
	// Per ADR-005 the only value format ever written to workload image fields.
	DigestReferencePattern = `^[^@\s]+@sha256:[0-9a-f]{64}$`
)

// Condition and reason vocabulary named by the ADRs. Behavior tickets (controllers)
// build on this frozen subset; additional condition types may be introduced there.
const (
	// ConditionTypeComplete is the terminal condition on task objects
	// (ImageStreamImport / ImageStreamMapping). status=True with reason Succeeded or
	// Failed; TTL GC (ADR-004) keys off it.
	ConditionTypeComplete = "Complete"
	// ConditionTypeForbidden reports a SubjectAccessReview denial for a subscription
	// target (ADR-003): zero writes for that target, never silently degraded.
	ConditionTypeForbidden = "Forbidden"
	// ConditionTypeConflict reports a Server-Side Apply conflict on an image field
	// owned by another subscription (ADR-008: first-wins, zero writes).
	ConditionTypeConflict = "Conflict"
	// ConditionTypeUnsupported reports a target Kind outside the built-in adapter
	// set (ADR-006), or a status policy that cannot be evaluated (ADR-005).
	ConditionTypeUnsupported = "Unsupported"

	// ReasonSucceeded is the reason used with ConditionTypeComplete on success.
	ReasonSucceeded = "Succeeded"
	// ReasonFailed is the reason used with ConditionTypeComplete on failure.
	ReasonFailed = "Failed"
)

// RolloutStatus is the rollout dimension of the per-target Outcome Stages model
// (ADR-005 §3), extended with the Superseded and Conflict terminal states named by
// ADR-008. Worst-state aggregation across targets (Failed > Unknown > Progressing >
// Succeeded) is computed by the subscription controller (ADR-008), not by the API.
// +kubebuilder:validation:Enum=Progressing;Succeeded;Failed;Unknown;Superseded;Conflict
type RolloutStatus string

const (
	// RolloutProgressing means the target rollout is still advancing.
	RolloutProgressing RolloutStatus = "Progressing"
	// RolloutSucceeded means the target rollout completed successfully.
	RolloutSucceeded RolloutStatus = "Succeeded"
	// RolloutFailed means the target rollout failed.
	RolloutFailed RolloutStatus = "Failed"
	// RolloutUnknown means the rollout outcome cannot be determined (e.g. no status
	// policy for a third-party target); it is never defaulted to success (ADR-005).
	RolloutUnknown RolloutStatus = "Unknown"
	// RolloutSuperseded is a terminal state: a newer image arrived mid-rollout
	// (latest-wins, ADR-008); the delivery is neither retried nor rolled back.
	RolloutSuperseded RolloutStatus = "Superseded"
	// RolloutConflict is a terminal state: the image field is owned by another
	// subscription (first-wins via SSA, ADR-008); zero writes were performed.
	RolloutConflict RolloutStatus = "Conflict"
)

// HealthStatus is the health dimension of the per-target Outcome Stages model
// (ADR-005 §3). Health is reported independently from rollout.
// +kubebuilder:validation:Enum=Healthy;Unhealthy;Unknown
type HealthStatus string

const (
	// HealthHealthy means the evaluated health policy reports healthy.
	HealthHealthy HealthStatus = "Healthy"
	// HealthUnhealthy means the evaluated health policy reports unhealthy.
	HealthUnhealthy HealthStatus = "Unhealthy"
	// HealthUnknown means health cannot be determined (no/failed status policy);
	// it is never defaulted to healthy (ADR-005 §5).
	HealthUnknown HealthStatus = "Unknown"
)

// TargetReference locates a workload object to receive image updates.
// All four fields are required so a target is always unambiguous (design §7.2).
type TargetReference struct {
	// APIVersion of the target object, e.g. apps/v1.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	APIVersion string `json:"apiVersion"`

	// Kind of the target object, e.g. Deployment.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Kind string `json:"kind"`

	// Namespace of the target object. Targets may live in any namespace;
	// cross-namespace access is authorized via SubjectAccessReview (ADR-003).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace"`

	// Name of the target object.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// StatusPolicyRef points at a CUE status policy stored in a ConfigMap (ADR-005 §4).
// v1 freezes the storage/IO contract only; the evaluator lands in v1.1. Targets with
// an unavailable policy report rollout/health as Unknown with an explicit condition.
type StatusPolicyRef struct {
	// Name of the ConfigMap holding the policy.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace of the ConfigMap.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// Key of the policy document inside the ConfigMap data.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Key string `json:"key"`
}

// LocalSecretReference references a secret in the same namespace as the referring
// object. Used for dockerconfigjson pull secrets (ADR-007). Credential material is
// never inlined into spec, status, logs or Events.
type LocalSecretReference struct {
	// Name of the referenced secret (type kubernetes.io/dockerconfigjson).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// ImageSourceReference names an external image in a registry (the pull side of the
// catalog, ADR-004/ADR-007). v1 supports exactly one kind: DockerImage, addressed
// through the standard OCI / Docker Registry v2 API.
type ImageSourceReference struct {
	// Kind of the image source. v1 supports DockerImage only.
	// +kubebuilder:validation:Enum=DockerImage
	// +kubebuilder:default=DockerImage
	// +optional
	Kind string `json:"kind,omitempty"`

	// Name is the full external image reference, e.g. quay.io/team/app:stable or
	// quay.io/team/app@sha256:<64hex>. Tag references are required for scheduled
	// imports so tag drift can be detected (digest references cannot drift).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	Name string `json:"name"`
}

// SubscriptionSourceRef is the logical source of a subscription. v1 supports kind
// ImageStreamTag only, addressed in the logical "<stream>:<tag>" colon form; users
// never write encoded mirror object names (ADR-002). Full shape validation of the
// name (single colon split, stream/tag grammar) is performed by the validating
// webhook (spec decision 6, ticket T3).
type SubscriptionSourceRef struct {
	// Kind of the source. v1 supports ImageStreamTag only.
	// +kubebuilder:validation:Enum=ImageStreamTag
	// +kubebuilder:validation:Required
	Kind string `json:"kind"`

	// Name in logical "<stream>:<tag>" form, e.g. "payments-api:stable".
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=512
	Name string `json:"name"`

	// Namespace of the source stream; may differ from the subscription namespace
	// (catalog namespace is imageflow-system).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`
}

// MirrorEventType classifies entries of the bounded event summary that floats up to
// the mirror objects (ADR-002 R10: image change history + delivery summaries).
// +kubebuilder:validation:Enum=ImageChanged;Delivery
type MirrorEventType string

const (
	// MirrorEventImageChanged records a tag-to-digest association change on the
	// source stream (the internal Image Change surfaced on the mirror).
	MirrorEventImageChanged MirrorEventType = "ImageChanged"
	// MirrorEventDelivery records a delivery summary: which subscription consumed
	// the change and what came out of it (user story 39).
	MirrorEventDelivery MirrorEventType = "Delivery"
)

// MirrorStatusEvent is one entry of the bounded event summary on mirror objects
// (ImageStreamTag/ImageTag .status.events, cap MirrorEventMaxItems). Overflow falls
// to kube Events and audit logs (ADR-008); the array keeps the most recent entries.
type MirrorStatusEvent struct {
	// Type of the event: ImageChanged or Delivery.
	// +kubebuilder:validation:Required
	Type MirrorEventType `json:"type"`

	// Reason is a stable machine-readable reason for the event.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable summary.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	Message string `json:"message,omitempty"`

	// ObservedAt is when the underlying change or delivery outcome was observed.
	// +kubebuilder:validation:Required
	ObservedAt metav1.Time `json:"observedAt"`

	// OldDigest is the previously associated digest (ImageChanged events).
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	OldDigest string `json:"oldDigest,omitempty"`

	// NewDigest is the newly associated digest (ImageChanged events).
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	NewDigest string `json:"newDigest,omitempty"`

	// Subscription identifies the consuming subscription (Delivery events).
	// +optional
	Subscription *SubscriptionObjectRef `json:"subscription,omitempty"`

	// Target identifies the workload the delivery went to (Delivery events).
	// +optional
	Target *TargetReference `json:"target,omitempty"`

	// Result is the delivery outcome using the RolloutStatus vocabulary,
	// including the Superseded and Conflict terminal states (Delivery events).
	// +optional
	Result RolloutStatus `json:"result,omitempty"`
}

// SubscriptionObjectRef identifies an ImageSubscription by namespace and name
// (group/kind are implicit).
type SubscriptionObjectRef struct {
	// Namespace of the subscription.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// Name of the subscription.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// ConsumerReference names a subscription that consumes the mirrored stream tag or
// digest as its source ("who consumed this", user story 39).
type ConsumerReference struct {
	// Namespace of the consuming subscription.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// Name of the consuming subscription.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// MirrorImage is the current image association synthesized onto a mirror object
// from ImageStream.status and ImageArtifact (ADR-002 R4/R9).
type MirrorImage struct {
	// Digest of the currently associated image, sha256:<64hex>.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	Digest string `json:"digest"`

	// ImageReference is the canonical digest reference <repo>@sha256:<64hex>.
	// +optional
	// +kubebuilder:validation:Pattern=`^[^@\s]+@sha256:[0-9a-f]{64}$`
	ImageReference string `json:"imageReference,omitempty"`

	// ArtifactRef is the name of the cluster-scoped ImageArtifact for this digest
	// (sha256-<64hex>).
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ArtifactRef string `json:"artifactRef,omitempty"`

	// Created is when this association was first recorded on the source stream.
	// +optional
	Created *metav1.Time `json:"created,omitempty"`
}
