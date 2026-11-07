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

// MirrorObjectStatus is the shared controller-owned status of the read-only mirror
// objects ImageStreamTag and ImageTag (ADR-002 R4/R9). The mirror synthesis
// controller derives it from ImageStream.status + ImageArtifact; it carries the
// R10 enrichment: the current association, the bounded event summary (image change
// history + delivery summaries, cap MirrorEventMaxItems = 5) and consumer
// references. Overflow falls to kube Events and audit logs (ADR-008).
//
// Write-protection model (ticket T2 acceptance): mirror specs are guarded by a CEL
// transition rule (self == oldSelf) that rejects every user spec update without
// depending on webhook availability. Status protection is structural: the status
// subresource strips status from main-endpoint user writes, and /status RBAC is
// restricted to the controller SA (P5). CRD-level CEL cannot be identity-aware,
// so it cannot allow controller status writes while rejecting user ones; the
// webhook-free guarantees are therefore spec=CEL and status=subresource+RBAC.
type MirrorObjectStatus struct {
	// ObservedGeneration is the generation of the source ImageStream last
	// synthesized into this mirror (mirror lag is seconds, ADR-002 consequences).
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions of the mirror object, keyed by type.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Image is the current association synthesized from the source stream.
	// +optional
	Image *MirrorImage `json:"image,omitempty"`

	// Events is the bounded event summary (image changes and delivery summaries)
	// floated up from the catalog and subscription sides, most recent first.
	// Bounded to MirrorEventMaxItems (5) at CRD level (ADR-008).
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=5
	Events []MirrorStatusEvent `json:"events,omitempty"`

	// Consumers lists the subscriptions sourcing this stream tag / digest.
	// Capped by MirrorConsumerMaxItems as a structural etcd guard (ADR-002 R4
	// requires bounded enrichment without naming a number).
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=50
	Consumers []ConsumerReference `json:"consumers,omitempty"`
}
