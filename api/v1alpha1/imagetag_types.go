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

// ImageTagSpec identifies the mirrored stream digest. It is synthesized and owned
// by the mirror controller and immutable afterwards: the CEL transition rule
// rejects user spec writes without depending on webhook availability (ADR-002 R9,
// mechanism identical to R4). The spec fields are authoritative; the object name is
// only an index encoded as <stream>--sha256-<64hex> (RFC 1123). Retag operations
// never mutate an ImageTag: they write a new ImageStreamMapping (ADR-002 R9).
//
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ImageTag is a controller-owned mirror object: spec is immutable and user writes are rejected; only metadata.annotations are user-writable (ADR-002 R9)"
type ImageTagSpec struct {
	// Stream is the name of the source ImageStream in the same namespace.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Stream string `json:"stream"`

	// Digest of the mirrored image, sha256:<64hex>. Together with Stream this is
	// the stable address used for rollback, audit and pinned deployments
	// (user story 3).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	Digest string `json:"digest"`
}

// ImageTagStatus is the controller-owned mirror status (see MirrorObjectStatus for
// the write-protection model).
type ImageTagStatus struct {
	MirrorObjectStatus `json:",inline"`
}

// ImageTag is the read-only mirror view of one digest within an ImageStream:
// precise stream+digest addressing for rollback, audit and pinned deployments
// (ADR-002 R9, mechanism identical to ImageStreamTag R4). It is synthesized by the
// controller from ImageStream.status + ImageArtifact with an ownerRef to the
// stream (cascading GC). spec/status are fully controller-owned; the user-writable
// surface is exactly metadata.annotations.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Stream",type=string,JSONPath=`.spec.stream`
// +kubebuilder:printcolumn:name="Digest",type=string,JSONPath=`.spec.digest`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ImageTag struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageTagSpec   `json:"spec,omitempty"`
	Status ImageTagStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ImageTagList contains a list of ImageTag.
type ImageTagList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageTag `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImageTag{}, &ImageTagList{})
}
