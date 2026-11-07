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

// ImageStreamTagSpec identifies the mirrored stream tag. It is synthesized and
// owned by the mirror controller and immutable afterwards: the CEL transition rule
// rejects user spec writes without depending on webhook availability (ADR-002 R4,
// spec decision 6). The spec fields are authoritative; the object name is only an
// index encoded as <stream>--<tag> (RFC 1123, first "--" split is injective
// because stream names must not contain "--", webhook-enforced in T3).
//
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ImageStreamTag is a controller-owned mirror object: spec is immutable and user writes are rejected; only metadata.annotations are user-writable (ADR-002 R4)"
type ImageStreamTagSpec struct {
	// Stream is the name of the source ImageStream in the same namespace.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Stream string `json:"stream"`

	// Tag is the tag name within the stream (OCI tag grammar).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`
	Tag string `json:"tag"`
}

// ImageStreamTagStatus is the controller-owned mirror status (see
// MirrorObjectStatus for the write-protection model).
type ImageStreamTagStatus struct {
	MirrorObjectStatus `json:",inline"`
}

// ImageStreamTag is the read-only mirror view of one tag of an ImageStream:
// kubectl get/watch works on vanilla Kubernetes with OpenShift-aligned UX
// (ADR-002 R4). It is synthesized by the controller from ImageStream.status +
// ImageArtifact with an ownerRef to the stream (cascading GC on stream deletion,
// user story 8). spec/status are fully controller-owned; the user-writable surface
// is exactly metadata.annotations. Subscriptions reference it in the logical
// "<stream>:<tag>" colon form and never by encoded object name.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Stream",type=string,JSONPath=`.spec.stream`
// +kubebuilder:printcolumn:name="Tag",type=string,JSONPath=`.spec.tag`
// +kubebuilder:printcolumn:name="Digest",type=string,JSONPath=`.status.image.digest`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ImageStreamTag struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageStreamTagSpec   `json:"spec,omitempty"`
	Status ImageStreamTagStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ImageStreamTagList contains a list of ImageStreamTag.
type ImageStreamTagList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageStreamTag `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImageStreamTag{}, &ImageStreamTagList{})
}
