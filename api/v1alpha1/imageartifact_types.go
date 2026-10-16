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

// ImageArtifactSpec is the content-addressed image metadata (ADR-002 R2).
// It is written once by the controller at registration time (Import/Mapping paths)
// and never updated: the artifact identity is the digest, so the spec is immutable
// by construction. The CEL transition rule below rejects every spec update —
// including user writes — without depending on webhook availability; the user's
// only writable surface on this object is metadata.annotations (which the
// controller preserves and never overwrites).
//
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="ImageArtifact spec is controller-owned and immutable; only metadata.annotations are user-writable (ADR-002 R2)"
type ImageArtifactSpec struct {
	// Digest is the immutable image identity, sha256:<64hex>. The object name is
	// derived from it (sha256-<64hex>, RFC 1123); one artifact per digest exists
	// cluster-wide (global dedup).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	Digest string `json:"digest"`

	// ImageReference is a canonical digest reference <repo>@sha256:<64hex>
	// observed at registration time.
	// +optional
	// +kubebuilder:validation:Pattern=`^[^@\s]+@sha256:[0-9a-f]{64}$`
	ImageReference string `json:"imageReference,omitempty"`

	// MediaType of the image manifest, e.g. application/vnd.oci.image.manifest.v1+json.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	MediaType string `json:"mediaType,omitempty"`

	// SizeBytes is the total size of the image blobs as reported by the registry.
	// ImageFlow stores metadata only, never blobs (ADR-007).
	// +optional
	// +kubebuilder:validation:Minimum=0
	SizeBytes *int64 `json:"sizeBytes,omitempty"`

	// Created is the image creation time from the registry metadata.
	// +optional
	Created *metav1.Time `json:"created,omitempty"`

	// Architecture of the image, e.g. amd64.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	Architecture string `json:"architecture,omitempty"`

	// OS of the image, e.g. linux.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	OS string `json:"os,omitempty"`
}

// ImageArtifactStatus is controller-owned status synced from the registration
// paths. User writes are structurally impossible: the status subresource strips
// status from main-endpoint writes, and /status RBAC is restricted to the
// controller service account (ADR-002 R2, P5 RBAC). CRD-level CEL cannot be
// identity-aware, so it guards the spec instead (see ImageArtifactSpec).
type ImageArtifactStatus struct {
	// Conditions of the artifact, keyed by type.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ImageArtifact is the cluster-scoped, content-addressed image metadata store:
// one object per digest, globally deduplicated (ADR-002 R2). spec/status are
// controller-owned; the user-writable surface is exactly metadata.annotations.
// Write access = controller SA + platform-admin RBAC.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="ImageReference",type=string,JSONPath=`.spec.imageReference`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ImageArtifact struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageArtifactSpec   `json:"spec,omitempty"`
	Status ImageArtifactStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ImageArtifactList contains a list of ImageArtifact.
type ImageArtifactList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageArtifact `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImageArtifact{}, &ImageArtifactList{})
}
