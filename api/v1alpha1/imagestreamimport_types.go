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

// ImageStreamImportSpec is the import request (pull path, ADR-004): read image
// metadata from an external registry through the standard OCI / Docker Registry v2
// API only (ADR-007) and register it onto (stream, tag). Tasks are created
// on-demand by users or periodically by the scheduled-import controller.
type ImageStreamImportSpec struct {
	// Stream is the target ImageStream name in the same namespace as this task.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Stream string `json:"stream"`

	// Tag to associate the imported image with on the target stream.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`
	Tag string `json:"tag"`

	// From is the external image reference to import (registry reachable through
	// the OCI v2 API, subject to the controller registry allowlist, ADR-007).
	// +kubebuilder:validation:Required
	From ImageSourceReference `json:"from"`

	// PullSecretRef optionally references a dockerconfigjson secret in the same
	// namespace used to authenticate against the registry. Credential material
	// never appears in spec, status, logs or Events (ADR-007, user story 12).
	// +optional
	PullSecretRef *LocalSecretReference `json:"pullSecretRef,omitempty"`
}

// ImageStreamImportStatus reports the import result with diagnosable reasons
// (network, credentials, manifest problems — user story 11). Controller-owned
// through the status subresource.
type ImageStreamImportStatus struct {
	TaskObjectStatus `json:",inline"`

	// Digest resolved from the external reference, sha256:<64hex>.
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	Digest string `json:"digest,omitempty"`

	// ImageReference is the resolved canonical digest reference
	// <repo>@sha256:<64hex>.
	// +optional
	// +kubebuilder:validation:Pattern=`^[^@\s]+@sha256:[0-9a-f]{64}$`
	ImageReference string `json:"imageReference,omitempty"`

	// ArtifactRef is the name of the cluster-scoped ImageArtifact created or
	// reused for the resolved digest (sha256-<64hex>).
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ArtifactRef string `json:"artifactRef,omitempty"`
}

// ImageStreamImport is a short-lived task CRD that pulls image metadata from an
// external registry into the catalog (ADR-002 R6, ADR-004). On success the
// controller records an ImageArtifact and the (stream, tag) association; completed
// tasks are TTL garbage collected (success 1h / failure 24h, controller flags) and
// audited via Events. ImageFlow never pushes and never stores blobs (ADR-007).
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Stream",type=string,JSONPath=`.spec.stream`
// +kubebuilder:printcolumn:name="Tag",type=string,JSONPath=`.spec.tag`
// +kubebuilder:printcolumn:name="Complete",type=string,JSONPath=`.status.conditions[?(@.type=="Complete")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ImageStreamImport struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageStreamImportSpec   `json:"spec,omitempty"`
	Status ImageStreamImportStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ImageStreamImportList contains a list of ImageStreamImport.
type ImageStreamImportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageStreamImport `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImageStreamImport{}, &ImageStreamImportList{})
}
