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

// ImageStreamMappingSpec is the registration request (push path, ADR-004): CI
// pipelines create a Mapping after a successful BuildRun to register the built
// repo@digest onto (stream, tag). Mapping is the only v1 bridge from build output
// into the catalog.
type ImageStreamMappingSpec struct {
	// Stream is the target ImageStream name in the same namespace as this task.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Stream string `json:"stream"`

	// Tag to associate the pushed image with on the target stream. Retag is done
	// by writing a new Mapping for the existing digest (ADR-002 R9); there is no
	// other mutation path.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`
	Tag string `json:"tag"`

	// Image is the pushed image as a digest reference <repo>@sha256:<64hex>.
	// Tag-only references are rejected: a registration without a digest cannot
	// produce an Image Change and would poison the downstream chain (ADR-004).
	// The controller may verify the reference against the registry; caller-provided
	// metadata is not blindly trusted (ADR-002 R7).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^[^@\s]+@sha256:[0-9a-f]{64}$`
	Image string `json:"image"`

	// PullSecretRef optionally references a dockerconfigjson secret in the same
	// namespace, used when the controller verifies the image against the registry.
	// Credential material never appears in spec, status, logs or Events (ADR-007).
	// +optional
	PullSecretRef *LocalSecretReference `json:"pullSecretRef,omitempty"`
}

// ImageStreamMappingStatus reports the registration result. Controller-owned
// through the status subresource. Registering the same (stream, tag, digest)
// again is an idempotent no-op success and produces no duplicate Image Change
// (ADR-004, user story 16).
type ImageStreamMappingStatus struct {
	TaskObjectStatus `json:",inline"`

	// Digest normalized from spec.image, sha256:<64hex>.
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	Digest string `json:"digest,omitempty"`

	// ArtifactRef is the name of the cluster-scoped ImageArtifact created or
	// reused for the digest (sha256-<64hex>).
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ArtifactRef string `json:"artifactRef,omitempty"`
}

// ImageStreamMapping is a short-lived task CRD that registers an already-pushed
// image into the catalog (ADR-002 R7, ADR-004). Same lifecycle as
// ImageStreamImport: TTL GC after completion (success 1h / failure 24h,
// controller flags) and Event audit throughout. Created by CI/Tekton pipeline
// steps; quickstart ships a ready-made example (user story 17).
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Stream",type=string,JSONPath=`.spec.stream`
// +kubebuilder:printcolumn:name="Tag",type=string,JSONPath=`.spec.tag`
// +kubebuilder:printcolumn:name="Complete",type=string,JSONPath=`.status.conditions[?(@.type=="Complete")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ImageStreamMapping struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageStreamMappingSpec   `json:"spec,omitempty"`
	Status ImageStreamMappingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ImageStreamMappingList contains a list of ImageStreamMapping.
type ImageStreamMappingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageStreamMapping `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImageStreamMapping{}, &ImageStreamMappingList{})
}
