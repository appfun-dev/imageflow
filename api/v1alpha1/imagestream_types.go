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

// TagImportPolicy is the v1 minimal subset of the OpenShift import policy
// (ADR-002 R8, ADR-004 §3): scheduled + intervalSeconds. tagPolicy, importMode and
// referencePolicy are deliberately deferred to a future compatibility layer.
type TagImportPolicy struct {
	// Scheduled makes the controller periodically re-import the tag's external
	// source (spec.tags[].from) so tag drift is discovered automatically
	// (user story 10).
	// +kubebuilder:default=false
	// +optional
	Scheduled *bool `json:"scheduled,omitempty"`

	// IntervalSeconds is the period of a scheduled import. Defaults to 900s
	// (ADR-004). The controller may additionally clamp the effective interval via
	// its flags.
	// +kubebuilder:default=900
	// +kubebuilder:validation:Minimum=1
	// +optional
	IntervalSeconds *int32 `json:"intervalSeconds,omitempty"`
}

// TagReference declares one tag of a stream (ImageStream.spec.tags[]).
//
// +kubebuilder:validation:XValidation:rule="!has(self.importPolicy) || !self.importPolicy.scheduled || has(self.from)",message="spec.tags[].from is required when importPolicy.scheduled is true: a scheduled import needs a persistent external source to poll"
type TagReference struct {
	// Name of the tag, e.g. "stable". OCI tag grammar. Note: the mirror object name
	// encoding <stream>--<tag> (ADR-002) additionally requires DNS-1123-safe tags;
	// tags with uppercase or underscore characters may be normalized by the mirror
	// synthesis controller (spec fields remain authoritative, names are indexes).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`
	Name string `json:"name"`

	// From is the external registry source for this tag (pull path). Required for
	// scheduled imports; on-demand ImageStreamImport tasks may override it.
	// +optional
	From *ImageSourceReference `json:"from,omitempty"`

	// ImportPolicy controls periodic re-imports of From.
	// +optional
	ImportPolicy TagImportPolicy `json:"importPolicy,omitempty"`
}

// TagEvent records one tag-to-digest association in the bounded per-tag history
// (ImageStream.status.tags[].items, cap StreamTagHistoryMaxItems = 3, ADR-008).
// Items are ordered most recent first. Overflow falls to kube Events and audit
// logs; enforcement of the runtime bound is the catalog controller's job (flags),
// the CRD cap is the structural guard.
type TagEvent struct {
	// Digest of the associated image, sha256:<64hex>. The digest is the change
	// detection key: same tag + new digest = real image change (ADR-004).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	Digest string `json:"digest"`

	// ImageReference is the canonical digest reference <repo>@sha256:<64hex>.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[^@\s]+@sha256:[0-9a-f]{64}$`
	ImageReference string `json:"imageReference"`

	// Created is when this association was recorded on the stream.
	// +kubebuilder:validation:Required
	Created metav1.Time `json:"created"`

	// Generation is the ImageStream metadata.generation observed when this
	// association was recorded.
	// +optional
	Generation int64 `json:"generation,omitempty"`
}

// NamedTagEventList carries the bounded association history of one tag.
type NamedTagEventList struct {
	// Tag name this history belongs to (matches a spec.tags[].name).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Tag string `json:"tag"`

	// Items are the tag-to-digest associations, most recent first.
	// Bounded to StreamTagHistoryMaxItems (3) at CRD level (ADR-008).
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=3
	Items []TagEvent `json:"items,omitempty"`
}

// ImageStreamSpec declares the tags of the stream. Write paths are exactly two
// (ADR-002 R1): users edit spec; Import/Mapping task controllers update status.
type ImageStreamSpec struct {
	// Tags declared on this stream, keyed by tag name.
	// +optional
	// +listType=map
	// +listMapKey=name
	Tags []TagReference `json:"tags,omitempty"`
}

// ImageStreamStatus carries the tag-to-digest associations with bounded history.
// Controller-owned through the status subresource; the ImageStream is the single
// source of truth for the catalog (ADR-002 R1).
type ImageStreamStatus struct {
	// ObservedGeneration is the stream generation last processed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions of the stream (e.g. import failures), keyed by type.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Tags with their current association and bounded history, keyed by tag name.
	// +optional
	// +listType=map
	// +listMapKey=tag
	Tags []NamedTagEventList `json:"tags,omitempty"`
}

// ImageStream is the catalog's single source of truth for tag-to-digest
// associations (ADR-002 R1). It stores no image layers and is not a registry
// (ADR-007). The catalog namespace is imageflow-system.
//
// Admission note (ticket T2 acceptance): the constraint that stream names must not
// contain "--" (so mirror names <stream>--<tag> split injectively on the first
// "--", ADR-002) is enforced by the validating webhook in ticket T3, not here.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
type ImageStream struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ImageStreamSpec   `json:"spec,omitempty"`
	Status ImageStreamStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ImageStreamList contains a list of ImageStream.
type ImageStreamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageStream `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ImageStream{}, &ImageStreamList{})
}
