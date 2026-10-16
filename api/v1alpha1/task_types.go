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

// TaskObjectStatus is the shared status base of the short-lived task objects
// (ImageStreamImport / ImageStreamMapping, ADR-002 R6/R7: spec = request,
// status = result + diagnosable reason). Completion is reported through the
// ConditionTypeComplete condition (reason Succeeded/Failed) plus CompletedAt;
// TTL GC (success 1h / failure 24h, controller flags) keys off that terminal
// state. Every state transition is audited via kube Events.
type TaskObjectStatus struct {
	// Conditions of the task, keyed by type. The terminal condition is
	// Complete=True with reason Succeeded or Failed and a diagnosable message.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// StartedAt is when the controller began processing the task.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the task reached its terminal state (success or
	// failure). TTL garbage collection counts from this timestamp (ADR-004).
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}
