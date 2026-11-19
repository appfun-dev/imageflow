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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestCanonicalTargetKeyFormat pins the name-form encoding: the
// apiVersion/kind/namespace/name four-tuple joined with "/".
func TestCanonicalTargetKeyFormat(t *testing.T) {
	ref := TargetReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Namespace:  "payments",
		Name:       "api",
	}
	if got, want := CanonicalTargetKey(ref), "apps/v1/Deployment/payments/api"; got != want {
		t.Fatalf("CanonicalTargetKey = %q, want %q", got, want)
	}
}

// TestCanonicalTargetKeySelectorFormat pins the selector-form encoding
// (ADR-014): the apiVersion/kind/namespace three-tuple followed by
// "/selector(<body>)", where <body> is the deterministic canonical encoding
// of the LabelSelector — matchLabels sorted by key as "key=value", then
// matchExpressions sorted by (key, operator, values) as "key Operator v1,v2"
// with values sorted; entries joined with ";". The "selector(" wrapper can
// never collide with a name: object names are DNS subdomains and cannot
// contain "(". The test data is declared in unsorted order on purpose so
// the pinned string proves the encoding sorts.
func TestCanonicalTargetKeySelectorFormat(t *testing.T) {
	ref := TargetReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Namespace:  "payments",
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"tier": "backend", "app": "payment"},
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "region", Operator: metav1.LabelSelectorOpExists},
				{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"staging", "prod"}},
			},
		},
	}
	want := "apps/v1/Deployment/payments/selector(app=payment;tier=backend;env In prod,staging;region Exists)"
	if got := CanonicalTargetKey(ref); got != want {
		t.Fatalf("CanonicalTargetKey = %q, want %q", got, want)
	}

	// The extension-point contract (ADR-014): whenever LabelSelector is set,
	// the selector form is encoded — a both-fields reference (rejected by
	// admission) still derives deterministically and never falls back to the
	// name form.
	both := ref
	both.Name = "api"
	if got := CanonicalTargetKey(both); got != want {
		t.Fatalf("CanonicalTargetKey with both fields set = %q, want selector form %q", got, want)
	}
}

// TestCanonicalTargetKeyUniqueness verifies that changing any single field of
// the four-tuple yields a distinct key.
func TestCanonicalTargetKeyUniqueness(t *testing.T) {
	base := TargetReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Namespace:  "payments",
		Name:       "api",
	}
	baseKey := CanonicalTargetKey(base)

	variants := map[string]TargetReference{
		"apiVersion": {APIVersion: "batch/v1", Kind: base.Kind, Namespace: base.Namespace, Name: base.Name},
		"kind":       {APIVersion: base.APIVersion, Kind: "StatefulSet", Namespace: base.Namespace, Name: base.Name},
		"namespace":  {APIVersion: base.APIVersion, Kind: base.Kind, Namespace: "other", Name: base.Name},
		"name":       {APIVersion: base.APIVersion, Kind: base.Kind, Namespace: base.Namespace, Name: "worker"},
	}
	seen := map[string]string{"base": baseKey}
	for field, ref := range variants {
		key := CanonicalTargetKey(ref)
		if key == baseKey {
			t.Errorf("variant %s collides with base key %q", field, baseKey)
		}
		if other, dup := seen[key]; dup {
			t.Errorf("variant %s collides with %s on key %q", field, other, key)
		}
		seen[key] = field
	}
}

// TestCanonicalTargetKeySelectorUniqueness verifies the selector form
// (ADR-014): changing any selector clause yields a distinct key, and the
// selector form never collides with the name form of the same tuple.
func TestCanonicalTargetKeySelectorUniqueness(t *testing.T) {
	base := TargetReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Namespace:  "payments",
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "payment"},
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"prod"}},
			},
		},
	}
	baseKey := CanonicalTargetKey(base)

	variants := map[string]TargetReference{
		"apiVersion": {
			APIVersion: "batch/v1", Kind: base.Kind, Namespace: base.Namespace,
			LabelSelector: base.LabelSelector,
		},
		"kind": {
			APIVersion: base.APIVersion, Kind: "StatefulSet", Namespace: base.Namespace,
			LabelSelector: base.LabelSelector,
		},
		"namespace": {
			APIVersion: base.APIVersion, Kind: base.Kind, Namespace: "other",
			LabelSelector: base.LabelSelector,
		},
		"matchLabels value": {
			APIVersion: base.APIVersion, Kind: base.Kind, Namespace: base.Namespace,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels:      map[string]string{"app": "billing"},
				MatchExpressions: base.LabelSelector.MatchExpressions,
			},
		},
		"matchExpressions values": {
			APIVersion: base.APIVersion, Kind: base.Kind, Namespace: base.Namespace,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: base.LabelSelector.MatchLabels,
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"staging"}},
				},
			},
		},
		"matchExpressions operator": {
			APIVersion: base.APIVersion, Kind: base.Kind, Namespace: base.Namespace,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: base.LabelSelector.MatchLabels,
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "env", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"prod"}},
				},
			},
		},
		// The two location forms of the same tuple must never share a key:
		// status entries keyed by targetRef (ADR-014) would otherwise merge a
		// Named Target with a Selector Target.
		"name form of the same tuple": {
			APIVersion: base.APIVersion, Kind: base.Kind, Namespace: base.Namespace, Name: "api",
		},
	}
	seen := map[string]string{"base": baseKey}
	for field, ref := range variants {
		key := CanonicalTargetKey(ref)
		if key == baseKey {
			t.Errorf("variant %s collides with base key %q", field, baseKey)
		}
		if other, dup := seen[key]; dup {
			t.Errorf("variant %s collides with %s on key %q", field, other, key)
		}
		seen[key] = field
	}
}

// TestCanonicalTargetKeyStability verifies the derivation is a pure function
// of the reference: repeated calls return the identical string.
func TestCanonicalTargetKeyStability(t *testing.T) {
	ref := TargetReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Namespace:  "payments",
		Name:       "api",
	}
	if CanonicalTargetKey(ref) != CanonicalTargetKey(ref) {
		t.Fatal("CanonicalTargetKey is not stable across calls")
	}
}

// TestCanonicalTargetKeySelectorStability verifies the selector form
// (ADR-014) is order-independent: the matchExpressions slice order and the
// Go map iteration order of matchLabels must never change the derived key,
// otherwise the same target would fan out into several status entries.
func TestCanonicalTargetKeySelectorStability(t *testing.T) {
	ordered := TargetReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Namespace:  "payments",
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app": "payment", "tier": "backend"},
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"prod", "staging"}},
				{Key: "region", Operator: metav1.LabelSelectorOpExists},
			},
		},
	}
	reordered := TargetReference{
		APIVersion: ordered.APIVersion,
		Kind:       ordered.Kind,
		Namespace:  ordered.Namespace,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"tier": "backend", "app": "payment"},
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "region", Operator: metav1.LabelSelectorOpExists},
				{Key: "env", Operator: metav1.LabelSelectorOpIn, Values: []string{"staging", "prod"}},
			},
		},
	}
	if got, want := CanonicalTargetKey(ordered), CanonicalTargetKey(reordered); got != want {
		t.Fatalf("selector clause order changed the key: %q vs %q", got, want)
	}

	// Go randomizes map iteration order; deriving the key from a matchLabels
	// map must be repeatable on every single call.
	key := CanonicalTargetKey(ordered)
	for i := 0; i < 100; i++ {
		if CanonicalTargetKey(ordered) != key {
			t.Fatalf("call %d changed the key, want stable %q", i, key)
		}
	}
}
