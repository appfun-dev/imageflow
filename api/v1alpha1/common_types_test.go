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

import "testing"

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
