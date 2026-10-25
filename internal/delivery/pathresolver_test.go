package delivery

import (
	"errors"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// T10 unit lane: the generic Image Field Resolver in isolation — the
// violation matrix (existence / string type / writability), the merged
// minimal apply shape and the atomic-list guard on the real managedFields
// encoding. The envtest lane (test/integration/t10_imagefields_test.go)
// proves the same contract against a real API server with real SSA.

const (
	t10uAPI       = "apps.thirdparty.test/v1alpha1"
	t10uKind      = "EdgeService"
	t10uName      = "edge"
	t10uNs        = "t10-unit"
	t10uDigest    = "quay.io/example/edge@sha256:" + t10uDigestHex
	t10uDigestHex = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
)

// t10uLive is the representative third-party object shape: a plain string
// field, a stable-key (list-type=map) container array, a non-string leaf,
// arrays/objects/nulls for the type matrix, an atomic legacy array with a
// duplicated identity key and a status surface for the root guard.
func t10uLive() map[string]interface{} {
	return map[string]interface{}{
		fieldSpec: map[string]interface{}{
			"edgeImage": "quay.io/example/edge:pre",
			"runtime": map[string]interface{}{
				"size": int64(3),
				"containers": []interface{}{
					map[string]interface{}{
						fieldName: "edge", fieldImage: "quay.io/example/edge:pre1", "mode": "canary",
					},
					map[string]interface{}{
						fieldName: "core", fieldImage: "quay.io/example/edge:pre2",
					},
				},
				"flag":    true,
				"nothing": nil,
				"struct":  map[string]interface{}{"a": "b"},
				"tags":    []interface{}{"a", "b"},
			},
			"legacy": map[string]interface{}{
				"images": []interface{}{
					map[string]interface{}{fieldName: "old", fieldImage: "quay.io/example/edge:pre3"},
					map[string]interface{}{fieldName: "old", fieldImage: "quay.io/example/edge:pre4"},
				},
			},
		},
		fieldStatus: map[string]interface{}{"phase": "Ready"},
	}
}

func t10uObject(body map[string]interface{}, entries ...metav1.ManagedFieldsEntry) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		fieldAPIVersion: t10uAPI,
		fieldKind:       t10uKind,
		fieldMetadata: map[string]interface{}{
			fieldName: t10uName, fieldNamespace: t10uNs,
		},
	}}
	for key, value := range body {
		obj.Object[key] = value
	}
	if len(entries) > 0 {
		obj.SetManagedFields(entries)
	}
	return obj
}

func t10uFieldsV1(raw string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager:   "kubectl-create",
		Operation: metav1.ManagedFieldsOperationUpdate,
		FieldsV1:  &metav1.FieldsV1{Raw: []byte(raw)},
	}
}

// t10uViolation runs one Plan call and asserts it fails with an explicit
// *PathViolationError naming the offending raw path.
func t10uViolation(t *testing.T, obj *unstructured.Unstructured, path, wantReason string) {
	t.Helper()
	_, err := NewPathResolver().Plan(obj, []string{path}, t10uDigest)
	var violation *PathViolationError
	if !errors.As(err, &violation) {
		t.Fatalf("Plan(%q) error = %v (%T), want *PathViolationError", path, err, err)
	}
	if violation.Path != path {
		t.Fatalf("violation path = %q, want %q", violation.Path, path)
	}
	if wantReason != "" && !contains(violation.Reason, wantReason) {
		t.Fatalf("violation reason %q does not contain %q", violation.Reason, wantReason)
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || len(haystack) >= len(needle) &&
		(haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func TestPathResolverViolationMatrix(t *testing.T) {
	obj := t10uObject(t10uLive())
	cases := []struct {
		name string
		path string
		want string
	}{
		// existence
		{"missing leaf", ".spec.runtime.initImage", "does not exist"},
		{"missing intermediate", ".spec.nosuch.image", "does not exist"},
		// string type
		{"integer leaf", ".spec.runtime.size", "not a string"},
		{"integer leaf says integer", ".spec.runtime.size", "an integer"},
		{"boolean leaf", ".spec.runtime.flag", "a boolean"},
		{"null leaf", ".spec.runtime.nothing", "null"},
		{"object leaf", ".spec.runtime.struct", "an object"},
		{"array leaf", ".spec.runtime.tags", "an array"},
		// path shape against the live object
		{"non-map intermediate", ".spec.runtime.size.deeper", "not an object"},
		{"array without filter", ".spec.runtime.containers.image", "stable-key filter"},
		// The frozen grammar already rejects a terminal filter; the
		// non-terminal filter below reaches the live-object check instead.
		{"terminal filter is a grammar violation", ".spec.edgeImage[name=x]", "plain field"},
		{"filter on non-array", ".spec.edgeImage[name=x].image", "not an array"},
		{"filter selects nothing", ".spec.runtime.containers[name=ghost].image", "selects no element"},
		{"filter miss lists present keys", ".spec.runtime.containers[name=ghost].image", "edge, core"},
		{"ambiguous stable key", ".spec.legacy.images[name=old].image", "not unique"},
		// writability: system-owned roots
		{"status root", ".status.phase", "not writable"},
		{"status root explains subresource", ".status.phase", "status subresource"},
		{"metadata root", ".metadata.name", "system-owned"},
		{"apiVersion root", ".apiVersion", "TypeMeta"},
		{"kind root", ".kind", "TypeMeta"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t10uViolation(t, obj, tc.path, tc.want)
		})
	}
}

func TestPathResolverSetViolationsAreExplicit(t *testing.T) {
	obj := t10uObject(t10uLive())
	t.Run("grammar", func(t *testing.T) {
		t10uViolation(t, obj, ".spec.runtime.containers[name=*].image", "")
		_, err := NewPathResolver().Plan(obj, []string{".spec.*"}, t10uDigest)
		var violation *PathViolationError
		if !errors.As(err, &violation) {
			t.Fatalf("wildcard path error = %v, want *PathViolationError", err)
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		_, err := NewPathResolver().Plan(obj,
			[]string{".spec.edgeImage", ".spec.edgeImage"}, t10uDigest)
		var violation *PathViolationError
		if !errors.As(err, &violation) {
			t.Fatalf("duplicate error = %v, want *PathViolationError", err)
		}
		if !contains(violation.Reason, "duplicates") {
			t.Fatalf("reason %q must say duplicates", violation.Reason)
		}
	})
	t.Run("overlap", func(t *testing.T) {
		_, err := NewPathResolver().Plan(obj,
			[]string{".spec.runtime", ".spec.runtime.size"}, t10uDigest)
		var violation *PathViolationError
		if !errors.As(err, &violation) {
			t.Fatalf("overlap error = %v, want *PathViolationError", err)
		}
		if !contains(violation.Reason, "overlaps") {
			t.Fatalf("reason %q must say overlaps", violation.Reason)
		}
	})
}

func TestPathResolverEmptyImageFieldsIsAnError(t *testing.T) {
	if _, err := NewPathResolver().Plan(t10uObject(t10uLive()), nil, t10uDigest); err == nil {
		t.Fatal("an empty imageFields set must be an explicit error, never a silent no-op")
	}
}

func TestPathResolverPlansOneMergedMinimalApply(t *testing.T) {
	obj := t10uObject(t10uLive())
	plan, err := NewPathResolver().Plan(obj, []string{
		".spec.runtime.containers[name=edge].image",
		".spec.edgeImage",
		".spec.runtime.containers[name=core].image",
	}, t10uDigest)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	want := map[string]interface{}{
		fieldAPIVersion: t10uAPI,
		fieldKind:       t10uKind,
		fieldMetadata: map[string]interface{}{
			fieldName:      t10uName,
			fieldNamespace: t10uNs,
			fieldAnnotations: map[string]interface{}{
				ManagedByAnnotation: ManagedByAnnotationValue,
			},
		},
		fieldSpec: map[string]interface{}{
			"edgeImage": t10uDigest,
			"runtime": map[string]interface{}{
				"containers": []interface{}{
					map[string]interface{}{fieldName: "edge", fieldImage: t10uDigest},
					map[string]interface{}{fieldName: "core", fieldImage: t10uDigest},
				},
			},
		},
	}
	if !reflect.DeepEqual(plan.Apply.Object, want) {
		t.Fatalf("apply config =\n%#v\nwant\n%#v", plan.Apply.Object, want)
	}

	// The apply payload must not carry anything beyond identity, audit
	// annotation and the declared image fields (minimal-field update).
	spec := plan.Apply.Object[fieldSpec].(map[string]interface{})
	if len(spec) != 2 {
		t.Fatalf("apply spec keys = %v, want only edgeImage+runtime", keysOf(spec))
	}
	runtime := spec["runtime"].(map[string]interface{})
	if len(runtime) != 1 {
		t.Fatalf("apply runtime keys = %v, want only containers", keysOf(runtime))
	}
	element := runtime["containers"].([]interface{})[0].(map[string]interface{})
	if len(element) != 2 {
		t.Fatalf("apply element keys = %v, want only name+image (identity key + declared field)",
			keysOf(element))
	}

	wantObserved := []string{
		"quay.io/example/edge:pre1", "quay.io/example/edge:pre", "quay.io/example/edge:pre2",
	}
	if len(plan.Fields) != len(wantObserved) {
		t.Fatalf("fields = %d, want %d", len(plan.Fields), len(wantObserved))
	}
	for i, f := range plan.Fields {
		if f.Observed != wantObserved[i] {
			t.Fatalf("field %d observed = %q, want %q", i, f.Observed, wantObserved[i])
		}
		if !f.Found {
			t.Fatalf("field %d must be Found (violations are errors, not silent gaps)", i)
		}
		if f.Container != "" {
			t.Fatalf("field %d container = %q, want empty (the path IS the identity)", i, f.Container)
		}
	}
	if got := plan.Fields[0].Path.String(); got != ".spec.runtime.containers[name=edge].image" {
		t.Fatalf("canonical path = %q", got)
	}
}

func TestPathResolverMergesNestedFilterPaths(t *testing.T) {
	body := map[string]interface{}{
		fieldSpec: map[string]interface{}{
			"groups": []interface{}{
				map[string]interface{}{
					fieldName: "g",
					"pods": []interface{}{
						map[string]interface{}{fieldName: "p", fieldImage: "quay.io/example/edge:pre"},
						map[string]interface{}{fieldName: "q", fieldImage: "quay.io/example/edge:untouched"},
					},
				},
			},
		},
	}
	obj := t10uObject(body)
	_, err := NewPathResolver().Plan(obj, []string{
		".spec.groups[name=g].pods[name=p].image",
		".spec.groups[name=g].size",
	}, t10uDigest)
	if err == nil {
		t.Fatal("size is absent under the matched element: Plan must fail")
	}
	plan, err := NewPathResolver().Plan(obj, []string{
		".spec.groups[name=g].pods[name=p].image",
	}, t10uDigest)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	want := map[string]interface{}{
		"groups": []interface{}{
			map[string]interface{}{
				fieldName: "g",
				"pods": []interface{}{
					map[string]interface{}{fieldName: "p", fieldImage: t10uDigest},
				},
			},
		},
	}
	got := plan.Apply.Object[fieldSpec]
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("nested apply body =\n%#v\nwant\n%#v", got, want)
	}
}

func TestPathResolverAtomicListGuard(t *testing.T) {
	body := map[string]interface{}{
		fieldSpec: map[string]interface{}{
			"legacy": map[string]interface{}{
				"images": []interface{}{
					map[string]interface{}{fieldName: "old", fieldImage: "quay.io/example/edge:pre3"},
					map[string]interface{}{fieldName: "kept", fieldImage: "quay.io/example/edge:sibling"},
				},
			},
			"runtime": map[string]interface{}{
				"containers": []interface{}{
					map[string]interface{}{fieldName: "edge", fieldImage: "quay.io/example/edge:pre1"},
				},
			},
		},
	}

	t.Run("atomic ownership refuses the element write", func(t *testing.T) {
		obj := t10uObject(body, t10uFieldsV1(
			`{"f:spec":{"f:legacy":{"f:images":{}}}}`))
		t10uViolation(t, obj, ".spec.legacy.images[name=old].image", "atomically")
	})
	t.Run("granular ownership allows the element write", func(t *testing.T) {
		obj := t10uObject(body, t10uFieldsV1(
			`{"f:spec":{"f:runtime":{"f:containers":{"k:{\"name\":\"edge\"}":{"f:image":{},"f:name":{}}}}}}`))
		if _, err := NewPathResolver().Plan(obj,
			[]string{".spec.runtime.containers[name=edge].image"}, t10uDigest); err != nil {
			t.Fatalf("map-type list must be writable: %v", err)
		}
	})
	t.Run("another owner of the same array keys its elements", func(t *testing.T) {
		obj := t10uObject(body, t10uFieldsV1(
			`{"f:spec":{"f:legacy":{"f:images":{"k:{\"name\":\"other\"}":{"f:image":{}}}}}}`))
		if _, err := NewPathResolver().Plan(obj,
			[]string{".spec.legacy.images[name=old].image"}, t10uDigest); err != nil {
			t.Fatalf("k:-keyed ownership must not trip the atomic guard: %v", err)
		}
	})
	t.Run("unrelated ownership allows the element write", func(t *testing.T) {
		obj := t10uObject(body, t10uFieldsV1(`{"f:spec":{"f:edgeImage":{}}}`))
		if _, err := NewPathResolver().Plan(obj,
			[]string{".spec.runtime.containers[name=edge].image"}, t10uDigest); err != nil {
			t.Fatalf("unrelated owner must not block: %v", err)
		}
	})
	t.Run("malformed fieldsV1 is skipped conservatively", func(t *testing.T) {
		// 42 is valid JSON (so SetManagedFields round-trips it) but not
		// a fieldsV1 object — the guard must skip it, not fail closed on garbage.
		obj := t10uObject(body, t10uFieldsV1(`42`))
		if _, err := NewPathResolver().Plan(obj,
			[]string{".spec.runtime.containers[name=edge].image"}, t10uDigest); err != nil {
			t.Fatalf("malformed entry must not block the write: %v", err)
		}
	})
}

func TestPathResolverWritesOnlyTheDigestValue(t *testing.T) {
	obj := t10uObject(t10uLive())
	plan, err := NewPathResolver().Plan(obj, []string{".spec.edgeImage"}, t10uDigest)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	got := plan.Apply.Object[fieldSpec].(map[string]interface{})["edgeImage"]
	if got != t10uDigest {
		t.Fatalf("written value = %v, want the digest reference verbatim", got)
	}
	if plan.Fields[0].Observed != "quay.io/example/edge:pre" {
		t.Fatalf("observed = %q, want the live pre-image", plan.Fields[0].Observed)
	}
}

func keysOf(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
