package imagefields

import (
	"errors"
	"strings"
	"testing"
)

func TestParseAcceptsFrozenGrammar(t *testing.T) {
	accepted := []struct {
		name string
		path string
		// segments as "field" or "field[key=value]"
		want []string
	}{
		{
			name: "simple leaf field",
			path: ".spec.image",
			want: []string{"spec", "image"},
		},
		{
			name: "single-layer equality filter on a stable identity key",
			path: ".spec.template.spec.containers[name=app].image",
			want: []string{"spec", "template", "spec", "containers[name=app]", "image"},
		},
		{
			name: "ADR-005 canonical example",
			path: ".spec.runtime.containers[name=edge].image",
			want: []string{"spec", "runtime", "containers[name=edge]", "image"},
		},
		{
			name: "design-doc sidecars example",
			path: ".spec.sidecars[name=proxy].image",
			want: []string{"spec", "sidecars[name=proxy]", "image"},
		},
		{
			name: "one filter per segment on several segments",
			path: ".spec.groups[name=blue].containers[name=app].image",
			want: []string{"spec", "groups[name=blue]", "containers[name=app]", "image"},
		},
		{
			name: "underscore and hyphen in field names",
			path: ".spec.my_field.sub-field.image",
			want: []string{"spec", "my_field", "sub-field", "image"},
		},
		{
			name: "uppercase and digits in field names",
			path: ".spec.Field2.v1beta.image",
			want: []string{"spec", "Field2", "v1beta", "image"},
		},
		{
			name: "filter key with underscore",
			path: ".spec.pods[app_id=front].image",
			want: []string{"spec", "pods[app_id=front]", "image"},
		},
		{
			name: "filter value with dots, underscore and hyphen",
			path: ".spec.instances[name=app.team-1_x].image",
			want: []string{"spec", "instances[name=app.team-1_x]", "image"},
		},
		{
			name: "filter value starting with a digit",
			path: ".spec.rings[name=0].image",
			want: []string{"spec", "rings[name=0]", "image"},
		},
		{
			name: "single segment path",
			path: ".image",
			want: []string{"image"},
		},
		{
			name: "underscore-leading field name",
			path: ".spec._internal.image",
			want: []string{"spec", "_internal", "image"},
		},
	}

	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.path)
			if err != nil {
				t.Fatalf("Parse(%q) returned unexpected error: %v", tc.path, err)
			}
			if got.Raw != tc.path {
				t.Fatalf("Raw = %q, want %q", got.Raw, tc.path)
			}
			if len(got.Segments) != len(tc.want) {
				t.Fatalf("got %d segments, want %d (%v)", len(got.Segments), len(tc.want), got.Segments)
			}
			for i, w := range tc.want {
				field, filterSpec, hasFilter := strings.Cut(w, "[")
				seg := got.Segments[i]
				if seg.Field != field {
					t.Errorf("segment %d field = %q, want %q", i, seg.Field, field)
				}
				if !hasFilter {
					if seg.Filter != nil {
						t.Errorf("segment %d has unexpected filter %+v", i, seg.Filter)
					}
					continue
				}
				if seg.Filter == nil {
					t.Fatalf("segment %d missing filter, want [%s", i, filterSpec)
				}
				wantKey, wantValue, _ := strings.Cut(strings.TrimSuffix(filterSpec, "]"), "=")
				if seg.Filter.Key != wantKey || seg.Filter.Value != wantValue {
					t.Errorf("segment %d filter = [%s=%s], want [%s=%s]",
						i, seg.Filter.Key, seg.Filter.Value, wantKey, wantValue)
				}
			}
			// Canonical rendering must round-trip: the grammar admits no
			// alternative spellings.
			if got.String() != tc.path {
				t.Errorf("String() = %q, want round-trip %q", got.String(), tc.path)
			}
		})
	}
}

func TestParseRejectsEverythingOutsideFrozenGrammar(t *testing.T) {
	rejected := []struct {
		name       string
		path       string
		wantReason string // substring that must appear in the error
	}{
		// Forbidden syntax families named by the acceptance criteria.
		{"wildcard segment", ".spec.containers[*].image", "wildcard"},
		{"wildcard field", ".spec.*", "wildcard"},
		{"question-mark wildcard", ".spec.containers[name=app?].image", "wildcard"},
		{"numeric subscript", ".spec.containers[0].image", "numeric subscript"},
		{"numeric subscript bare", ".spec.0.image", "numeric subscript"},
		{"chained filters", ".spec.containers[name=a][env=b].image", "chained or nested filters"},
		{"multi-condition comma", ".spec.containers[name=a,env=b].image", "multi-condition"},
		{"multi-condition and", ".spec.containers[name=a&&env=b].image", "multi-condition"},
		{"not-equal operator", ".spec.containers[name!=a].image", "equality filters"},
		{"greater-than operator", ".spec.containers[name>a].image", "equality filters"},
		{"expression call", ".spec.containers[name=a].image.length()", `segments are separated by "."`},
		{"jsonpath prefix", "$.spec.image", "JSONPath"},
		{"quoted filter value", `.spec.containers[name="a"].image`, "quoted values"},
		{"single-quoted filter value", `.spec.containers[name='a'].image`, "quoted values"},

		// Structural violations.
		{"empty path", "", "empty"},
		{"no leading dot", "spec.image", `must start with "."`},
		{"only a dot", ".", `must not end with "."`},
		{"trailing dot", ".spec.image.", `must not end with "."`},
		{"double dot", ".spec..image", "empty segment"},
		{"terminal filter", ".spec.containers[name=a]", "must end on a plain field"},
		{"filter without field", ".[name=a].image", "must follow the field name"},
		{"empty filter", ".spec.containers[].image", "filter is empty"},
		{"empty filter key", ".spec.containers[=a].image", "filter key is empty"},
		{"empty filter value", ".spec.containers[name=].image", "filter value is empty"},
		{"unterminated filter", ".spec.containers[name=a.image", "unterminated filter"},
		{"filter value containing the leaf", ".spec.containers[name=a.image]", "must end on a plain field"},
		{"space in path", ".spec.containers[name =a].image", "filter"},
		{"space between segments", ".spec .image", `invalid character`},
		{"tab in path", ".spec\t.image", `invalid character`},
		{"slash in path", ".spec/containers.image", `invalid character`},
		{"colon in path", ".spec:name.image", `invalid character`},
		{"at sign in path", ".spec@name.image", `invalid character`},
		{"field starting with hyphen", ".spec.-name.image", "start of a field name"},
		{"field starting with dot value", ".spec.containers[name=.a].image", "start of the filter value"},
		{"unicode field", ".spec.镜像.image", "start of a field name"},
		{"bracket value not closed by key", ".spec.containers[a b=c].image", "filter"},
		{"path too long", ".spec." + strings.Repeat("a", MaxPathLength), "maximum length"},
	}

	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.path)
			if err == nil {
				t.Fatalf("Parse(%q) = %+v, want rejection", tc.path, got)
			}
			if got != nil {
				t.Fatalf("Parse(%q) returned a path together with an error", tc.path)
			}
			if !strings.Contains(err.Error(), tc.wantReason) {
				t.Fatalf("Parse(%q) error = %q, want it to contain %q", tc.path, err.Error(), tc.wantReason)
			}
			// Errors must be typed so callers can build field-precise
			// admission messages.
			var ge *Error
			if !errors.As(err, &ge) {
				t.Fatalf("Parse(%q) error %v is not of type *Error", tc.path, err)
			}
			if ge.Path != tc.path {
				t.Fatalf("Error.Path = %q, want %q", ge.Path, tc.path)
			}
		})
	}
}

func TestValidateSetDetectsDuplicatesAndOverlaps(t *testing.T) {
	tests := []struct {
		name      string
		paths     []string
		wantKinds []ViolationKind // in detection order
		wantPairs [][2]int        // (Index, Other) per expected violation
	}{
		{
			name:  "distinct leaves of the same element do not conflict",
			paths: []string{".spec.containers[name=a].image", ".spec.containers[name=a].imagePullPolicy"},
		},
		{
			name:  "distinct array elements do not conflict",
			paths: []string{".spec.containers[name=a].image", ".spec.containers[name=b].image"},
		},
		{
			name:  "distinct subtrees do not conflict",
			paths: []string{".spec.runtime.containers[name=edge].image", ".spec.sidecars[name=proxy].image"},
		},
		{
			name:  "filter presence makes segments distinct",
			paths: []string{".spec.containers.image", ".spec.containers[name=a].image"},
		},
		{
			name:      "exact duplicate",
			paths:     []string{".spec.image", ".spec.image"},
			wantKinds: []ViolationKind{ViolationDuplicate},
			wantPairs: [][2]int{{0, 1}},
		},
		{
			name:      "duplicate with filter",
			paths:     []string{".spec.containers[name=a].image", ".spec.containers[name=a].image"},
			wantKinds: []ViolationKind{ViolationDuplicate},
			wantPairs: [][2]int{{0, 1}},
		},
		{
			name:      "ancestor overlap",
			paths:     []string{".spec.runtime", ".spec.runtime.containers[name=edge].image"},
			wantKinds: []ViolationKind{ViolationOverlap},
			wantPairs: [][2]int{{0, 1}},
		},
		{
			name:      "overlap indices follow list order regardless of direction",
			paths:     []string{".spec.runtime.containers[name=edge].image", ".spec.runtime"},
			wantKinds: []ViolationKind{ViolationOverlap},
			wantPairs: [][2]int{{0, 1}},
		},
		{
			name: "multiple problems in one set",
			paths: []string{
				".spec.containers[name=a].image",
				".spec.containers[*].image",      // grammar
				".spec.containers[name=a].image", // duplicate of 0
			},
			wantKinds: []ViolationKind{ViolationGrammar, ViolationDuplicate},
			wantPairs: [][2]int{{1, -1}, {0, 2}},
		},
		{
			name:  "empty set is valid (the locator XOR is the webhook's job)",
			paths: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, violations := ValidateSet(tc.paths)
			if len(parsed) != len(tc.paths) {
				t.Fatalf("parsed %d entries, want %d", len(parsed), len(tc.paths))
			}
			if len(violations) != len(tc.wantKinds) {
				t.Fatalf("got %d violations (%v), want %d", len(violations), violations, len(tc.wantKinds))
			}
			for i, want := range tc.wantKinds {
				v := violations[i]
				if v.Kind != want {
					t.Errorf("violation %d kind = %v, want %v (%v)", i, v.Kind, want, v)
				}
				if [2]int{v.Index, v.Other} != tc.wantPairs[i] {
					t.Errorf("violation %d indices = (%d,%d), want (%d,%d)",
						i, v.Index, v.Other, tc.wantPairs[i][0], tc.wantPairs[i][1])
				}
				if v.Error() == "" {
					t.Errorf("violation %d has an empty message", i)
				}
			}
			// Entries that parsed successfully are returned positionally.
			for i, p := range parsed {
				if p == nil {
					continue
				}
				if p.Raw != tc.paths[i] {
					t.Errorf("parsed[%d].Raw = %q, want %q", i, p.Raw, tc.paths[i])
				}
			}
		})
	}
}

func TestValidateSetGrammarViolationCarriesReason(t *testing.T) {
	_, violations := ValidateSet([]string{".spec.containers[0].image"})
	if len(violations) != 1 {
		t.Fatalf("got %d violations, want 1", len(violations))
	}
	v := violations[0]
	if v.Kind != ViolationGrammar || v.Index != 0 || v.Other != -1 {
		t.Fatalf("unexpected violation %+v", v)
	}
	if v.Path != ".spec.containers[0].image" {
		t.Fatalf("Violation.Path = %q", v.Path)
	}
	if !strings.Contains(v.Reason, "numeric subscript") {
		t.Fatalf("Violation.Reason = %q, want it to name the numeric subscript rule", v.Reason)
	}
}
