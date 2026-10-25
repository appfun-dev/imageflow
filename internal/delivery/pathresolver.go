package delivery

// The generic Image Field Resolver (T10): the PathAdapter behind every
// target located by imageFields[] (ADR-005 §1). It turns the restricted
// dot-grammar paths of third-party CRD targets into the same minimal SSA
// write the built-in adapters produce, with the same exclusivity semantics
// (digest-only values, per-path field ownership, first-wins Conflict).
//
// Write contract — verified BEFORE any write, per path, against the live
// object; every failure is an explicit *PathViolationError and the engine
// performs zero writes (no guessing, ever):
//
//   - existence: every intermediate and the leaf field must be present;
//   - string type: the leaf must be a JSON string (a digest reference is
//     only ever written into a string field);
//   - writability:
//   - system-owned roots (metadata, status, apiVersion, kind) are refused —
//     a main-resource apply cannot honestly update the status subresource
//     and must not rewrite cluster-managed identity;
//   - arrays are addressed ONLY through a stable-key filter that selects
//     exactly one element (zero or multiple matches are violations);
//   - an array managed atomically in the live object (no managedFields
//     entry keys its elements, i.e. the CRD lacks x-kubernetes-list-type:
//     map keyed on the filter key) is refused: an element-level SSA write
//     would replace the whole array and erase sibling elements.
//
// The apply configuration merges every declared path of one target into a
// single minimal payload (TypeMeta + object identity + audit annotation +
// only the declared image fields); filtered segments render as list-map
// elements carrying their identity key, so SSA merges them element-wise and
// managedFields records per-element ownership — the registry the exclusivity
// inspection (ownership.go) reads.
//
// Grammar, duplicate and overlap violations are re-checked here with
// imagefields.ValidateSet even though the T3 webhook already rejects them
// at admission: objects can bypass the webhook (direct API writes, test
// lanes without webhook config), and the resolver never trusts its input.

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/appfun-dev/imageflow/internal/imagefields"
)

// Object-section and apply-payload field names. Constants keep the
// navigation literals single-sourced (goconst shares the budget with the
// package test files).
const (
	fieldMetadata    = "metadata"
	fieldStatus      = "status"
	fieldAPIVersion  = "apiVersion"
	fieldKind        = "kind"
	fieldNamespace   = "namespace"
	fieldAnnotations = "annotations"
)

// systemOwnedRoots names the top-level object sections that imageFields
// must never address, with the reason surfaced in the violation message.
var systemOwnedRoots = map[string]string{
	fieldMetadata:   "cluster-managed object identity and metadata, not an image-field surface",
	fieldStatus:     "the observation surface (status subresource); a main-resource apply would silently drop the write",
	fieldAPIVersion: "TypeMeta, rewritten by the API server on every apply",
	fieldKind:       "TypeMeta, rewritten by the API server on every apply",
}

// presentValuesCap bounds the diagnosable key list rendered into a
// filter-miss violation message.
const presentValuesCap = 8

// PathResolver is the generic Image Field Resolver. It is a pure function
// of (live object, declared imageFields, digest reference): no client, no
// cache, no scheme — so registration needs no manager wiring beyond the
// single PathAdapter slot.
type PathResolver struct{}

// NewPathResolver builds the generic resolver (T10 seam implementation).
func NewPathResolver() *PathResolver { return &PathResolver{} }

// Plan validates every declared path against the live object and builds the
// minimal SSA apply configuration writing image into all of them. Any
// violation fails the whole target with a *PathViolationError — zero
// writes, never a partial or guessed plan.
func (r *PathResolver) Plan(
	obj *unstructured.Unstructured,
	imageFields []string,
	image string,
) (*Plan, error) {
	if len(imageFields) == 0 {
		// CRD MinItems=1 plus the T3 webhook make this unreachable for
		// admitted objects; stay honest instead of writing nothing silently.
		return nil, fmt.Errorf("%s/%s: imageFields must be non-empty (no default guessing)",
			obj.GetNamespace(), obj.GetName())
	}
	paths, violations := imagefields.ValidateSet(imageFields)
	if len(violations) > 0 {
		v := violations[0]
		return nil, &PathViolationError{Path: v.Path, Reason: violationReason(v)}
	}

	plan := &Plan{}
	body := map[string]interface{}{}
	for _, path := range paths {
		observed, err := resolvePath(obj, path)
		if err != nil {
			return nil, err
		}
		insertPath(body, path.Segments, image)
		plan.Fields = append(plan.Fields, FieldPlan{
			Path:     path,
			Observed: observed,
			Found:    true,
		})
	}
	plan.Apply = applyForBody(obj, body)
	return plan, nil
}

// violationReason renders a ValidateSet violation as the resolver-side
// reason text (grammar violations carry the parser's reason; duplicate and
// overlap violations carry their set context).
func violationReason(v imagefields.Violation) string {
	switch v.Kind {
	case imagefields.ViolationDuplicate:
		return fmt.Sprintf("duplicates %q in the same imageFields set: "+
			"each field is declared exactly once", v.OtherPath)
	case imagefields.ViolationOverlap:
		return fmt.Sprintf("overlaps %q in the same imageFields set: "+
			"a declared path must not be a strict prefix of another", v.OtherPath)
	default:
		return v.Reason
	}
}

// resolvePath walks one parsed path through the live object and returns
// the current leaf value. Every contract failure is a *PathViolationError
// naming the offending raw path with a class-distinct reason.
func resolvePath(obj *unstructured.Unstructured, path *imagefields.Path) (string, error) {
	segs := path.Segments
	if why, systemOwned := systemOwnedRoots[segs[0].Field]; systemOwned {
		return "", &PathViolationError{Path: path.Raw, Reason: fmt.Sprintf(
			"the root section %q is system-owned and not writable through imageFields (%s); "+
				"declare an object-body field instead", segs[0].Field, why)}
	}
	node := obj.Object
	for i, seg := range segs {
		here := (&imagefields.Path{Segments: segs[:i+1]}).String()
		value, exists := node[seg.Field]
		if !exists {
			return "", &PathViolationError{Path: path.Raw, Reason: fmt.Sprintf(
				"field %q does not exist in the live object (checked under %s)",
				seg.Field, segmentContext(segs, i))}
		}
		if seg.Filter == nil {
			if i == len(segs)-1 {
				leaf, isString := value.(string)
				if !isString {
					return "", &PathViolationError{Path: path.Raw, Reason: fmt.Sprintf(
						"field %q is %s, not a string — refusing to write a digest reference into it",
						here, jsonKind(value))}
				}
				return leaf, nil
			}
			if _, isArray := value.([]interface{}); isArray {
				return "", &PathViolationError{Path: path.Raw, Reason: fmt.Sprintf(
					"field %q is an array: address its elements with a stable-key filter "+
						"(for example %s[%s=<key>]); whole-array writes are not allowed",
					here, here, fieldName)}
			}
			child, isMap := value.(map[string]interface{})
			if !isMap {
				return "", &PathViolationError{Path: path.Raw, Reason: fmt.Sprintf(
					"field %q is %s, not an object — the path cannot continue through it",
					here, jsonKind(value))}
			}
			node = child
			continue
		}
		element, err := resolveFilter(obj, path.Raw, segs, i, value, here)
		if err != nil {
			return "", err
		}
		if i == len(segs)-1 {
			// The frozen grammar rejects terminal filters; stay defensive.
			return "", &PathViolationError{Path: path.Raw,
				Reason: "the path ends on a filter and never names a leaf field"}
		}
		node = element
	}
	// Unreachable: the leaf branch above always returns.
	return "", &PathViolationError{Path: path.Raw,
		Reason: "internal error: path resolution fell through without a leaf"}
}

// resolveFilter selects exactly one element of the array addressed by a
// filtered segment, refusing zero matches, ambiguous matches and
// atomically managed arrays.
func resolveFilter(
	obj *unstructured.Unstructured,
	rawPath string,
	segs []imagefields.Segment,
	i int,
	value interface{},
	here string,
) (map[string]interface{}, error) {
	seg := segs[i]
	arr, isArray := value.([]interface{})
	if !isArray {
		return nil, &PathViolationError{Path: rawPath, Reason: fmt.Sprintf(
			"field %q is %s, not an array — the stable-key filter [%s=%s] does not apply to it",
			here, jsonKind(value), seg.Filter.Key, seg.Filter.Value)}
	}
	matches, present := filterMatches(arr, seg.Filter)
	if len(matches) == 0 {
		return nil, &PathViolationError{Path: rawPath, Reason: fmt.Sprintf(
			"the filter [%s=%s] selects no element of the array %q (present %s values: %s) — "+
				"zero writes, no guessing",
			seg.Filter.Key, seg.Filter.Value, here, seg.Filter.Key, presentValues(present))}
	}
	if len(matches) > 1 {
		return nil, &PathViolationError{Path: rawPath, Reason: fmt.Sprintf(
			"the filter [%s=%s] selects %d elements of %q: the stable identity key is not unique "+
				"in the live object — refusing to guess which element to write",
			seg.Filter.Key, seg.Filter.Value, len(matches), here)}
	}
	if arrayManagedAtomically(obj, segs[:i+1]) {
		return nil, &PathViolationError{Path: rawPath, Reason: fmt.Sprintf(
			"the array %q is managed atomically in the live object (no managedFields entry keys "+
				"its elements; the CRD schema must declare x-kubernetes-list-type: map with "+
				"list-map-keys [%q]): an element-level write would replace the whole array and "+
				"erase sibling elements, so the path is not writable",
			here, seg.Filter.Key)}
	}
	return matches[0], nil
}

// filterMatches returns the elements whose identity key equals the filter
// value, plus the identity-key values present in the array (for the
// diagnosable zero-match message).
func filterMatches(
	arr []interface{},
	filter *imagefields.Filter,
) (matches []map[string]interface{}, present []string) {
	for _, entry := range arr {
		element, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		key, ok := element[filter.Key].(string)
		if !ok {
			continue
		}
		present = append(present, key)
		if key == filter.Value {
			matches = append(matches, element)
		}
	}
	return matches, present
}

// presentValues renders the identity-key list of a filter miss, bounded.
func presentValues(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	shown := values
	suffix := ""
	if len(shown) > presentValuesCap {
		shown = shown[:presentValuesCap]
		suffix = ", …"
	}
	return strings.Join(shown, ", ") + suffix
}

// arrayManagedAtomically reports whether any live managedFields entry owns
// the array addressed by prefix (whose last segment carries the filter) as
// a bare leaf — the on-the-wire shape of atomic-list ownership. Granular
// (x-kubernetes-list-type: map) owners always key their elements ("k:"
// children) and unused keys are pruned, so an empty node under a non-empty
// live array is conservatively treated as atomic: element-level SSA would
// replace the whole array and clobber sibling elements.
func arrayManagedAtomically(obj *unstructured.Unstructured, prefix []imagefields.Segment) bool {
	for _, entry := range obj.GetManagedFields() {
		if entry.FieldsV1 == nil || len(entry.FieldsV1.Raw) == 0 {
			continue
		}
		var root map[string]interface{}
		if err := json.Unmarshal(entry.FieldsV1.Raw, &root); err != nil {
			continue
		}
		node := root
		for _, seg := range prefix {
			next, owned := node["f:"+seg.Field]
			if !owned {
				break // this entry does not own the prefix
			}
			child, isMap := next.(map[string]interface{})
			if seg.Filter == nil {
				if !isMap {
					break
				}
				node = child
				continue
			}
			if !isMap {
				break
			}
			if len(child) == 0 {
				return true // bare leaf node under a live non-empty array
			}
			element, keyed := child[setElementKey(seg.Filter)]
			if !keyed {
				break // this entry owns other elements, not ours
			}
			elementMap, isMap := element.(map[string]interface{})
			if !isMap {
				break
			}
			node = elementMap
		}
	}
	return false
}

// insertPath merges one declared path into the shared apply body. Filtered
// segments render as single-element list-map values carrying the identity
// key, so sibling paths through the same array (different elements) and
// paths sharing object prefixes fold into one coherent minimal payload.
// Only called for fully resolved paths: the last segment is a plain field
// (the frozen grammar rejects terminal filters).
func insertPath(body map[string]interface{}, segs []imagefields.Segment, image string) {
	seg := segs[0]
	if seg.Filter == nil {
		if len(segs) == 1 {
			body[seg.Field] = image
			return
		}
		child, ok := body[seg.Field].(map[string]interface{})
		if !ok {
			child = map[string]interface{}{}
			body[seg.Field] = child
		}
		insertPath(child, segs[1:], image)
		return
	}
	list, _ := body[seg.Field].([]interface{})
	var element map[string]interface{}
	for _, entry := range list {
		candidate, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		if key, ok := candidate[seg.Filter.Key].(string); ok && key == seg.Filter.Value {
			element = candidate
			break
		}
	}
	if element == nil {
		element = map[string]interface{}{seg.Filter.Key: seg.Filter.Value}
		list = append(list, element)
	}
	body[seg.Field] = list
	if len(segs) > 1 {
		insertPath(element, segs[1:], image)
	}
}

// applyForBody wraps the merged declared fields into the minimal SSA apply
// configuration: TypeMeta, object identity, the static audit annotation —
// exactly the shape the built-in adapters produce.
func applyForBody(obj *unstructured.Unstructured, body map[string]interface{}) *unstructured.Unstructured {
	ac := &unstructured.Unstructured{Object: map[string]interface{}{
		fieldAPIVersion: obj.GetAPIVersion(),
		fieldKind:       obj.GetKind(),
		fieldMetadata: map[string]interface{}{
			fieldName:      obj.GetName(),
			fieldNamespace: obj.GetNamespace(),
			fieldAnnotations: map[string]interface{}{
				ManagedByAnnotation: ManagedByAnnotationValue,
			},
		},
	}}
	for field, value := range body {
		// The root guard in resolvePath keeps body keys away from
		// metadata/apiVersion/kind, so this never shadows the identity.
		ac.Object[field] = value
	}
	return ac
}

// segmentContext renders where a missing field was looked up.
func segmentContext(segs []imagefields.Segment, i int) string {
	if i == 0 {
		return "the object root"
	}
	return (&imagefields.Path{Segments: segs[:i]}).String()
}

// jsonKind names the JSON shape of a live value for violation messages.
func jsonKind(value interface{}) string {
	switch value.(type) {
	case nil:
		return "null"
	case string:
		return "a string"
	case bool:
		return "a boolean"
	case int64:
		return "an integer"
	case float64:
		return "a number"
	case map[string]interface{}:
		return "an object"
	case []interface{}:
		return "an array"
	default:
		return fmt.Sprintf("a %T value", value)
	}
}
