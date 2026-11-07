// Package imagefields implements the restricted image-field path grammar that
// ADR-005 froze for ImageSubscription targets on third-party CRDs.
//
// Grammar (v1, frozen — any change requires a new ADR):
//
//	path      := "." segment ( "." segment )*
//	segment   := field filter?
//	field     := identStart identChar*
//	filter    := "[" key "=" value "]"
//	key       := identStart identChar*
//	value     := valueStart valueChar*
//	identStart:= "A".."Z" | "a".."z" | "_"
//	identChar := identStart | "0".."9" | "-"
//	valueStart:= "A".."Z" | "a".."z" | "0".."9"
//	valueChar := valueStart | "." | "_" | "-"
//
// Rules and rationale:
//
//   - a path addresses object fields by name and starts with "." — e.g.
//     .spec.runtime.containers[name=edge].image;
//   - arrays may only be traversed through a single-layer equality filter on a
//     stable identity key ([name=edge]); the filter must follow a field name;
//   - a path must end on a plain field (the image string), never on a filter,
//     because the resolver writes a string value into the addressed leaf;
//   - forbidden: wildcards (* and ?), numeric subscripts ([0] — unstable),
//     chained or nested filters ([a=b][c=d]), multi-condition filters
//     ([a=b,c=d]), operators other than "=" ([a!=b]), quoted values
//     ([name="x"] — the design-doc draft showed quotes; the frozen ADR-005
//     grammar does not include them), JSONPath prefixes ($), whitespace and
//     arbitrary expressions.
//
// The grammar is deliberately NOT claimed to be compatible with the OpenShift
// fieldPath annotation syntax (ADR-005 §1, ticket T3 acceptance criteria).
//
// This package is pure logic — no Kubernetes API machinery — so the admission
// webhook (T3), the subscription controller admission re-validation (T8) and
// the runtime generic image field resolver (T10) share exactly one
// implementation of the frozen contract.
package imagefields

import (
	"errors"
	"fmt"
	"strings"
)

// MaxPathLength bounds the length of one raw path. Generous headroom over real
// pod-template paths keeps the parser safe against adversarial input while
// never rejecting a legitimate configuration.
const MaxPathLength = 512

// Filter selects one element of an array by equality on a stable identity key,
// e.g. [name=edge]. Single-layer equality only (ADR-005).
type Filter struct {
	// Key is the identity field of the array elements, e.g. "name".
	Key string
	// Value is the required value of Key, e.g. "edge".
	Value string
}

// Segment is one addressable step of a path: an object field, optionally
// narrowed to a single array element through Filter.
type Segment struct {
	// Field is the object field name addressed by this segment.
	Field string
	// Filter is non-nil when Field addresses an array and exactly one element
	// is selected by a stable-identity equality filter.
	Filter *Filter
}

// Path is a parsed and grammar-valid image-field path. Segments are ordered
// from the object root towards the leaf; the last segment never carries a
// Filter (a path always ends on a plain string field).
type Path struct {
	// Raw is the original input string.
	Raw string
	// Segments are the parsed path segments, for runtime resolution (T10).
	Segments []Segment
}

// String renders the canonical form of the path. The grammar admits no
// alternative spellings, so String() round-trips to Raw for parsed paths.
func (p *Path) String() string {
	var b strings.Builder
	for _, s := range p.Segments {
		b.WriteByte('.')
		b.WriteString(s.Field)
		if s.Filter != nil {
			b.WriteByte('[')
			b.WriteString(s.Filter.Key)
			b.WriteByte('=')
			b.WriteString(s.Filter.Value)
			b.WriteByte(']')
		}
	}
	return b.String()
}

// Error reports why a path violates the frozen grammar.
type Error struct {
	// Path is the offending raw input.
	Path string
	// Reason describes the violation.
	Reason string
	// At is the byte offset in Path where the violation was detected;
	// -1 when the violation is not positional.
	At int
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.At >= 0 {
		return fmt.Sprintf("image-field path %q is invalid at offset %d: %s", e.Path, e.At, e.Reason)
	}
	return fmt.Sprintf("image-field path %q is invalid: %s", e.Path, e.Reason)
}

// ViolationKind classifies a problem found in a set of paths.
type ViolationKind int

const (
	// ViolationGrammar means one path violates the frozen grammar.
	ViolationGrammar ViolationKind = iota
	// ViolationDuplicate means two entries address the same field.
	ViolationDuplicate
	// ViolationOverlap means one entry addresses a strict prefix of another,
	// so both writes would touch the same leaf field (ADR-005: overlapping
	// paths reject the whole configuration).
	ViolationOverlap
)

// Violation describes one problem in a set of image-field paths. Index (and
// Other for pair violations) are positions in the input slice, so callers can
// build field-precise admission errors.
type Violation struct {
	// Kind of the violation.
	Kind ViolationKind
	// Index of the offending path (grammar) or first path of the pair.
	Index int
	// Other is the index of the second path of a duplicate/overlap pair;
	// -1 for grammar violations.
	Other int
	// Path is the raw offending path (grammar) or first path of the pair.
	Path string
	// OtherPath is the raw second path of the pair; empty for grammar
	// violations.
	OtherPath string
	// Reason describes the violation without indices, suitable as the detail
	// of a field error.
	Reason string
}

// Error implements the error interface.
func (v Violation) Error() string {
	switch v.Kind {
	case ViolationDuplicate:
		return fmt.Sprintf("image-field path %q duplicates %q: the same field must not be declared twice", v.OtherPath, v.Path)
	case ViolationOverlap:
		return fmt.Sprintf("image-field path %q overlaps %q: one path addresses a strict prefix of the other, so both would write the same field", v.OtherPath, v.Path)
	default:
		return (&Error{Path: v.Path, Reason: v.Reason, At: -1}).Error()
	}
}

// Parse validates one raw path against the frozen grammar and returns its
// parsed form. The returned error is always of type *Error.
func Parse(raw string) (*Path, error) {
	if err := precheck(raw); err != nil {
		return nil, err
	}
	segments, err := scanSegments(raw)
	if err != nil {
		return nil, err
	}
	if last := segments[len(segments)-1]; last.Filter != nil {
		return nil, &Error{
			Path: raw,
			At:   -1,
			Reason: "path must end on a plain field addressing the image string, not on an array filter" +
				" (a filter selects an object element, not a string field)",
		}
	}
	return &Path{Raw: raw, Segments: segments}, nil
}

// ValidateSet parses every path of one target's imageFields list and detects
// duplicates and overlaps inside the set (ADR-005: duplicate/overlapping paths
// reject the whole configuration). It returns the parsed paths — nil entries
// where parsing failed — and all violations found. An empty violation slice
// means the set is acceptable as a whole.
//
// Overlap semantics: path A conflicts with path B when A's segments are a
// strict prefix of B's (A addresses an object that B writes inside). Segments
// only compare equal when both field name and filter match, so different array
// elements ([name=a] vs [name=b]) and sibling leaves (.image vs
// .imagePullPolicy) never conflict.
func ValidateSet(raw []string) ([]*Path, []Violation) {
	parsed := make([]*Path, len(raw))
	var violations []Violation
	for i, r := range raw {
		p, err := Parse(r)
		if err != nil {
			reason := err.Error()
			var ge *Error
			if errors.As(err, &ge) {
				reason = ge.Reason
			}
			violations = append(violations, Violation{
				Kind: ViolationGrammar, Index: i, Other: -1, Path: r, Reason: reason,
			})
			continue
		}
		parsed[i] = p
	}
	for i := range parsed {
		if parsed[i] == nil {
			continue
		}
		for j := i + 1; j < len(parsed); j++ {
			if parsed[j] == nil {
				continue
			}
			switch relate(parsed[i], parsed[j]) {
			case relationEqual:
				violations = append(violations, Violation{
					Kind: ViolationDuplicate, Index: i, Other: j,
					Path: parsed[i].Raw, OtherPath: parsed[j].Raw,
				})
			case relationPrefix:
				violations = append(violations, Violation{
					Kind: ViolationOverlap, Index: i, Other: j,
					Path: parsed[i].Raw, OtherPath: parsed[j].Raw,
				})
			case relationDisjoint:
			}
		}
	}
	return parsed, violations
}

// precheck rejects the forbidden syntax families with explicit reasons before
// the structural scan runs.
func precheck(raw string) error {
	switch {
	case raw == "":
		return &Error{Reason: "path is empty", At: -1}
	case len(raw) > MaxPathLength:
		return &Error{
			Path: raw, At: -1,
			Reason: fmt.Sprintf("path exceeds the maximum length of %d bytes", MaxPathLength),
		}
	case raw[0] == '$':
		return &Error{
			Path: raw, At: 0,
			Reason: `JSONPath syntax is not supported: paths use the restricted ImageFlow dot grammar and start with "."`,
		}
	}
	if i := strings.IndexAny(raw, "*?"); i >= 0 {
		return &Error{Path: raw, At: i, Reason: "wildcards are not allowed"}
	}
	if i := strings.IndexAny(raw, `"'`); i >= 0 {
		return &Error{
			Path: raw, At: i,
			Reason: "quoted values are not part of the frozen grammar: use the plain [key=value] equality filter",
		}
	}
	if raw[0] != '.' {
		return &Error{
			Path: raw, At: 0,
			Reason: `path must start with "." (restricted dot-path grammar, not JSONPath)`,
		}
	}
	return nil
}

// scanSegments walks the "."-separated segment list.
func scanSegments(raw string) ([]Segment, error) {
	var segments []Segment
	for i := 0; i < len(raw); {
		// raw[i] == '.' is guaranteed by the caller (precheck) and by the
		// separator check at the end of each iteration.
		dot := i
		i++
		if i >= len(raw) {
			return nil, &Error{
				Path: raw, At: dot,
				Reason: `path must not end with "." (trailing empty segment)`,
			}
		}
		if raw[i] == '.' {
			return nil, &Error{Path: raw, At: i, Reason: `empty segment ("..") is not allowed`}
		}
		seg, next, err := scanSegment(raw, i)
		if err != nil {
			return nil, err
		}
		segments = append(segments, seg)
		i = next
		if i < len(raw) && raw[i] != '.' {
			return nil, &Error{
				Path: raw, At: i,
				Reason: fmt.Sprintf("invalid character %q: segments are separated by \".\""+
					" and only the [key=value] filter may follow a field name", raw[i]),
			}
		}
	}
	return segments, nil
}

// scanSegment parses one segment starting at i (the field-name start) and
// returns it with the offset just past the segment. The caller guarantees
// i < len(raw) and raw[i] != '.'.
func scanSegment(raw string, i int) (Segment, int, error) {
	if !isFieldFirst(raw[i]) {
		switch {
		case raw[i] == '[':
			return Segment{}, 0, &Error{
				Path: raw, At: i,
				Reason: "array filter must follow the field name that addresses the array (e.g. .containers[name=x].image)",
			}
		case isDigit(raw[i]):
			return Segment{}, 0, &Error{
				Path: raw, At: i,
				Reason: "field names must start with a letter or \"_\" (numeric subscripts are not allowed anywhere in the grammar)",
			}
		default:
			return Segment{}, 0, &Error{
				Path: raw, At: i,
				Reason: fmt.Sprintf("invalid character %q at the start of a field name:"+
					" fields are JSON-like identifiers [A-Za-z_][A-Za-z0-9_-]*", raw[i]),
			}
		}
	}
	start := i
	for i < len(raw) && isIdentChar(raw[i]) {
		i++
	}
	seg := Segment{Field: raw[start:i]}
	if i < len(raw) && raw[i] == '[' {
		filter, next, err := scanFilter(raw, i)
		if err != nil {
			return Segment{}, 0, err
		}
		seg.Filter = filter
		i = next
	}
	return seg, i, nil
}

// scanFilter parses "[key=value]" starting at the "[" and returns the filter
// with the offset just past the "]".
func scanFilter(raw string, open int) (*Filter, int, error) {
	i := open + 1
	if i >= len(raw) {
		return nil, 0, &Error{
			Path: raw, At: open,
			Reason: "path ends inside an array filter: expected [key=value]",
		}
	}
	if isDigit(raw[i]) {
		return nil, 0, &Error{
			Path: raw, At: i,
			Reason: "numeric subscripts are not allowed: arrays are addressed only through a stable identity key filter like [name=value]",
		}
	}
	if !isFieldFirst(raw[i]) {
		switch raw[i] {
		case '=':
			return nil, 0, &Error{Path: raw, At: i, Reason: "filter key is empty: expected [key=value]"}
		case ']':
			return nil, 0, &Error{Path: raw, At: i, Reason: "filter is empty: expected [key=value]"}
		default:
			return nil, 0, &Error{
				Path: raw, At: i,
				Reason: fmt.Sprintf("invalid character %q in filter key:"+
					" keys are identifiers [A-Za-z_][A-Za-z0-9_-]*", raw[i]),
			}
		}
	}
	keyStart := i
	for i < len(raw) && isIdentChar(raw[i]) {
		i++
	}
	key := raw[keyStart:i]
	if i >= len(raw) {
		return nil, 0, &Error{
			Path: raw, At: open,
			Reason: `unterminated filter: expected "=" and a value before "]"`,
		}
	}
	if raw[i] != '=' {
		if raw[i] == '!' || raw[i] == '<' || raw[i] == '>' {
			return nil, 0, &Error{
				Path: raw, At: i,
				Reason: `only single "=" equality filters are supported: !=, < and > are not part of the frozen grammar`,
			}
		}
		return nil, 0, &Error{
			Path: raw, At: i,
			Reason: fmt.Sprintf("invalid character %q in filter: expected \"=\" after the key", raw[i]),
		}
	}
	i++ // consume '='
	valueStart := i
	if i >= len(raw) {
		return nil, 0, &Error{Path: raw, At: open, Reason: "path ends inside an array filter: expected a value"}
	}
	if raw[i] == ']' {
		return nil, 0, &Error{Path: raw, At: i, Reason: "filter value is empty: expected [key=value]"}
	}
	if !isValueFirst(raw[i]) {
		return nil, 0, &Error{
			Path: raw, At: i,
			Reason: fmt.Sprintf("invalid character %q at the start of the filter value:"+
				" values are non-empty [A-Za-z0-9][A-Za-z0-9._-]* (stable identity values such as container names)", raw[i]),
		}
	}
	for i < len(raw) && isValueChar(raw[i]) {
		i++
	}
	value := raw[valueStart:i]
	if i >= len(raw) {
		return nil, 0, &Error{Path: raw, At: open, Reason: `unterminated filter: expected "]"`}
	}
	if raw[i] != ']' {
		if raw[i] == ',' || raw[i] == '&' || raw[i] == '|' {
			return nil, 0, &Error{
				Path: raw, At: i,
				Reason: "multi-condition filters are not allowed: exactly one key=value equality per array field (single-layer filter)",
			}
		}
		return nil, 0, &Error{
			Path: raw, At: i,
			Reason: fmt.Sprintf("invalid character %q in filter value: values are [A-Za-z0-9][A-Za-z0-9._-]*", raw[i]),
		}
	}
	i++ // consume ']'
	if i < len(raw) && raw[i] == '[' {
		return nil, 0, &Error{
			Path: raw, At: i,
			Reason: "chained or nested filters are not allowed: one single-layer [key=value] filter per array field is the maximum",
		}
	}
	return &Filter{Key: key, Value: value}, i, nil
}

// relation classifies how two parsed paths relate segment-wise.
type relation int

const (
	relationDisjoint relation = iota
	relationEqual
	relationPrefix // a is a strict prefix of b or vice versa
)

func relate(a, b *Path) relation {
	n := len(a.Segments)
	if len(b.Segments) < n {
		n = len(b.Segments)
	}
	for k := 0; k < n; k++ {
		if !segmentEqual(a.Segments[k], b.Segments[k]) {
			return relationDisjoint
		}
	}
	if len(a.Segments) == len(b.Segments) {
		return relationEqual
	}
	return relationPrefix
}

func segmentEqual(a, b Segment) bool {
	if a.Field != b.Field {
		return false
	}
	switch {
	case a.Filter == nil && b.Filter == nil:
		return true
	case a.Filter == nil || b.Filter == nil:
		return false
	default:
		return a.Filter.Key == b.Filter.Key && a.Filter.Value == b.Filter.Value
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isLetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

func isFieldFirst(c byte) bool { return isLetter(c) || c == '_' }

func isIdentChar(c byte) bool { return isLetter(c) || isDigit(c) || c == '_' || c == '-' }

func isValueFirst(c byte) bool { return isLetter(c) || isDigit(c) }

func isValueChar(c byte) bool { return isLetter(c) || isDigit(c) || c == '.' || c == '_' || c == '-' }
