package delivery

// managedFields ownership inspection (ADR-008: "managedFields 即独占登记表").
//
// The exclusivity registry is the target object's metadata.managedFields list:
// every entry records one field manager and the set of fields it owns, encoded
// as the fieldsV1 JSON trie ({"f:spec":{"f:template":...}} with set-element
// keys rendered as k:{"name":"app"}). This file answers one question per
// declared image field path: which manager class owns it —
//
//   - the applying subscription itself (re-asserting ownership is safe),
//   - another ImageFlow subscription (the first-wins exclusivity registry:
//     the later subscription reports a terminal Conflict and writes NOTHING),
//   - a non-ImageFlow manager (user kubectl / GitOps tooling: a declaring
//     subscription takes the field over with a forced apply).
//
// The walk is deliberately conservative: any parse surprise counts as
// "not owned by this entry". A missed ImageFlow ownership can only lead to a
// server-side apply conflict, which the engine handles through the same
// classification on a fresh read — the API server remains the arbiter.

import (
	"encoding/json"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/appfun-dev/imageflow/internal/imagefields"
)

// Ownership classifies who owns a declared field path per managedFields.
type Ownership int

const (
	// OwnershipUnowned means no manager entry owns the path (fresh field, or
	// released): a plain (unforced) apply claims it.
	OwnershipUnowned Ownership = iota
	// OwnershipSelf means this subscription's field manager already owns the
	// path: re-asserting is a no-op-safe apply.
	OwnershipSelf
	// OwnershipOtherSubscription means ANOTHER ImageFlow subscription field
	// manager owns the path (ADR-008 first-wins). The identity of the owning
	// manager is reported so the engine can decide between a live Conflict
	// and an orphaned-entry takeover (occupant deleted or target dropped
	// from its spec).
	OwnershipOtherSubscription
	// OwnershipForeign means only non-ImageFlow managers (user/GitOps) own
	// the path: the declaring subscription takes it over with a forced
	// apply.
	OwnershipForeign
)

// FieldOwnership is the ownership classification of one declared field path.
type FieldOwnership struct {
	// Path is the canonical field path (ADR-005 grammar).
	Path string
	// Ownership is the manager-class verdict.
	Ownership Ownership
	// SubscriptionManagers lists the ImageFlow field managers (other than
	// self) owning the path, for the live-claimant lookup and diagnosable
	// Conflict messages.
	SubscriptionManagers []string
}

// InspectOwnership classifies every declared field path of one plan against
// the target's managedFields. self is this subscription's field manager.
func InspectOwnership(entries []metav1.ManagedFieldsEntry, fields []FieldPlan, self string) []FieldOwnership {
	out := make([]FieldOwnership, 0, len(fields))
	for _, f := range fields {
		out = append(out, inspectOne(entries, f.Path, self))
	}
	return out
}

func inspectOne(entries []metav1.ManagedFieldsEntry, path *imagefields.Path, self string) FieldOwnership {
	result := FieldOwnership{Path: path.String(), Ownership: OwnershipUnowned}
	anyOwner := false
	selfOwns := false
	for i := range entries {
		entry := &entries[i]
		if entry.FieldsV1 == nil || len(entry.FieldsV1.Raw) == 0 {
			continue
		}
		if !FieldsV1OwnsPath(entry.FieldsV1.Raw, path) {
			continue
		}
		anyOwner = true
		switch {
		case entry.Manager == self:
			selfOwns = true
		case IsImageFlowFieldManager(entry.Manager):
			result.SubscriptionManagers = append(result.SubscriptionManagers, entry.Manager)
		}
	}
	switch {
	case len(result.SubscriptionManagers) > 0:
		sort.Strings(result.SubscriptionManagers)
		result.Ownership = OwnershipOtherSubscription
	case selfOwns:
		// Self ownership wins over foreign co-owners: re-asserting is safe
		// and the equal-and-owned fast path skips the write entirely.
		result.Ownership = OwnershipSelf
	case anyOwner:
		// Only non-ImageFlow managers (user/GitOps) own the path: the
		// declaring subscription takes it over (unforced apply when values
		// agree; forced apply after the conflict round trip when they
		// differ).
		result.Ownership = OwnershipForeign
	default:
		result.Ownership = OwnershipUnowned
	}
	return result
}

// FieldsV1OwnsPath reports whether one managedFields fieldsV1 trie owns the
// leaf addressed by path. Intermediate segments must exist as field maps; a
// set filter ([key=value]) descends through the k:<key-json> element entry.
// Malformed JSON or unexpected shapes answer false (conservative, see the
// file header).
func FieldsV1OwnsPath(fieldsV1 []byte, path *imagefields.Path) bool {
	var root map[string]interface{}
	if err := json.Unmarshal(fieldsV1, &root); err != nil {
		return false
	}
	current := root
	for i, seg := range path.Segments {
		next, ok := current["f:"+seg.Field]
		if !ok {
			return false
		}
		if seg.Filter != nil {
			nextMap, ok := next.(map[string]interface{})
			if !ok {
				return false
			}
			elem, ok := nextMap[setElementKey(seg.Filter)]
			if !ok {
				return false
			}
			elemMap, ok := elem.(map[string]interface{})
			if !ok {
				return false
			}
			current = elemMap
			continue
		}
		if i == len(path.Segments)-1 {
			// The leaf key exists in the parent map: owned.
			return true
		}
		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return false
		}
		current = nextMap
	}
	return false
}

// setElementKey renders the managedFields element key of a set filter:
// k:{"name":"app"} — the API server serializes set keys as compact JSON
// objects with sorted keys.
func setElementKey(filter *imagefields.Filter) string {
	key := map[string]string{filter.Key: filter.Value}
	encoded, err := json.Marshal(key)
	if err != nil {
		// Unreachable for string maps; the conservative false in the caller
		// handles it.
		return "k:\x00invalid"
	}
	return "k:" + string(encoded)
}
