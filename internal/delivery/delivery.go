// Package delivery implements the write-side building blocks of the
// ImageFlow delivery engine (ticket T9, ADR-005/006/008): the extensible
// workload adapter registry, the per-subscription Server-Side Apply field
// manager identity, and the managedFields ownership inspection that makes
// field-level exclusivity detectable.
//
// # Adapter registry (extensibility contract)
//
// Two adapter seams exist, mirroring the two mutually exclusive locator
// styles of ImageSubscription targets (spec decision 6):
//
//   - KindAdapter: one adapter per built-in workload GVK, locating image
//     fields by containerNames (ADR-006 first batch: Deployment, StatefulSet,
//     DaemonSet). Registered at package init. EXTENDING THE BUILT-IN KIND SET
//     REQUIRES A NEW ADR (ADR-006 decision 4) — registration is the mechanical
//     seam, the ADR is the governance gate.
//   - PathAdapter: the generic imageFields[] resolver for third-party kinds
//     (ADR-005 grammar). The runtime resolver is ticket T10's lane; T10
//     registers its implementation through RegisterPathAdapter from its own
//     file (init pattern), with no edits to T9 code. Until a PathAdapter is
//     registered, imageFields-located targets honestly report Unsupported.
//
// # Field-level exclusivity (ADR-008)
//
// Every subscription applies with one stable field manager
// (FieldManagerFor): managedFields is the exclusivity registry. Same field
// claimed by two subscriptions = first wins, the later subscription reports a
// terminal Conflict and performs ZERO writes. Different fields (different
// containers / different paths) coexist naturally. The ownership inspection
// in ownership.go distinguishes three manager classes on a contested field:
// this subscription (own), another ImageFlow subscription (exclusivity
// registry), and everybody else (user/GitOps managers ImageFlow takes over
// from when a subscription declares the field).
package delivery

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/appfun-dev/imageflow/internal/imagefields"
)

// FieldManagerPrefix is the frozen prefix of the per-subscription SSA field
// manager identity (ADR-008: imageflow.appfun.dev/sub-<ns>-<name>). Renaming a
// field manager is an ownership migration and a breaking change, so the prefix
// and the format are part of the API evolution rules.
const FieldManagerPrefix = "imageflow.appfun.dev/sub-"

// fieldManagerMaxLength mirrors validation.FieldManagerMaxLength in
// k8s.io/apimachinery (the API server rejects longer fieldManager values on
// every write, apply included).
const fieldManagerMaxLength = 128

// fieldManagerHashLen is the hex length of the stability suffix used when the
// frozen format would exceed the API server limit.
const fieldManagerHashLen = 12

// ManagedByAnnotation is the workload annotation the ADR-008 audit summary
// uses. It carries a static value (the ImageFlow domain), never a
// subscription identity: several subscriptions may co-own one workload
// (different containers) and per-subscription values would make the annotation
// itself an SSA conflict surface. managedFields — not this annotation — is the
// authoritative exclusivity registry.
const ManagedByAnnotation = "imageflow.appfun.dev/managed-by"

// ManagedByAnnotationValue is the static value written into
// ManagedByAnnotation by every subscription's apply configuration. Identical
// values from different field managers co-own the field without conflict.
const ManagedByAnnotationValue = "imageflow.appfun.dev"

// FieldManagerFor returns the stable SSA field manager identity of one
// subscription: imageflow.appfun.dev/sub-<ns>-<name> (ADR-008).
//
// The API server hard-limits fieldManager to 128 characters
// (validation.FieldManagerMaxLength) while namespaces (63) and names (253)
// can jointly overflow it. For overflowing identities the frozen prefix is
// kept and the <ns>-<name> part is truncated and suffixed with "-h<hash>",
// where hash is the first 12 hex characters of SHA-256 over
// "<ns>\x00<name>". The derivation depends only on (ns, name), so the identity
// of one subscription is stable across restarts and never collides with
// another subscription's identity at realistic scale (48-bit hash over
// namespace-scoped names). Identities within the limit keep the exact ADR-008
// format.
func FieldManagerFor(namespace, name string) string {
	fm := FieldManagerPrefix + namespace + "-" + name
	if len(fm) <= fieldManagerMaxLength {
		return fm
	}
	sum := sha256.Sum256([]byte(namespace + "\x00" + name))
	hash := hex.EncodeToString(sum[:])[:fieldManagerHashLen]
	// "-h<hash>" suffix: budget accounts for the separator, the marker and
	// the hash so the derived identity never exceeds the API server limit.
	budget := fieldManagerMaxLength - len(FieldManagerPrefix) - 2 - len(hash)
	base := namespace + "-" + name
	if len(base) > budget {
		base = strings.TrimRight(base[:budget], ".-")
	}
	return FieldManagerPrefix + base + "-h" + hash
}

// IsImageFlowFieldManager reports whether a managedFields manager identity
// belongs to the ImageFlow subscription family (any subscription, this one or
// another). Managers outside this family are user/GitOps/tooling identities
// that a declaring subscription takes over from (force apply); managers inside
// the family are the field-level exclusivity registry (ADR-008 first-wins).
func IsImageFlowFieldManager(manager string) bool {
	return strings.HasPrefix(manager, FieldManagerPrefix)
}

// FieldPlan is one image field a subscription declares on one target.
type FieldPlan struct {
	// Path is the parsed canonical location of the field, used for the
	// managedFields ownership inspection (ADR-005 grammar; built-in adapters
	// render pod-template container paths in exactly this grammar).
	Path *imagefields.Path
	// Container is the located container name for built-in adapters; empty
	// for generic path-located fields.
	Container string
	// Observed is the field value currently stored on the target object.
	Observed string
	// Found reports whether the field exists on the target and holds a
	// string. Found=false means the write contract cannot be verified
	// against the live object; the engine reports it, never guesses
	// (spec decision 3).
	Found bool
}

// Plan is everything the engine needs to declare one target's image fields
// with a single Server-Side Apply call.
type Plan struct {
	// Apply is the SSA apply configuration: TypeMeta + object metadata +
	// ONLY the declared image fields (plus the audit annotation). Minimal
	// field updates, never overwriting unrelated fields (ADR-005 §1).
	Apply *unstructured.Unstructured
	// Fields are the declared image fields in locator order.
	Fields []FieldPlan
}

// KindAdapter locates and declares image fields of one built-in workload GVK
// by container name (ADR-006). Implementations are pure: no client I/O.
type KindAdapter interface {
	// GVK is the exact group/version/kind this adapter serves. Target
	// references outside the registered GVKs are never guessed onto an
	// adapter (no cross-version fallbacks).
	GVK() schema.GroupVersionKind
	// Plan locates every name in containerNames inside obj and returns the
	// apply configuration declaring image on all of them. A name that does
	// not exist on the object is an error of type *ContainersNotFoundError
	// (honest reporting, no default guessing — spec decision 6).
	Plan(obj *unstructured.Unstructured, containerNames []string, image string) (*Plan, error)
}

// PathAdapter is the generic imageFields[] resolver seam for third-party
// kinds (ADR-005 grammar, runtime resolution = ticket T10). T10 registers its
// implementation with RegisterPathAdapter; the delivery engine consults
// LookupPathAdapter for every imageFields-located target and reports
// Unsupported while the slot is empty.
type PathAdapter interface {
	// Plan resolves every raw imageFields path against obj and returns the
	// apply configuration declaring image on all resolved fields. Grammar
	// violations, missing fields and non-string leaves are errors — the
	// write contract verifies the live object before writing and never
	// guesses (ADR-005 §1, spec decision 3).
	Plan(obj *unstructured.Unstructured, imageFields []string, image string) (*Plan, error)
}

// ContainersNotFoundError reports containerNames that do not exist in the
// target's pod template. The failure surface is explicit (per-target
// Condition + Event, never silent — T9 acceptance criteria).
type ContainersNotFoundError struct {
	// Kind of the target object.
	Kind string
	// Missing lists the requested container names that were not found, in
	// request order.
	Missing []string
	// Present lists the container names the object does carry, for the
	// diagnosable message (bounded by the pod template size).
	Present []string
}

func (e *ContainersNotFoundError) Error() string {
	return fmt.Sprintf("container name(s) %s not found in %s pod template (present: %s)",
		strings.Join(e.Missing, ", "), e.Kind, strings.Join(e.Present, ", "))
}

// PathViolationError reports an imageFields path that cannot be resolved
// against the live object: a grammar violation, a missing intermediate or
// leaf field, a non-string leaf, or a filter that selects no element
// (ADR-005 §1: verify before writing, never guess; spec decision 3). The
// generic resolver (T10) returns it; the delivery engine maps it onto the
// stable InvalidImageField per-target reason.
type PathViolationError struct {
	// Path is the offending raw imageFields entry.
	Path string
	// Reason describes the violation against the live object.
	Reason string
}

func (e *PathViolationError) Error() string {
	return fmt.Sprintf("image field path %q cannot be resolved against the live object: %s "+
		"(zero writes; the write contract verifies existence, string type and writability "+
		"before writing, ADR-005)", e.Path, e.Reason)
}

var registryMu sync.RWMutex

var (
	kindAdapters = map[schema.GroupVersionKind]KindAdapter{}
	pathAdapter  PathAdapter
)

// RegisterKindAdapter registers (or replaces) the adapter of one built-in
// workload GVK. Built-in adapters register from init(); a future ADR-approved
// Kind extension registers the same way without touching engine code
// (ADR-006 consequence: "Adapter 接口按可注册/可扩展设计").
func RegisterKindAdapter(a KindAdapter) {
	registryMu.Lock()
	defer registryMu.Unlock()
	kindAdapters[a.GVK()] = a
}

// LookupKindAdapter returns the adapter registered for an exact GVK, or nil.
func LookupKindAdapter(gvk schema.GroupVersionKind) KindAdapter {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return kindAdapters[gvk]
}

// RegisterPathAdapter installs the generic imageFields resolver (T10 seam).
// Registering nil clears the slot. The last registration wins; production
// wires exactly one (T10, from its own init()).
func RegisterPathAdapter(p PathAdapter) {
	registryMu.Lock()
	defer registryMu.Unlock()
	pathAdapter = p
}

// LookupPathAdapter returns the registered generic imageFields resolver, or
// nil while T10's runtime resolution is not part of the build.
func LookupPathAdapter() PathAdapter {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return pathAdapter
}

// Classification explains why no adapter can serve one target reference.
type Classification int

const (
	// ClassificationUnsupportedKind is a Kind outside the built-in first
	// batch that ADR-006 explicitly excludes (Job, CronJob, Pod,
	// ReplicationController): the built-in adapter set refuses to process it
	// and the engine reports an honest Unsupported condition.
	ClassificationUnsupportedKind Classification = iota
	// ClassificationNoAdapter is any other Kind with no registered adapter
	// (unknown apps-group version, third-party CRD located by
	// containerNames, ...). Also reported as Unsupported: the engine never
	// guesses semantics for kinds it does not serve.
	ClassificationNoAdapter
	// ClassificationNoPathAdapter is an imageFields-located target while no
	// generic PathAdapter is registered (T10 pending). Reported as
	// Unsupported with a message naming the missing resolver.
	ClassificationNoPathAdapter
)

// excludedFirstBatchKinds are the kinds ADR-006 decision 3 names explicitly:
// the built-in adapter set refuses them with an honest Unsupported condition.
var excludedFirstBatchKinds = map[schema.GroupVersionKind]bool{
	{Group: "batch", Version: "v1", Kind: "Job"}:                  true,
	{Group: "batch", Version: "v1", Kind: "CronJob"}:              true,
	{Group: "", Version: "v1", Kind: "Pod"}:                       true,
	{Group: "", Version: "v1", Kind: "ReplicationController"}:     true,
	{Group: "batch", Version: "v1beta1", Kind: "CronJob"}:         true,
	{Group: "extensions", Version: "v1beta1", Kind: "Deployment"}: true,
	{Group: "apps", Version: "v1beta1", Kind: "StatefulSet"}:      true,
	{Group: "apps", Version: "v1beta2", Kind: "DaemonSet"}:        true,
	{Group: "extensions", Version: "v1beta1", Kind: "DaemonSet"}:  true,
}

// Classify resolves which adapter serves one target locator, or explains why
// none does. locatorIsPaths distinguishes imageFields-located targets (true)
// from containerNames-located ones (false).
func Classify(gvk schema.GroupVersionKind, locatorIsPaths bool) (KindAdapter, PathAdapter, Classification, bool) {
	if locatorIsPaths {
		if p := LookupPathAdapter(); p != nil {
			return nil, p, 0, true
		}
		return nil, nil, ClassificationNoPathAdapter, false
	}
	if a := LookupKindAdapter(gvk); a != nil {
		return a, nil, 0, true
	}
	if excludedFirstBatchKinds[gvk] {
		return nil, nil, ClassificationUnsupportedKind, false
	}
	return nil, nil, ClassificationNoAdapter, false
}

// UnsupportedMessage renders the honest Unsupported explanation for one
// classification (ADR-006: 如实报告，不猜测语义; extending the built-in set
// requires a new ADR).
func UnsupportedMessage(gvk schema.GroupVersionKind, c Classification) string {
	ref := gvk.GroupVersion().String() + " " + gvk.Kind
	switch c {
	case ClassificationUnsupportedKind:
		return fmt.Sprintf("%s is outside the built-in first batch (Deployment, StatefulSet, "+
			"DaemonSet): no rollout semantics are observable for this Kind, so the built-in "+
			"adapter set refuses to process it (ADR-006). Extending the built-in set requires "+
			"a new ADR.", ref)
	case ClassificationNoPathAdapter:
		return fmt.Sprintf("%s is located by imageFields, but no generic image field resolver "+
			"is registered in this build: third-party path resolution (ADR-005 grammar) is not "+
			"available, so no writes are performed and rollout/health stay Unknown.", ref)
	default:
		return fmt.Sprintf("no delivery adapter is registered for %s: the built-in adapter set "+
			"serves Deployment, StatefulSet and DaemonSet located by containerNames (ADR-006); "+
			"third-party kinds require imageFields plus the generic resolver. Extending the "+
			"built-in set requires a new ADR.", ref)
	}
}
