// Package authz implements the ImageFlow subscription authorization gate
// (ticket T8, ADR-003): SubjectAccessReview-based verification that the
// created-by identity of an ImageSubscription holds the update verb on every
// delivery target.
//
// The gate is the single SAR surface of the process and is consumed at the
// three verification timings frozen by ADR-003:
//
//  1. create admission — the validating webhook (internal/webhook) checks the
//     creator against every target before a new subscription is accepted;
//  2. targets change — the same webhook re-checks whenever spec.targets is
//     modified on update;
//  3. periodic re-verification — the subscription-authz controller
//     (internal/controller) re-checks every subscription at the
//     --sar-recheck-period cadence and flips the Forbidden condition when
//     permissions are granted or revoked.
//
// Call volume is bounded by design (ADR-003: SAR calls ≈ subscriptions ×
// targets × recheck frequency): definitive results are cached for one TTL
// (the recheck period) inside a bounded cache, and cache misses pass through
// a clock-injected token-bucket rate limiter before any SubjectAccessReview
// is created. Only definitive allowed/denied results are cached; unresolved
// checks (rate limited, unmapped kind, SAR API failure) are never cached and
// never fail open — callers must treat VerdictUnknown as "retry later, do not
// write".
//
// Zero-write guarantee: a denied target must receive no writes from any
// ImageFlow component. The delivery engine (T9) consumes this gate (or the
// Forbidden condition the controller persists) before every write.
package authz

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
	"github.com/appfun-dev/imageflow/internal/clock"
)

// DefaultRecheckPeriod is the ADR-003 default periodic re-verification
// interval; the --sar-recheck-period flag overrides it. It is also the default
// SAR result cache TTL: a cached verdict never outlives one recheck period, so
// permission revocation flips the gate within one period.
const DefaultRecheckPeriod = 10 * time.Minute

// Rate limiter defaults. They bound the SAR burst a controller restart or a
// subscription churn can produce against the API server; steady-state volume
// is already absorbed by the cache.
const (
	// DefaultLimiterQPS is the sustained SubjectAccessReview creation rate.
	DefaultLimiterQPS = 50.0
	// DefaultLimiterBurst is the token bucket capacity for SAR creation.
	DefaultLimiterBurst = 100
	// DefaultCacheMaxEntries bounds the SAR result cache size.
	DefaultCacheMaxEntries = 4096
)

// sarVerb is the verb ADR-003 freezes for target authorization: the creator
// must hold update on every target resource.
const sarVerb = "update"

// Stable machine-readable reason codes carried in TargetVerdict.Reason for
// unresolved (VerdictUnknown) verdicts. They are part of the gate API surface
// consumed by the webhook, the subscription-authz controller and the T9
// delivery engine.
const (
	// ReasonRateLimited reports that the SAR rate limiter rejected the check;
	// the caller must retry later (fail closed, never fail open).
	ReasonRateLimited = "RateLimited"
	// ReasonKindNotInstalled reports that the target kind cannot be mapped to
	// a resource (CRD absent). Delivery is structurally impossible while the
	// kind is unknown, so admission surfaces this as a warning and the
	// controller retries; it is never a Forbidden verdict.
	ReasonKindNotInstalled = "KindNotInstalled"
	// ReasonSubjectAccessReviewFailed reports a transport/API failure of the
	// SubjectAccessReview call itself.
	ReasonSubjectAccessReviewFailed = "SubjectAccessReviewFailed"
	// ReasonTargetMappingFailed reports any other RESTMapper failure
	// (malformed apiVersion/kind, discovery unavailable).
	ReasonTargetMappingFailed = "TargetMappingFailed"
)

// Verdict classifies one authorization check of one target.
type Verdict string

const (
	// VerdictAllowed means SAR returned allowed=true: the creator may update
	// the target.
	VerdictAllowed Verdict = "Allowed"
	// VerdictDenied means SAR returned allowed=false: the creator must not
	// write to the target (Forbidden condition, zero writes, ADR-003).
	VerdictDenied Verdict = "Denied"
	// VerdictUnknown means the gate could not reach a definitive answer
	// (rate limited, kind not installed, SAR API failure). Callers must not
	// write and should retry; unknown results are never cached.
	VerdictUnknown Verdict = "Unknown"
)

// Subject is the identity a SAR check is performed for. It is derived from the
// created-by annotation (ADR-003, T3 identity model): Username is always the
// Kubernetes username submitted to SAR; Groups carries the structural
// service-account groups when the identity is a service account (the API
// server attaches them to every SA request, so SAR must see them to evaluate
// group-based bindings). Plain user identities carry no groups: the frozen
// annotation model records usernames only.
type Subject struct {
	Username string
	Groups   []string
}

// serviceAccountGroupsPrefix is the structural group prefix the API server
// attaches to every service-account identity.
const serviceAccountGroupsPrefix = "system:serviceaccounts"

// ServiceAccountGroups returns the structural groups of a service-account
// identity in namespace, mirroring what the API server attaches when the
// service account authenticates ("system:serviceaccounts" and
// "system:serviceaccounts:<namespace>").
func ServiceAccountGroups(namespace string) []string {
	return []string{serviceAccountGroupsPrefix, serviceAccountGroupsPrefix + ":" + namespace}
}

// TargetVerdict is the authorization outcome for one subscription target.
type TargetVerdict struct {
	// Target is the checked workload reference.
	Target imageflowv1alpha1.TargetReference
	// Verdict is Allowed, Denied or Unknown.
	Verdict Verdict
	// Reason explains a Denied verdict (the SAR evaluation reason) or an
	// Unknown verdict (one of the Reason* codes plus detail). Empty when
	// Allowed.
	Reason string
}

// Allowed reports whether the target passed authorization.
func (v TargetVerdict) Allowed() bool { return v.Verdict == VerdictAllowed }

// Denied reports whether the target failed authorization definitively.
func (v TargetVerdict) Denied() bool { return v.Verdict == VerdictDenied }

// FormatTarget renders a target reference in the user-facing form used by
// admission messages, condition messages and Events.
func FormatTarget(ref imageflowv1alpha1.TargetReference) string {
	return ref.APIVersion + " " + ref.Kind + " " + ref.Namespace + "/" + ref.Name
}

// Bounds of the user-facing denied-target rendering shared by the admission
// authorizer (internal/webhook) and the subscription-authz controller
// (internal/controller): one consistent vocabulary across admission denials,
// Forbidden condition messages and Events.
const (
	// MaxListedDeniedTargets caps how many denied targets are spelled out
	// before the remainder is summarized.
	MaxListedDeniedTargets = 5
	// MaxTargetReasonLength caps one SAR denial reason inside a list or a
	// per-target status message.
	MaxTargetReasonLength = 256
)

// FormatDeniedTargets renders the bounded denied-target list: at most
// MaxListedDeniedTargets entries of the form
// "<apiVersion> <Kind> <ns>/<name> (<SAR reason>)", the remainder summarized.
func FormatDeniedTargets(denied []TargetVerdict) string {
	var b strings.Builder
	for i, verdict := range denied {
		if i == MaxListedDeniedTargets {
			fmt.Fprintf(&b, "; and %d more", len(denied)-MaxListedDeniedTargets)
			break
		}
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "%s (%s)", FormatTarget(verdict.Target),
			TruncateMessage(verdict.Reason, MaxTargetReasonLength))
	}
	return b.String()
}

// TruncateMessage caps a user-visible message at max bytes on a rune
// boundary, marking the cut.
func TruncateMessage(message string, max int) string {
	if len(message) <= max {
		return message
	}
	const suffix = "..."
	cut := max - len(suffix)
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return strings.TrimSpace(message[:cut]) + suffix
}

// Options configures a Gate. Zero values select the documented defaults.
type Options struct {
	// Clock is the time seam (seam S3) driving cache TTL and limiter refill.
	// nil selects clock.Real{}.
	Clock clock.Clock
	// TTL is how long a definitive SAR verdict stays cached. It must not
	// exceed the recheck period so a cached allow can never outlive one
	// periodic re-verification. <= 0 selects DefaultRecheckPeriod.
	TTL time.Duration
	// LimiterQPS is the sustained SAR creation rate. <= 0 selects
	// DefaultLimiterQPS.
	LimiterQPS float64
	// LimiterBurst is the token bucket capacity. <= 0 selects
	// DefaultLimiterBurst.
	LimiterBurst int
	// CacheMaxEntries bounds the SAR result cache. <= 0 selects
	// DefaultCacheMaxEntries.
	CacheMaxEntries int
	// Metrics receives the cache/verdict counters. nil selects the
	// process-wide shared collectors registered against controller-runtime's
	// metrics registry; tests pass NewMetrics() for unregistered collectors.
	Metrics *Metrics
}

// Gate performs cached, rate-limited SubjectAccessReview checks of one
// identity against subscription targets. A Gate is safe for concurrent use.
// Each Gate owns its cache and limiter; the webhook-side and controller-side
// gates of one process are deliberately separate instances (a shared instance
// would couple independent manager lifecycles in tests and embedders).
type Gate struct {
	client  client.Client
	mapper  meta.RESTMapper
	clock   clock.Clock
	cache   *resultCache
	limiter *tokenBucket
	metrics *Metrics
}

// NewGate builds a Gate. cl creates SubjectAccessReview objects
// (authorization.k8s.io/v1 must be in the client scheme — clientgoscheme
// covers it); mapper resolves target kinds to resources. Both are required:
// a nil client or mapper makes every check VerdictUnknown (fail closed).
func NewGate(cl client.Client, mapper meta.RESTMapper, opts Options) *Gate {
	clk := opts.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = DefaultRecheckPeriod
	}
	qps := opts.LimiterQPS
	if qps <= 0 {
		qps = DefaultLimiterQPS
	}
	burst := opts.LimiterBurst
	if burst <= 0 {
		burst = DefaultLimiterBurst
	}
	maxEntries := opts.CacheMaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultCacheMaxEntries
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = sharedMetrics()
	}
	return &Gate{
		client:  cl,
		mapper:  mapper,
		clock:   clk,
		cache:   newResultCache(ttl, maxEntries),
		limiter: newTokenBucket(clk, qps, burst),
		metrics: metrics,
	}
}

// CheckTargets checks every target reference and returns one verdict per
// input, in input order. It never returns an error: per-target failures
// surface as VerdictUnknown with a stable reason code so callers decide
// between admission warnings, requeues and Forbidden surfaces.
func (g *Gate) CheckTargets(
	ctx context.Context,
	subject Subject,
	targets []imageflowv1alpha1.TargetReference,
) []TargetVerdict {
	verdicts := make([]TargetVerdict, 0, len(targets))
	for _, ref := range targets {
		verdicts = append(verdicts, g.CheckTarget(ctx, subject, ref))
	}
	return verdicts
}

// CheckTarget performs one cached, rate-limited SAR check of subject against
// the update verb on the target (ADR-003). Definitive results are cached for
// one TTL; unresolved results are not cached.
func (g *Gate) CheckTarget(
	ctx context.Context,
	subject Subject,
	ref imageflowv1alpha1.TargetReference,
) TargetVerdict {
	attrs, err := g.resourceAttributes(ref)
	if err != nil {
		g.metrics.checks.WithLabelValues(verdictLabelUnknown).Inc()
		return TargetVerdict{Target: ref, Verdict: VerdictUnknown, Reason: err.Error()}
	}

	key := cacheKeyFor(subject, attrs)
	now := g.clock.Now()
	if entry, ok := g.cache.get(key, now); ok {
		g.metrics.lookups.WithLabelValues(lookupResultHit).Inc()
		if entry.allowed {
			g.metrics.checks.WithLabelValues(verdictLabelAllowed).Inc()
			return TargetVerdict{Target: ref, Verdict: VerdictAllowed}
		}
		g.metrics.checks.WithLabelValues(verdictLabelDenied).Inc()
		return TargetVerdict{Target: ref, Verdict: VerdictDenied, Reason: entry.reason}
	}
	g.metrics.lookups.WithLabelValues(lookupResultMiss).Inc()

	if !g.limiter.take(now) {
		g.metrics.checks.WithLabelValues(verdictLabelRateLimited).Inc()
		return TargetVerdict{
			Target:  ref,
			Verdict: VerdictUnknown,
			Reason: fmt.Sprintf("%s: SubjectAccessReview rate limit exhausted; retry later "+
				"(authorization never fails open)", ReasonRateLimited),
		}
	}

	sar := &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               subject.Username,
			Groups:             subject.Groups,
			ResourceAttributes: attrs,
		},
	}
	if err := g.client.Create(ctx, sar); err != nil {
		g.metrics.checks.WithLabelValues(verdictLabelUnknown).Inc()
		return TargetVerdict{
			Target:  ref,
			Verdict: VerdictUnknown,
			Reason:  fmt.Sprintf("%s: %v", ReasonSubjectAccessReviewFailed, err),
		}
	}
	if sar.Status.EvaluationError != "" {
		// The authorizer itself failed (e.g. an authorizing webhook errored):
		// no definitive answer, nothing cached, fail closed.
		g.metrics.checks.WithLabelValues(verdictLabelUnknown).Inc()
		return TargetVerdict{
			Target:  ref,
			Verdict: VerdictUnknown,
			Reason: fmt.Sprintf("%s: authorizer evaluation error: %s",
				ReasonSubjectAccessReviewFailed, sar.Status.EvaluationError),
		}
	}

	if sar.Status.Allowed {
		g.cache.put(key, cacheEntry{allowed: true}, now)
		g.metrics.checks.WithLabelValues(verdictLabelAllowed).Inc()
		return TargetVerdict{Target: ref, Verdict: VerdictAllowed}
	}

	reason := sar.Status.Reason
	if reason == "" {
		reason = fmt.Sprintf("SubjectAccessReview denied %s on %s for %q",
			sarVerb, FormatTarget(ref), subject.Username)
	}
	g.cache.put(key, cacheEntry{allowed: false, reason: reason}, now)
	g.metrics.checks.WithLabelValues(verdictLabelDenied).Inc()
	return TargetVerdict{Target: ref, Verdict: VerdictDenied, Reason: reason}
}

// resourceAttributes maps a target reference to SAR resource attributes for
// the frozen update verb. The RESTMapper resolves the target kind to its
// plural resource name (built-in kinds and installed CRDs alike); an unmapped
// kind yields a stable ReasonKindNotInstalled error (ADR-003 double insurance:
// a subscription may legitimately be created before its target kind/object
// exists, and the gate re-checks periodically).
func (g *Gate) resourceAttributes(
	ref imageflowv1alpha1.TargetReference,
) (*authorizationv1.ResourceAttributes, error) {
	if g.mapper == nil {
		return nil, fmt.Errorf("%s: no RESTMapper configured", ReasonTargetMappingFailed)
	}
	group, version := splitAPIVersion(ref.APIVersion)
	gvk := schema.GroupVersionKind{Group: group, Version: version, Kind: ref.Kind}
	mapping, err := g.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		if meta.IsNoMatchError(err) {
			return nil, fmt.Errorf("%s: %s is not served by the cluster (CRD absent?); "+
				"delivery is impossible until the kind exists and the gate re-checks",
				ReasonKindNotInstalled, FormatTarget(ref))
		}
		return nil, fmt.Errorf("%s: %s: %v", ReasonTargetMappingFailed, FormatTarget(ref), err)
	}
	return &authorizationv1.ResourceAttributes{
		Namespace: ref.Namespace,
		Verb:      sarVerb,
		Group:     mapping.Resource.Group,
		Version:   mapping.Resource.Version,
		Resource:  mapping.Resource.Resource,
		Name:      ref.Name,
	}, nil
}

// splitAPIVersion splits "group/version" into its parts; a bare version
// (core group) yields an empty group.
func splitAPIVersion(apiVersion string) (group, version string) {
	if g, v, found := strings.Cut(apiVersion, "/"); found {
		return g, v
	}
	return "", apiVersion
}
