package authz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	authorizationv1 "k8s.io/api/authorization/v1"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

// TestGateAllowsAuthorizedCreator pins the allow direction (AC: 授权允许方向
// 不误伤) and the SAR request construction: the frozen update verb, the
// mapped plural resource, and name-scoped resource attributes.
func TestGateAllowsAuthorizedCreator(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})
	ctx := context.Background()

	verdict := gate.CheckTarget(ctx, userSubject(), deploymentTarget("payments-api"))

	if verdict.Verdict != VerdictAllowed || !verdict.Allowed() {
		t.Fatalf("verdict = %+v, want Allowed", verdict)
	}
	if verdict.Reason != "" {
		t.Errorf("allowed verdict must carry no reason, got %q", verdict.Reason)
	}
	if sarLog.calls != 1 {
		t.Fatalf("SAR calls = %d, want 1", sarLog.calls)
	}
	spec := sarLog.requests[0].Spec
	if spec.User != testUser || len(spec.Groups) != 0 {
		t.Errorf("SAR subject = %q/%v, want %q with no groups", spec.User, spec.Groups, testUser)
	}
	attrs := spec.ResourceAttributes
	if attrs == nil {
		t.Fatal("SAR must be resource-scoped")
	}
	if attrs.Verb != "update" {
		t.Errorf("verb = %q, want the ADR-003 frozen update verb", attrs.Verb)
	}
	if attrs.Group != "apps" || attrs.Version != "v1" || attrs.Resource != "deployments" {
		t.Errorf("resource = %s/%s %s, want apps/v1 deployments", attrs.Group, attrs.Version, attrs.Resource)
	}
	if attrs.Namespace != testNamespace || attrs.Name != "payments-api" {
		t.Errorf("target = %s/%s, want %s/payments-api", attrs.Namespace, attrs.Name, testNamespace)
	}
}

// TestGateDeniesUnauthorizedCreator pins the deny direction and that the SAR
// denial reason is surfaced verbatim for Conditions/Events/messages.
func TestGateDeniesUnauthorizedCreator(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})
	sarLog.denyAll()

	verdict := gate.CheckTarget(context.Background(), userSubject(), deploymentTarget("billing"))

	if verdict.Verdict != VerdictDenied || !verdict.Denied() {
		t.Fatalf("verdict = %+v, want Denied", verdict)
	}
	if !strings.Contains(verdict.Reason, "forbidden for "+testUser) {
		t.Errorf("denial reason %q must carry the SAR evaluation reason", verdict.Reason)
	}
	if sarLog.calls != 1 {
		t.Errorf("SAR calls = %d, want 1", sarLog.calls)
	}
}

// TestGatePassesServiceAccountGroups verifies the structural SA groups reach
// the SAR spec so group-based bindings are evaluated exactly as they would be
// for a live request from that service account.
func TestGatePassesServiceAccountGroups(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})

	verdict := gate.CheckTarget(context.Background(), saSubject(), deploymentTarget("ci-app"))

	if verdict.Verdict != VerdictAllowed {
		t.Fatalf("verdict = %+v, want Allowed", verdict)
	}
	spec := sarLog.requests[0].Spec
	if spec.User != testSAUser {
		t.Errorf("SAR user = %q, want %q", spec.User, testSAUser)
	}
	want := []string{"system:serviceaccounts", "system:serviceaccounts:" + testSANamespace}
	if strings.Join(spec.Groups, ",") != strings.Join(want, ",") {
		t.Errorf("SAR groups = %v, want %v", spec.Groups, want)
	}
}

// TestCacheServesRepeatCheckWithoutSARCall is the cache-hit direction
// (AC: SAR 缓存命中/未命中两个方向单测): the second check of the same
// (identity, target) pair is served from cache with zero API calls, and the
// hit/miss counters move accordingly.
func TestCacheServesRepeatCheckWithoutSARCall(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})
	ctx := context.Background()
	target := deploymentTarget("payments-api")

	first := gate.CheckTarget(ctx, userSubject(), target)
	second := gate.CheckTarget(ctx, userSubject(), target)

	if first.Verdict != VerdictAllowed || second.Verdict != VerdictAllowed {
		t.Fatalf("verdicts = %+v / %+v, want Allowed twice", first, second)
	}
	if sarLog.calls != 1 {
		t.Errorf("SAR calls = %d, want 1 (second check must hit the cache)", sarLog.calls)
	}
	m := gate.metrics
	if got := testutil.ToFloat64(m.lookups.WithLabelValues(lookupResultMiss)); got != 1 {
		t.Errorf("cache misses = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.lookups.WithLabelValues(lookupResultHit)); got != 1 {
		t.Errorf("cache hits = %v, want 1", got)
	}
}

// TestCacheExpiresAtRecheckPeriodAndPicksUpRevocation proves the cache never
// outlives one recheck period in both directions: a cached allow flips to
// denied once the TTL elapsed and the creator lost permission, and a cached
// denial flips back to allowed once permission is granted — the deterministic
// core of "权限回收后定期复验及时翻转状态" (injected clock, seam S3).
func TestCacheExpiresAtRecheckPeriodAndPicksUpRevocation(t *testing.T) {
	const ttl = 10 * time.Minute
	gate, sarLog, clk := newGateFixture(t, Options{TTL: ttl})
	ctx := context.Background()
	target := deploymentTarget("payments-api")

	if v := gate.CheckTarget(ctx, userSubject(), target); v.Verdict != VerdictAllowed {
		t.Fatalf("initial verdict = %+v, want Allowed", v)
	}

	// Permission revoked; inside the TTL the cached allow is still served.
	sarLog.denyAll()
	clk.Advance(ttl - time.Second)
	if v := gate.CheckTarget(ctx, userSubject(), target); v.Verdict != VerdictAllowed {
		t.Fatalf("inside TTL verdict = %+v, want cached Allowed", v)
	}
	if sarLog.calls != 1 {
		t.Fatalf("SAR calls inside TTL = %d, want 1 (cache hit)", sarLog.calls)
	}

	// Once the TTL elapsed the revocation is picked up.
	clk.Advance(2 * time.Second)
	v := gate.CheckTarget(ctx, userSubject(), target)
	if v.Verdict != VerdictDenied {
		t.Fatalf("after TTL verdict = %+v, want Denied", v)
	}
	if sarLog.calls != 2 {
		t.Fatalf("SAR calls after TTL = %d, want 2 (cache miss re-check)", sarLog.calls)
	}

	// Permission granted again; after the TTL the gate flips back to allowed.
	sarLog.allowAll()
	clk.Advance(ttl)
	if v := gate.CheckTarget(ctx, userSubject(), target); v.Verdict != VerdictAllowed {
		t.Fatalf("re-grant verdict = %+v, want Allowed", v)
	}
	if sarLog.calls != 3 {
		t.Errorf("SAR calls after re-grant = %d, want 3", sarLog.calls)
	}
}

// TestCacheNeverSharedAcrossIdentities pins the identity-change semantics:
// a cached verdict of one creator is never served for another, even for the
// same target.
func TestCacheNeverSharedAcrossIdentities(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})
	ctx := context.Background()
	target := deploymentTarget("shared-app")

	if v := gate.CheckTarget(ctx, userSubject(), target); v.Verdict != VerdictAllowed {
		t.Fatalf("alice verdict = %+v, want Allowed", v)
	}
	sarLog.denyAll()
	v := gate.CheckTarget(ctx, saSubject(), target)
	if v.Verdict != VerdictDenied {
		t.Fatalf("service-account verdict = %+v, want Denied (fresh SAR, no cross-identity reuse)", v)
	}
	if sarLog.calls != 2 {
		t.Errorf("SAR calls = %d, want 2 (one per identity)", sarLog.calls)
	}
}

// TestCacheIsBounded verifies overflow eviction: the cache never grows past
// its bound and evicted entries cause fresh SAR calls (no unbounded memory,
// no silently stale verdicts).
func TestCacheIsBounded(t *testing.T) {
	gate, sarLog, clk := newGateFixture(t, Options{CacheMaxEntries: 2})
	ctx := context.Background()

	for _, name := range []string{"app-one", "app-two", "app-three"} {
		if v := gate.CheckTarget(ctx, userSubject(), deploymentTarget(name)); v.Verdict != VerdictAllowed {
			t.Fatalf("%s verdict = %+v, want Allowed", name, v)
		}
	}
	if got := gate.cache.len(); got > 2 {
		t.Fatalf("cache size = %d, want <= 2 (bounded)", got)
	}
	if sarLog.calls != 3 {
		t.Fatalf("SAR calls = %d, want 3 distinct targets", sarLog.calls)
	}

	// The oldest entry was evicted and is re-fetched.
	clk.Advance(time.Minute)
	if v := gate.CheckTarget(ctx, userSubject(), deploymentTarget("app-one")); v.Verdict != VerdictAllowed {
		t.Fatalf("evicted-entry verdict = %+v, want Allowed", v)
	}
	if sarLog.calls != 4 {
		t.Errorf("SAR calls = %d, want 4 (evicted entry must not be served stale)", sarLog.calls)
	}
}

// TestUnknownResultsAreNeverCachedNorFailOpen: a failing SAR call yields
// Unknown (callers must requeue, never write) and the next check hits the API
// again instead of serving a cached non-answer.
func TestUnknownResultsAreNeverCachedNorFailOpen(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})
	sarLog.createErr = errors.New("apiserver unavailable")
	ctx := context.Background()
	target := deploymentTarget("payments-api")

	v := gate.CheckTarget(ctx, userSubject(), target)
	if v.Verdict != VerdictUnknown {
		t.Fatalf("verdict = %+v, want Unknown on SAR failure", v)
	}
	if !strings.HasPrefix(v.Reason, ReasonSubjectAccessReviewFailed) {
		t.Errorf("reason = %q, want prefix %q", v.Reason, ReasonSubjectAccessReviewFailed)
	}

	sarLog.createErr = nil
	v = gate.CheckTarget(ctx, userSubject(), target)
	if v.Verdict != VerdictAllowed {
		t.Fatalf("retry verdict = %+v, want Allowed", v)
	}
	if sarLog.calls != 2 {
		t.Errorf("SAR calls = %d, want 2 (the failure must not be cached)", sarLog.calls)
	}
}

// TestEvaluationErrorIsUnknown covers an authorizer that answered with an
// evaluation error: no definitive verdict, nothing cached, fail closed.
func TestEvaluationErrorIsUnknown(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})
	sarLog.decide = func(sar *authorizationv1.SubjectAccessReview) {
		sar.Status.EvaluationError = "authorizing webhook timed out"
	}

	v := gate.CheckTarget(context.Background(), userSubject(), deploymentTarget("payments-api"))
	if v.Verdict != VerdictUnknown || !strings.Contains(v.Reason, "authorizing webhook timed out") {
		t.Fatalf("verdict = %+v, want Unknown carrying the evaluation error", v)
	}
	if got := gate.cache.len(); got != 0 {
		t.Errorf("cache size = %d, want 0 (evaluation errors are never cached)", got)
	}
}

// TestUnmappedKindIsKindNotInstalledWithoutSARCall: a target whose kind the
// cluster does not serve yields the stable KindNotInstalled unknown verdict
// and consumes neither an SAR call nor a limiter token (the subscription may
// legitimately predate its target CRD; the gate re-checks periodically).
func TestUnmappedKindIsKindNotInstalledWithoutSARCall(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{LimiterBurst: 1})
	thirdParty := imageflowv1alpha1.TargetReference{
		APIVersion: "example.io/v1",
		Kind:       "WebService",
		Namespace:  testNamespace,
		Name:       "payments-edge",
	}

	v := gate.CheckTarget(context.Background(), userSubject(), thirdParty)
	if v.Verdict != VerdictUnknown || !strings.HasPrefix(v.Reason, ReasonKindNotInstalled) {
		t.Fatalf("verdict = %+v, want Unknown/%s", v, ReasonKindNotInstalled)
	}
	if sarLog.calls != 0 {
		t.Fatalf("SAR calls = %d, want 0 for an unmapped kind", sarLog.calls)
	}

	// The single-token limiter bucket is untouched: a mapped target right
	// after the unmapped one still gets its SAR through.
	if v := gate.CheckTarget(context.Background(), userSubject(),
		deploymentTarget("payments-api")); v.Verdict != VerdictAllowed {
		t.Fatalf("mapped-target verdict = %+v, want Allowed (limiter token preserved)", v)
	}
}

// TestNilMapperFailsClosed: a gate built without a RESTMapper never answers
// Allowed (fail closed on misconfiguration).
func TestNilMapperFailsClosed(t *testing.T) {
	gate, _, _ := newGateFixture(t, Options{})
	gate.mapper = nil

	v := gate.CheckTarget(context.Background(), userSubject(), deploymentTarget("payments-api"))
	if v.Verdict != VerdictUnknown || !strings.HasPrefix(v.Reason, ReasonTargetMappingFailed) {
		t.Fatalf("verdict = %+v, want Unknown/%s", v, ReasonTargetMappingFailed)
	}
}

// TestRateLimitShedsExcessSARCalls is the named rate-limiting case
// (AC: 限流行为命名用例): with a one-token bucket the second distinct check
// is shed as Unknown/RateLimited without an API call, the injected clock
// refills the bucket, and cache hits never consume tokens.
func TestRateLimitShedsExcessSARCalls(t *testing.T) {
	gate, sarLog, clk := newGateFixture(t, Options{LimiterQPS: 1, LimiterBurst: 1})
	ctx := context.Background()
	first := deploymentTarget("burst-app-one")
	second := deploymentTarget("burst-app-two")

	if v := gate.CheckTarget(ctx, userSubject(), first); v.Verdict != VerdictAllowed {
		t.Fatalf("first verdict = %+v, want Allowed", v)
	}
	v := gate.CheckTarget(ctx, userSubject(), second)
	if v.Verdict != VerdictUnknown || !strings.HasPrefix(v.Reason, ReasonRateLimited) {
		t.Fatalf("shed verdict = %+v, want Unknown/%s", v, ReasonRateLimited)
	}
	if sarLog.calls != 1 {
		t.Fatalf("SAR calls = %d, want 1 (the shed check must not reach the API)", sarLog.calls)
	}

	// The shed check is not cached: after refill it goes through.
	clk.Advance(time.Second)
	if v := gate.CheckTarget(ctx, userSubject(), second); v.Verdict != VerdictAllowed {
		t.Fatalf("refilled verdict = %+v, want Allowed", v)
	}
	if sarLog.calls != 2 {
		t.Fatalf("SAR calls after refill = %d, want 2", sarLog.calls)
	}

	// Cache hits consume no tokens.
	for i := 0; i < 5; i++ {
		if v := gate.CheckTarget(ctx, userSubject(), first); v.Verdict != VerdictAllowed {
			t.Fatalf("cached verdict = %+v, want Allowed", v)
		}
	}
	if sarLog.calls != 2 {
		t.Errorf("SAR calls = %d, want 2 (five cache hits, zero API calls)", sarLog.calls)
	}
	if got := testutil.ToFloat64(gate.metrics.checks.WithLabelValues(verdictLabelRateLimited)); got != 1 {
		t.Errorf("rate_limited counter = %v, want 1", got)
	}
}

// TestCheckTargetsKeepsInputOrderWithMixedVerdicts covers the batch API the
// webhook and the controller consume: one verdict per target, input order.
func TestCheckTargetsKeepsInputOrderWithMixedVerdicts(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})
	sarLog.denyByName("billing")
	targets := []imageflowv1alpha1.TargetReference{
		deploymentTarget("payments-api"),
		deploymentTarget("billing"),
		{APIVersion: "example.io/v1", Kind: "WebService", Namespace: testNamespace, Name: "edge"},
	}

	verdicts := gate.CheckTargets(context.Background(), userSubject(), targets)

	if len(verdicts) != 3 {
		t.Fatalf("verdicts = %d, want 3", len(verdicts))
	}
	want := []Verdict{VerdictAllowed, VerdictDenied, VerdictUnknown}
	for i, w := range want {
		if verdicts[i].Verdict != w {
			t.Errorf("verdict[%d] = %+v, want %s", i, verdicts[i], w)
		}
		if verdicts[i].Target != targets[i] {
			t.Errorf("verdict[%d] target = %+v, want %+v", i, verdicts[i].Target, targets[i])
		}
	}
}

// TestVerdictMetricsCounters pins the metric exposure (AC: SAR 缓存命中率
// 指标暴露): allowed/denied checks and hit/miss lookups are all countable on
// the gate's collector set.
func TestVerdictMetricsCounters(t *testing.T) {
	gate, sarLog, _ := newGateFixture(t, Options{})
	ctx := context.Background()

	sarLog.denyByName("billing")
	gate.CheckTarget(ctx, userSubject(), deploymentTarget("payments-api")) // miss → allowed
	gate.CheckTarget(ctx, userSubject(), deploymentTarget("billing"))      // miss → denied
	gate.CheckTarget(ctx, userSubject(), deploymentTarget("payments-api")) // hit → allowed

	m := gate.metrics
	if got := testutil.ToFloat64(m.checks.WithLabelValues(verdictLabelAllowed)); got != 2 {
		t.Errorf("allowed checks = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.checks.WithLabelValues(verdictLabelDenied)); got != 1 {
		t.Errorf("denied checks = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.lookups.WithLabelValues(lookupResultMiss)); got != 2 {
		t.Errorf("misses = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.lookups.WithLabelValues(lookupResultHit)); got != 1 {
		t.Errorf("hits = %v, want 1 (hit rate = 1/3)", got)
	}
}
