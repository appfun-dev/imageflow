// Package should_dsl provides Allure-aware Gomega assertion primitives.
//
// This package wraps gomega.Expect, gomega.Eventually, and gomega.Consistently
// to bind failure reporting to the Allure test context. All assertions report
// through the Allure context's FailNow method, ensuring proper test lifecycle
// integration with Allure reports.
//
// Usage:
//
//	import (
//	    . "github.com/appfun-dev/imageflow/test/utils/should_dsl"
//	)
//
//	allure.Test(t, "example", func(a *allure.Context) {
//	    AExpect(a, value).Should(Equal("expected"))
//	    AEventually(a, asyncFn).WithTimeout(5*time.Second).Should(BeTrue())
//	})
package should_dsl

import (
	"fmt"
	"runtime"
	"time"

	allure "github.com/allure-framework/allure-go/commons/gotest"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
)

// ─── Core Adapter ────────────────────────────────────────────────

// allureTB adapts *allure.Context to the TestingT interface that
// gomega.NewWithT requires. Every AExpect/AEventually call creates
// its own instance, so there is zero global state and full concurrency safety.
type allureTB struct {
	ctx *allure.Context
}

func (t *allureTB) Helper() {}

func (t *allureTB) Fatalf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	// Skip frames: Fatalf → gomega internal → caller's assertion line
	_, file, line, ok := runtime.Caller(2)
	if ok {
		msg = fmt.Sprintf("%s:%d: %s", file, line, msg)
	}
	// allure.Context.Fatalf accepts format string; FailNow() takes no args
	t.ctx.Fatalf("%s", msg)
}

// newG creates a per-call Gomega instance bound to the given Allure context.
func newG(a *allure.Context) gomega.Gomega {
	return gomega.NewWithT(&allureTB{ctx: a})
}

// ─── Assertion Entry Points ──────────────────────────────────────

// AExpect is the Allure-aware equivalent of gomega.Expect().
// Usage: AExpect(a, actual).Should(Equal("alice"))
func AExpect(a *allure.Context, actual interface{}, extra ...interface{}) gomega.Assertion {
	return newG(a).Expect(actual, extra...)
}

// AΩ is an alias for AExpect, mirroring gomega.Ω().
func AΩ(a *allure.Context, actual interface{}, extra ...interface{}) gomega.Assertion {
	return AExpect(a, actual, extra...)
}

// ─── Async Assertions ────────────────────────────────────────────

// AEventually mirrors gomega.Eventually(), bound to the Allure context.
// Supports all Gomega Eventually signatures:
//
//	AEventually(a, func() string { return getName() }).Should(Equal("alice"))
//	AEventually(a, ch).Should(Receive())
//	AEventually(a, actual).WithTimeout(5*time.Second).Should(...)
func AEventually(a *allure.Context, actualOrCtx interface{}, args ...interface{}) gomega.AsyncAssertion {
	return newG(a).Eventually(actualOrCtx, args...)
}

// AConsistently mirrors gomega.Consistently(), bound to the Allure context.
func AConsistently(a *allure.Context, actualOrCtx interface{}, args ...interface{}) gomega.AsyncAssertion {
	return newG(a).Consistently(actualOrCtx, args...)
}

// ─── Matcher Constructors (pass-through, no context needed) ─────
// These don't need Allure binding — they're pure value constructors.
// Re-exported here so callers can import one package instead of two.

var (
	Equal                = gomega.Equal
	BeEquivalentTo       = gomega.BeEquivalentTo
	BeIdenticalTo        = gomega.BeIdenticalTo
	BeNil                = gomega.BeNil
	BeTrue               = gomega.BeTrue
	BeFalse              = gomega.BeFalse
	HaveOccurred         = gomega.HaveOccurred
	Succeed              = gomega.Succeed
	MatchError           = gomega.MatchError
	ContainSubstring     = gomega.ContainSubstring
	HavePrefix           = gomega.HavePrefix
	HaveSuffix           = gomega.HaveSuffix
	MatchRegexp          = gomega.MatchRegexp
	HaveLen              = gomega.HaveLen
	HaveCap              = gomega.HaveCap
	BeEmpty              = gomega.BeEmpty
	ContainElement       = gomega.ContainElement
	ConsistOf            = gomega.ConsistOf
	HaveKey              = gomega.HaveKey
	HaveKeyWithValue     = gomega.HaveKeyWithValue
	BeNumerically        = gomega.BeNumerically
	BeTemporally         = gomega.BeTemporally
	BeAssignableToTypeOf = gomega.BeAssignableToTypeOf
	SatisfyAll           = gomega.SatisfyAll
	SatisfyAny           = gomega.SatisfyAny
	Not                  = gomega.Not
	And                  = gomega.And
	Or                   = gomega.Or
	WithTransform        = gomega.WithTransform
	Receive              = gomega.Receive
	BeSent               = gomega.BeSent
	MatchJSON            = gomega.MatchJSON
	MatchXML             = gomega.MatchXML
	MatchYAML            = gomega.MatchYAML
)

// ─── Custom Matcher Helper ───────────────────────────────────────

// AComposeMatcher lets you build custom matchers that still report
// through the Allure context when used with AExpect.
func AComposeMatcher(description string, match func(actual interface{}) (bool, error)) types.GomegaMatcher {
	return &customMatcher{desc: description, fn: match}
}

type customMatcher struct {
	desc string
	fn   func(interface{}) (bool, error)
}

func (m *customMatcher) Match(actual interface{}) (bool, error) { return m.fn(actual) }
func (m *customMatcher) FailureMessage(actual interface{}) string {
	return fmt.Sprintf("Expected %s", m.desc)
}
func (m *customMatcher) NegatedFailureMessage(actual interface{}) string {
	return fmt.Sprintf("Unexpected %s", m.desc)
}

// Ensure time is imported for async assertion signatures.
var _ = time.Second
