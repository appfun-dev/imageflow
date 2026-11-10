package clock

import (
	"testing"
	"time"

	. "github.com/allure-framework/allure-go/commons/gotest"
	agt "github.com/allure-framework/allure-go/commons/gotest"
	. "github.com/appfun-dev/imageflow/test/utils/should_dsl"
)

// TestFakeClockAdvancesDeterministically verifies Fake clock provides
// deterministic time advancement for unit tests.
//
// This test serves as the canonical example for Allure-annotated unit tests
// in ImageFlow. See TESTING.md for the full annotation schema.
func TestFakeClockAdvancesDeterministically(t *testing.T) {
	agt.Test(t, "Fake clock advances deterministically",
		func(a *agt.Context) {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

			a.Step("Create Fake clock with fixed start time", func(ctx *agt.Context) {
				ctx.StepDescription("NewFake initializes internal time to the provided value")
			})
			clk := NewFake(start)

			a.Step("Verify initial time equals start", func(ctx *agt.Context) {
				ctx.StepDescription("Now() should return exactly the construction parameter")
				AExpect(a, clk.Now()).Should(Equal(start),
					"initial time must equal construction parameter")
			})

			a.Step("Advance by 5 minutes", func(ctx *agt.Context) {
				ctx.StepDescription("Advance adds duration to internal time")
				clk.Advance(5 * time.Minute)
				want := start.Add(5 * time.Minute)
				AExpect(a, clk.Now()).Should(Equal(want),
					"time after Advance(5m) must equal start+5m")
			})

			a.Step("Verify Since computes elapsed duration", func(ctx *agt.Context) {
				ctx.StepDescription("Since returns duration between reference and current time")
				before := clk.Now()
				clk.Advance(time.Hour)
				AExpect(a, clk.Since(before)).Should(Equal(time.Hour),
					"Since must return exactly the advanced duration")
			})
		},
		// ─── Traceability ──────────────────────────────────────────
		WithAllureID("CLOCK-001"),
		WithTMS("CLOCK-001", "https://github.com/appfun-dev/imageflow/issues/CLOCK-001"),
		WithLabel("userstory", "US-INFRA-1"),
		WithLabel("scenario", "deterministic time advancement"),
		WithLabel("component", "clock"),
		WithLabel("layer", "unit"),
		// ─── Hierarchy ─────────────────────────────────────────────
		WithEpic("Infrastructure: Test Seams"),
		WithFeature("ADR-009 Testing Strategy"),
		WithStory("US-INFRA-1"),
		// ─── Documentation Links ───────────────────────────────────
		WithLink("https://github.com/appfun-dev/imageflow/blob/main/docs/adr/ADR-009-testing-strategy.md", "ADR-009", "docs"),
		WithLink("https://github.com/appfun-dev/imageflow/blob/main/TESTING.md", "TESTING.md", "docs"),
		// ─── Suite Classification ──────────────────────────────────
		WithParentSuite("unit"),
		WithSuite("Clock"),
		// ─── Description ───────────────────────────────────────────
		WithDescription(`Verifies that Fake clock provides deterministic time control for unit tests.

Test cases:
1. Initial time equals construction parameter
2. Advance() correctly increments internal time
3. Since() computes elapsed duration from a reference point

This is a foundational test seam used throughout ImageFlow to avoid
flaky time-dependent tests. Production code uses Real clock; tests
inject Fake for deterministic behavior.`),
	)
}

// TestFakeClockConcurrencySafety verifies Fake clock is safe for concurrent access.
func TestFakeClockConcurrencySafety(t *testing.T) {
	agt.Test(t, "Fake clock is safe for concurrent read/write",
		func(a *agt.Context) {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clk := NewFake(start)

			a.Step("Spawn concurrent readers and writers", func(ctx *agt.Context) {
				ctx.StepDescription("10 goroutines read, 10 goroutines write simultaneously")
				done := make(chan struct{})
				const iterations = 100

				// Concurrent readers
				for i := 0; i < 10; i++ {
					go func() {
						for j := 0; j < iterations; j++ {
							_ = clk.Now()
							_ = clk.Since(start)
						}
						done <- struct{}{}
					}()
				}

				// Concurrent writers
				for i := 0; i < 10; i++ {
					go func() {
						for j := 0; j < iterations; j++ {
							clk.Advance(time.Millisecond)
						}
						done <- struct{}{}
					}()
				}

				// Wait for all goroutines
				for i := 0; i < 20; i++ {
					<-done
				}
			})

			a.Step("Verify final time is consistent", func(ctx *agt.Context) {
				ctx.StepDescription("After 10 writers × 100ms each, time should advance by 1 second")
				expected := start.Add(10 * 100 * time.Millisecond)
				AExpect(a, clk.Now()).Should(BeTemporally(">=", expected),
					"final time must reflect all concurrent advances")
			})
		},
		WithAllureID("CLOCK-002"),
		WithTMS("CLOCK-002", "https://github.com/appfun-dev/imageflow/issues/CLOCK-002"),
		WithLabel("userstory", "US-INFRA-1"),
		WithLabel("scenario", "concurrent access safety"),
		WithLabel("component", "clock"),
		WithLabel("layer", "unit"),
		WithEpic("Infrastructure: Test Seams"),
		WithFeature("ADR-009 Testing Strategy"),
		WithStory("US-INFRA-1"),
		WithParentSuite("unit"),
		WithSuite("Clock"),
		WithDescription(`Verifies that Fake clock's RWMutex protects concurrent Now()/Advance() calls.
Spawns multiple goroutines performing simultaneous reads and writes to ensure
no data races occur under contention.`),
	)
}

// TestRealClockReturnsSystemTime verifies Real clock delegates to system time.
func TestRealClockReturnsSystemTime(t *testing.T) {
	agt.Test(t, "Real clock returns system time",
		func(a *agt.Context) {
			real := Real{}

			a.Step("Now() returns time within acceptable window", func(ctx *agt.Context) {
				ctx.StepDescription("Real.Now() should be within 1 second of time.Now()")
				before := time.Now()
				got := real.Now()
				after := time.Now()

				AExpect(a, got).Should(BeTemporally(">=", before),
					"Real.Now() must not return time before call")
				AExpect(a, got).Should(BeTemporally("<=", after),
					"Real.Now() must not return time after call")
			})

			a.Step("Since() computes elapsed duration", func(ctx *agt.Context) {
				ctx.StepDescription("Real.Since() should return positive duration for past time")
				past := time.Now().Add(-100 * time.Millisecond)
				AEventually(a, func() bool {
					return real.Since(past) >= 100*time.Millisecond
				}).WithTimeout(1 * time.Second).Should(BeTrue(),
					"Since must return at least the elapsed duration")
			})
		},
		WithAllureID("CLOCK-003"),
		WithTMS("CLOCK-003", "https://github.com/appfun-dev/imageflow/issues/CLOCK-003"),
		WithLabel("userstory", "US-INFRA-1"),
		WithLabel("scenario", "real clock delegation"),
		WithLabel("component", "clock"),
		WithLabel("layer", "unit"),
		WithEpic("Infrastructure: Test Seams"),
		WithFeature("ADR-009 Testing Strategy"),
		WithStory("US-INFRA-1"),
		WithParentSuite("unit"),
		WithSuite("Clock"),
		WithDescription(`Verifies that Real clock correctly delegates to time.Now() and time.Since().
Uses Eventually to account for potential clock drift in CI environments.`),
	)
}
