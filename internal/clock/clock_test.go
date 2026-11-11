// Package clock provides test seams for time-dependent code.
//
// This file contains Allure-annotated unit tests serving as the canonical
// example for ImageFlow testing conventions. See TESTING.md for the full
// annotation schema and CODING_STANDARDS.md for coding standards.
//
// Test Structure Convention:
// Each test function follows a three-part structure:
//   1. metadata := []ao.Option{...}     — Allure traceability and hierarchy
//   2. const (...)                      — Key test parameters (if any)
//   3. agt.Test(t, name, body, metadata...)  — Test execution
//
// Documentation lives in metadata.Description, not method comments.
// Method comments are limited to a single godoc line for go doc compatibility.
package clock

import (
	"testing"
	"time"

	agt "github.com/allure-framework/allure-go/commons/gotest"

	ao "github.com/appfun-dev/imageflow/test/utils/allure/options"

	. "github.com/appfun-dev/imageflow/test/utils/should_dsl"
)

// ════════════════════════════════════════════════════════════════════════════
// Test: Fake Clock Deterministic Advancement
// ════════════════════════════════════════════════════════════════════════════

// TestFakeClockAdvancesDeterministically verifies Fake clock deterministic time advancement.
func TestFakeClockAdvancesDeterministically(t *testing.T) {
	metadata := []ao.Option{
		// Traceability
		ao.AllureID("CLOCK-001"),
		ao.TMS("CLOCK-001", "https://github.com/appfun-dev/imageflow/issues/CLOCK-001"),
		ao.Label("userstory", "US-INFRA-1"),
		ao.Label("scenario", "deterministic time advancement"),
		ao.Label("component", "clock"),
		ao.Label("layer", "unit"),
		// Hierarchy
		ao.Epic("Infrastructure: Test Seams"),
		ao.Feature("ADR-009 Testing Strategy"),
		ao.Story("US-INFRA-1"),
		// Documentation Links
		ao.Link("https://github.com/appfun-dev/imageflow/blob/main/docs/adr/ADR-009-testing-strategy.md", "ADR-009", "docs"),
		ao.Link("https://github.com/appfun-dev/imageflow/blob/main/TESTING.md", "TESTING.md", "docs"),
		// Suite Classification
		ao.ParentSuite("unit"),
		ao.Suite("Clock"),
		// Description
		ao.Description(`Verifies that Fake clock provides deterministic time control for unit tests.

This test serves as the canonical example for Allure-annotated unit tests
in ImageFlow. See TESTING.md for the full annotation schema.

Test cases:
1. Initial time equals construction parameter
2. Advance() correctly increments internal time
3. Since() computes elapsed duration from a reference point

This is a foundational test seam used throughout ImageFlow to avoid
flaky time-dependent tests. Production code uses Real clock; tests
inject Fake for deterministic behavior.`),
	}

	// test parameters
	const (
		startTime        = "2026-01-01T00:00:00Z"
		advanceDuration  = 5 * time.Minute
		sinceTestAdvance = time.Hour
	)

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

			a.Step("Advance by configured duration", func(ctx *agt.Context) {
				ctx.StepDescription("Advance adds duration to internal time")
				clk.Advance(advanceDuration)
				want := start.Add(advanceDuration)
				AExpect(a, clk.Now()).Should(Equal(want),
					"time after Advance must equal start+duration")
			})

			a.Step("Verify Since computes elapsed duration", func(ctx *agt.Context) {
				ctx.StepDescription("Since returns duration between reference and current time")
				before := clk.Now()
				clk.Advance(sinceTestAdvance)
				AExpect(a, clk.Since(before)).Should(Equal(sinceTestAdvance),
					"Since must return exactly the advanced duration")
			})
		},
		metadata...,
	)
}

// ════════════════════════════════════════════════════════════════════════════
// Test: Fake Clock Concurrency Safety
// ════════════════════════════════════════════════════════════════════════════

// TestFakeClockConcurrencySafety verifies Fake clock concurrent access safety.
func TestFakeClockConcurrencySafety(t *testing.T) {
	metadata := []ao.Option{
		// Traceability
		ao.AllureID("CLOCK-002"),
		ao.TMS("CLOCK-002", "https://github.com/appfun-dev/imageflow/issues/CLOCK-002"),
		ao.Label("userstory", "US-INFRA-1"),
		ao.Label("scenario", "concurrent access safety"),
		ao.Label("component", "clock"),
		ao.Label("layer", "unit"),
		// Hierarchy
		ao.Epic("Infrastructure: Test Seams"),
		ao.Feature("ADR-009 Testing Strategy"),
		ao.Story("US-INFRA-1"),
		// Suite Classification
		ao.ParentSuite("unit"),
		ao.Suite("Clock"),
		// Description
		ao.Description(`Verifies that Fake clock's RWMutex protects concurrent Now()/Advance() calls.

Spawns multiple goroutines performing simultaneous reads (Now/Since) and
writes (Advance) to ensure no data races occur under contention.

Configuration:
- 10 reader goroutines + 10 writer goroutines
- Each performs 100 iterations
- Writers advance by 1ms per iteration

Run with -race flag to detect race conditions:
  go test -race ./internal/clock/...`),
	}

	// test parameters
	const (
		goroutineCount = 10
		iterations     = 100
		advancePerIter = time.Millisecond
	)

	agt.Test(t, "Fake clock is safe for concurrent read/write",
		func(a *agt.Context) {
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			clk := NewFake(start)

			a.Step("Spawn concurrent readers and writers", func(ctx *agt.Context) {
				ctx.StepDescription("10 goroutines read, 10 goroutines write simultaneously")
				done := make(chan struct{})

				// Concurrent readers: exercise Now() and Since() under contention
				for i := 0; i < goroutineCount; i++ {
					go func() {
						for j := 0; j < iterations; j++ {
							_ = clk.Now()
							_ = clk.Since(start)
						}
						done <- struct{}{}
					}()
				}

				// Concurrent writers: exercise Advance() under contention
				for i := 0; i < goroutineCount; i++ {
					go func() {
						for j := 0; j < iterations; j++ {
							clk.Advance(advancePerIter)
						}
						done <- struct{}{}
					}()
				}

				// Wait for all goroutines to complete
				for i := 0; i < goroutineCount*2; i++ {
					<-done
				}
			})

			a.Step("Verify final time reflects all advances", func(ctx *agt.Context) {
				expected := start.Add(goroutineCount * iterations * advancePerIter)
				ctx.StepDescription("After 10 writers × 100ms each, time should advance by 1 second")
				AExpect(a, clk.Now()).Should(BeTemporally(">=", expected),
					"final time must reflect all concurrent advances")
			})
		},
		metadata...,
	)
}

// ════════════════════════════════════════════════════════════════════════════
// Test: Real Clock System Time Delegation
// ════════════════════════════════════════════════════════════════════════════

// TestRealClockReturnsSystemTime verifies Real clock system time delegation.
func TestRealClockReturnsSystemTime(t *testing.T) {
	metadata := []ao.Option{
		// Traceability
		ao.AllureID("CLOCK-003"),
		ao.TMS("CLOCK-003", "https://github.com/appfun-dev/imageflow/issues/CLOCK-003"),
		ao.Label("userstory", "US-INFRA-1"),
		ao.Label("scenario", "real clock delegation"),
		ao.Label("component", "clock"),
		ao.Label("layer", "unit"),
		// Hierarchy
		ao.Epic("Infrastructure: Test Seams"),
		ao.Feature("ADR-009 Testing Strategy"),
		ao.Story("US-INFRA-1"),
		// Suite Classification
		ao.ParentSuite("unit"),
		ao.Suite("Clock"),
		// Description
		ao.Description(`Verifies that Real clock correctly delegates to time.Now() and time.Since().

Uses Eventually for Since() assertion to account for potential clock drift
in CI environments where system time may not be perfectly monotonic.

Assertions:
- Now() must return a time within the call window [before, after]
- Since() must return at least the elapsed duration`),
	}

	// test parameters
	const (
		elapsedThreshold  = 100 * time.Millisecond
		eventuallyTimeout = 1 * time.Second
	)

	agt.Test(t, "Real clock returns system time",
		func(a *agt.Context) {
			realClk := Real{}

			a.Step("Now() returns time within acceptable window", func(ctx *agt.Context) {
				ctx.StepDescription("Real.Now() should be within [before, after] of time.Now()")
				before := time.Now()
				got := realClk.Now()
				after := time.Now()

				AExpect(a, got).Should(BeTemporally(">=", before),
					"Real.Now() must not return time before call")
				AExpect(a, got).Should(BeTemporally("<=", after),
					"Real.Now() must not return time after call")
			})

			a.Step("Since() computes elapsed duration", func(ctx *agt.Context) {
				ctx.StepDescription("Real.Since() should return at least the elapsed duration")
				past := time.Now().Add(-elapsedThreshold)
				AEventually(a, func() bool {
					return realClk.Since(past) >= elapsedThreshold
				}).WithTimeout(eventuallyTimeout).Should(BeTrue(),
					"Since must return at least the elapsed duration")
			})
		},
		metadata...,
	)
}
