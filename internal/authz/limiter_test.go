package authz

import (
	"testing"
	"time"

	"github.com/appfun-dev/imageflow/internal/clock"
)

// Token-bucket unit tests with the injected clock (seam S3): refill rate,
// burst capacity cap and monotonic-time safety.

func TestTokenBucketRefillsAtQPS(t *testing.T) {
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFake(start)
	b := newTokenBucket(clk, 2, 1) // 2 tokens/second, capacity 1

	if !b.take(clk.Now()) {
		t.Fatal("the first token of a full bucket must be available")
	}
	if b.take(clk.Now()) {
		t.Fatal("an empty bucket must shed immediately (non-blocking)")
	}

	clk.Advance(400 * time.Millisecond) // 0.8 tokens: still short
	if b.take(clk.Now()) {
		t.Fatal("0.8 refilled tokens must not grant a request")
	}

	clk.Advance(150 * time.Millisecond) // 1.1 tokens accumulated
	if !b.take(clk.Now()) {
		t.Fatal("a fully refilled token must grant the request")
	}
	if b.take(clk.Now()) {
		t.Fatal("the refilled token must be consumed exactly once")
	}
}

func TestTokenBucketNeverExceedsBurst(t *testing.T) {
	start := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFake(start)
	b := newTokenBucket(clk, 10, 3)

	clk.Advance(time.Hour) // idle far beyond burst capacity
	granted := 0
	for i := 0; i < 10; i++ {
		if b.take(clk.Now()) {
			granted++
		}
	}
	if granted != 3 {
		t.Fatalf("granted = %d, want the burst capacity of 3", granted)
	}
}

func TestTokenBucketIgnoresClockGoingBackwards(t *testing.T) {
	start := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFake(start)
	b := newTokenBucket(clk, 1, 1)

	if !b.take(clk.Now()) {
		t.Fatal("bucket starts full")
	}
	// A clock adjustment backwards must neither refill nor corrupt state.
	earlier := start.Add(-time.Hour)
	if b.take(earlier) {
		t.Fatal("an empty bucket must stay empty when the clock goes backwards")
	}
	clk.Advance(time.Second)
	if !b.take(clk.Now()) {
		t.Fatal("normal refill must resume after a backwards observation")
	}
}
