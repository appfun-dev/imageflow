package authz

import (
	"math"
	"sync"
	"time"

	"github.com/appfun-dev/imageflow/internal/clock"
)

// tokenBucket is a non-blocking token-bucket rate limiter for SAR creation
// (ADR-003: SAR call volume must stay bounded). It refills from the injected
// clock, so tests advance time deterministically instead of sleeping.
//
// Semantics: take() consumes one token and reports true, or reports false
// without blocking when the bucket is empty. A rejected take surfaces to
// callers as VerdictUnknown/ReasonRateLimited — the gate never fails open and
// never parks a request goroutine (admission has a hard timeout; reconcile
// requeues with backoff instead).
type tokenBucket struct {
	mu     sync.Mutex
	clk    clock.Clock
	qps    float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(clk clock.Clock, qps float64, burst int) *tokenBucket {
	return &tokenBucket{
		clk:    clk,
		qps:    qps,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   clk.Now(),
	}
}

// take consumes one token if available, refilling first from the elapsed
// injected-clock time (capped at burst).
func (b *tokenBucket) take(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(b.burst, b.tokens+elapsed.Seconds()*b.qps)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
