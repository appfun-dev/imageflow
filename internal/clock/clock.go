package clock

import (
	"sync"
	"time"
)

// Clock is a test seam for time. Production code uses Real; tests use Fake.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	NewTimer(d time.Duration) *time.Timer
}

// Real uses the system clock.
type Real struct{}

func (Real) Now() time.Time                       { return time.Now() }
func (Real) Since(t time.Time) time.Duration      { return time.Since(t) }
func (Real) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }

// Fake provides deterministic time advancement for tests.
type Fake struct {
	mu  sync.RWMutex
	now time.Time
}

func NewFake(t time.Time) *Fake {
	return &Fake{now: t}
}

func (f *Fake) Now() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.now
}

func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func (f *Fake) Since(t time.Time) time.Duration {
	return f.Now().Sub(t)
}

func (f *Fake) NewTimer(d time.Duration) *time.Timer {
	panic("Fake.NewTimer is not supported; use Advance in tests")
}
