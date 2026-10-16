package clock

import (
	"testing"
	"time"
)

func TestFakeClockAdvancesDeterministically(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := NewFake(start)

	if got := clk.Now(); !got.Equal(start) {
		t.Fatalf("expected %v, got %v", start, got)
	}

	clk.Advance(5 * time.Minute)
	want := start.Add(5 * time.Minute)
	if got := clk.Now(); !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}

	before := clk.Now()
	clk.Advance(time.Hour)
	if got := clk.Since(before); got != time.Hour {
		t.Fatalf("expected Since=%v, got %v", time.Hour, got)
	}
}
