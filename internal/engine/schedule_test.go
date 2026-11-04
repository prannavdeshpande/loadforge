package engine

import (
	"math"
	"testing"
	"time"
)

func TestScheduleAt(t *testing.T) {
	t0 := time.Now()
	cases := []struct {
		rate float64
		i    uint64
		want time.Duration
	}{
		{1, 0, 0},
		{1, 7, 7 * time.Second},
		{3, 1, 333_333_333 * time.Nanosecond},
		{3, 2, 666_666_667 * time.Nanosecond}, // rounds to nearest ns
		{3, 3, time.Second},
		{100_000, 1, 10 * time.Microsecond},
		{0.5, 3, 6 * time.Second},
		// No drift after a day of scheduling: computed from i, not accumulated.
		{3, 3 * 86_400, 86_400 * time.Second},
		{100_000, 100_000 * 86_400, 86_400 * time.Second},
		{7.123, 7123 * 3600, 3600 * 1000 * time.Second},
	}
	for _, c := range cases {
		s, err := NewSchedule(t0, c.rate)
		if err != nil {
			t.Fatalf("NewSchedule(%v): %v", c.rate, err)
		}
		if got := s.At(c.i).Sub(t0); got != c.want {
			t.Errorf("rate %v: At(%d) − t0 = %v, want %v", c.rate, c.i, got, c.want)
		}
	}
}

func TestScheduleSaturates(t *testing.T) {
	t0 := time.Now()
	s, err := NewSchedule(t0, 0.001)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []uint64{1 << 40, math.MaxUint64} {
		if got := s.At(i).Sub(t0); got != math.MaxInt64 {
			t.Errorf("At(%d) − t0 = %v, want saturated at MaxInt64", i, got)
		}
	}
}

func TestScheduleRejectsBadRates(t *testing.T) {
	for _, r := range []float64{0, -1, 0.0001} {
		if _, err := NewSchedule(time.Now(), r); err == nil {
			t.Errorf("NewSchedule(rate=%v) succeeded, want error", r)
		}
	}
}
