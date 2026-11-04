package engine

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistogramRangeAndClamp(t *testing.T) {
	h := NewHistogram()
	for _, d := range []time.Duration{time.Microsecond, 60 * time.Second} {
		if record(h, d) {
			t.Errorf("record(%v) clamped, want in range", d)
		}
	}
	if got := h.Min(); got != 1 {
		t.Errorf("min = %dµs, want 1", got)
	}
	if got := h.Max(); !h.ValuesAreEquivalent(got, HistMaxMicros) {
		t.Errorf("max = %dµs, want equivalent to %d", got, HistMaxMicros)
	}

	// Three significant digits: 1.234s must come back within 0.1%.
	h.Reset()
	record(h, 1234*time.Millisecond)
	if got := h.Max(); got < 1_233_000 || got > 1_235_000 {
		t.Errorf("1.234s recorded as %dµs, want within 3 significant digits", got)
	}

	// Above range: clamped, counted by the caller, never dropped.
	h.Reset()
	if !record(h, 61*time.Second) {
		t.Error("record(61s) did not report clamping")
	}
	if h.TotalCount() != 1 || !h.ValuesAreEquivalent(h.Max(), HistMaxMicros) {
		t.Errorf("61s: count=%d max=%dµs, want 1 value at 60s", h.TotalCount(), h.Max())
	}
}

func TestRotateLosesNothingUnderConcurrency(t *testing.T) {
	const writers, perWriter = 8, 20_000
	r := newRecorder(4)
	r.start(time.Now())

	var (
		wg      sync.WaitGroup
		stop    atomic.Bool
		rotated int64
	)
	rotDone := make(chan struct{})
	go func() {
		defer close(rotDone)
		for !stop.Load() {
			iv, _ := r.rotate(time.Now())
			rotated += iv.Latency.TotalCount()
		}
	}()
	for w := range writers {
		wg.Add(1)
		go func(s *shard) {
			defer wg.Done()
			for range perWriter {
				s.mu.Lock()
				record(s.cur.Latency, time.Millisecond)
				s.cur.OK++
				s.mu.Unlock()
			}
		}(r.exec[w%len(r.exec)])
	}
	wg.Wait()
	stop.Store(true)
	<-rotDone
	rotated += r.finish(time.Now()).Latency.TotalCount()

	if rotated != writers*perWriter {
		t.Errorf("intervals hold %d values, want %d", rotated, writers*perWriter)
	}
	tot := r.totals()
	if tot.Latency.TotalCount() != writers*perWriter || tot.OK != writers*perWriter {
		t.Errorf("totals: count=%d ok=%d, want %d", tot.Latency.TotalCount(), tot.OK, writers*perWriter)
	}
}
