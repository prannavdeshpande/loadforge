package engine

import (
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Histogram range and precision mandated by CLAUDE.md: 1µs to 60s at three
// significant digits. Values are recorded in microseconds.
const (
	HistMinMicros = 1
	HistMaxMicros = 60_000_000
	HistSigFigs   = 3
)

// NewHistogram returns an empty histogram with the standard LoadForge range.
func NewHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(HistMinMicros, HistMaxMicros, HistSigFigs)
}

// record adds d to h in microseconds. A value above 60s is clamped to 60s and
// reported through the return value so the caller can count it; it is never
// dropped. Negative durations record as zero.
func record(h *hdrhistogram.Histogram, d time.Duration) (clamped bool) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	if us > HistMaxMicros {
		us, clamped = HistMaxMicros, true
	}
	_ = h.RecordValue(us) // cannot fail: us is within the trackable range
	return clamped
}
