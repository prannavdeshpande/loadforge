package health

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// Thresholds are the verdict bands of design.md §5. A run may override any
// of them in runs.thresholds; the values actually used are stored with the
// verdict.
//
// Two VALID bounds are fixed by definition and cannot be set: a VALID run
// dropped nothing and lost no snapshot. Only their DEGRADED bounds move.
type Thresholds struct {
	// Dispatch-lag p99, microseconds: VALID ≤ LagP99ValidUS, DEGRADED ≤
	// LagP99DegradedUS, else INVALID.
	LagP99ValidUS    int64 `json:"lag_p99_valid_us"`
	LagP99DegradedUS int64 `json:"lag_p99_degraded_us"`

	// Achieved ÷ target rate: VALID ≥ RateValid, DEGRADED ≥ RateDegraded.
	RateValid    float64 `json:"rate_valid"`
	RateDegraded float64 `json:"rate_degraded"`

	// generator_dropped ÷ intended: VALID = 0, DEGRADED ≤ DroppedDegraded.
	DroppedDegraded float64 `json:"dropped_degraded"`

	// Received ÷ expected snapshots: VALID = 1, DEGRADED ≥
	// CompletenessDegraded.
	CompletenessDegraded float64 `json:"completeness_degraded"`

	// Worker CPU p95, percent: VALID ≤ CPUValid, DEGRADED ≤ CPUDegraded.
	CPUValid    float64 `json:"cpu_valid"`
	CPUDegraded float64 `json:"cpu_degraded"`

	// Clock offset spread across workers (max − min), nanoseconds.
	ClockSpreadValidNS    int64 `json:"clock_spread_valid_ns"`
	ClockSpreadDegradedNS int64 `json:"clock_spread_degraded_ns"`
}

// DefaultThresholds are the bands in design.md §5.
func DefaultThresholds() Thresholds {
	return Thresholds{
		LagP99ValidUS:         1_000,
		LagP99DegradedUS:      10_000,
		RateValid:             0.99,
		RateDegraded:          0.95,
		DroppedDegraded:       0.001,
		CompletenessDegraded:  0.99,
		CPUValid:              70,
		CPUDegraded:           85,
		ClockSpreadValidNS:    1_000_000,
		ClockSpreadDegradedNS: 5_000_000,
	}
}

// ParseThresholds applies a run's overrides (a JSON object, possibly empty)
// to the defaults. An unknown key is an error rather than ignored, so a
// misspelt override can't silently leave a default in place.
func ParseThresholds(raw json.RawMessage) (Thresholds, error) {
	t := DefaultThresholds()
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return t, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&t); err != nil {
		return Thresholds{}, fmt.Errorf("health: thresholds: %w", err)
	}
	if dec.More() {
		return Thresholds{}, errors.New("health: thresholds: trailing data")
	}
	if err := t.Validate(); err != nil {
		return Thresholds{}, err
	}
	return t, nil
}

// Validate checks that every band is well formed: each VALID bound is at
// least as strict as its DEGRADED bound, and ratios and percentages are in
// range.
func (t Thresholds) Validate() error {
	var errs []error
	check := func(ok bool, format string, args ...any) {
		if !ok {
			errs = append(errs, fmt.Errorf("health: thresholds: "+format, args...))
		}
	}
	finite := func(vs ...float64) bool {
		for _, v := range vs {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return false
			}
		}
		return true
	}
	check(finite(t.RateValid, t.RateDegraded, t.DroppedDegraded, t.CompletenessDegraded, t.CPUValid, t.CPUDegraded),
		"values must be finite")
	check(t.LagP99ValidUS >= 0 && t.LagP99ValidUS <= t.LagP99DegradedUS,
		"lag p99: need 0 ≤ valid (%d) ≤ degraded (%d)", t.LagP99ValidUS, t.LagP99DegradedUS)
	check(t.RateDegraded >= 0 && t.RateDegraded <= t.RateValid && t.RateValid <= 1,
		"rate: need 0 ≤ degraded (%v) ≤ valid (%v) ≤ 1", t.RateDegraded, t.RateValid)
	check(t.DroppedDegraded >= 0 && t.DroppedDegraded <= 1,
		"dropped: need 0 ≤ degraded (%v) ≤ 1", t.DroppedDegraded)
	check(t.CompletenessDegraded >= 0 && t.CompletenessDegraded <= 1,
		"completeness: need 0 ≤ degraded (%v) ≤ 1", t.CompletenessDegraded)
	check(t.CPUValid >= 0 && t.CPUValid <= t.CPUDegraded && t.CPUDegraded <= 100,
		"cpu: need 0 ≤ valid (%v) ≤ degraded (%v) ≤ 100", t.CPUValid, t.CPUDegraded)
	check(t.ClockSpreadValidNS >= 0 && t.ClockSpreadValidNS <= t.ClockSpreadDegradedNS,
		"clock spread: need 0 ≤ valid (%d) ≤ degraded (%d)", t.ClockSpreadValidNS, t.ClockSpreadDegradedNS)
	return errors.Join(errs...)
}
