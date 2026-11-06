package health

import (
	"encoding/json"
	"fmt"
)

// Signal names, as they appear in verdict_detail.
const (
	SignalAborted           = "aborted"
	SignalLagP99            = "dispatch_lag_p99"
	SignalAchievedRate      = "achieved_rate"
	SignalDropped           = "generator_dropped"
	SignalCompleteness      = "snapshot_completeness"
	SignalCPU               = "worker_cpu_p95"
	SignalClockSpread       = "clock_offset_spread"
	SignalWorkerLost        = "worker_lost"
	SignalProtocolViolation = "protocol_violation"
)

// Inputs are a run's health measurements. A nil pointer means the signal
// was not measured, which is never VALID (design.md §5, §6.1).
type Inputs struct {
	Aborted bool // the run ended ABORTED: always INVALID
	Workers int  // members of the run

	// HasResult is false when the aggregator has no result for the run;
	// every signal derived from snapshots is then unmeasured.
	HasResult bool

	LagP99US *int64 // merged dispatch-lag p99; nil when nothing was released

	Intended            int64
	Dropped             int64
	TargetRatePerWorker float64
	// DispatchWindowNS is the total time, summed over workers on their
	// monotonic clocks, during which their dispatchers were scheduling
	// requests. It excludes the drain after the last release. Nil when not
	// reported; the rate signal is then unmeasured.
	DispatchWindowNS *int64

	ExpectedSnapshots, ReceivedSnapshots int64

	CPUP95Percent  *float64 // highest p95 across workers; nil = unmeasured
	ClockOffsetsNS []*int64 // one per worker; any nil makes the spread unmeasured

	LostWorkers        []string
	ProtocolViolations int64 // protocol_violation events plus rejected snapshots
}

// Signal is one health signal's evaluation.
type Signal struct {
	Name       string   `json:"name"`
	Verdict    Verdict  `json:"verdict"`
	Value      *float64 `json:"value,omitempty"` // nil when unmeasured or not numeric
	Unit       string   `json:"unit,omitempty"`
	Unmeasured bool     `json:"unmeasured,omitempty"`
	Detail     string   `json:"detail,omitempty"`
}

// Result is a run's verdict with the signals behind it.
type Result struct {
	Verdict    Verdict    `json:"verdict"`
	Signals    []Signal   `json:"signals"`
	Thresholds Thresholds `json:"thresholds"`
	Degraded   []string   `json:"degraded,omitempty"`   // signals at DEGRADED
	Invalid    []string   `json:"invalid,omitempty"`    // signals at INVALID
	Unmeasured []string   `json:"unmeasured,omitempty"` // signals not measured (counted as DEGRADED)
}

// Detail is the JSON stored in runs.verdict_detail.
func (r Result) Detail() json.RawMessage {
	b, err := json.Marshal(r)
	if err != nil {
		// Only finite floats and plain types: cannot fail.
		panic(fmt.Sprintf("health: marshal verdict detail: %v", err))
	}
	return b
}

func rank(v Verdict) int {
	switch v {
	case Valid:
		return 0
	case Degraded:
		return 1
	}
	return 2
}

// Worst returns the worse of two verdicts.
func Worst(a, b Verdict) Verdict {
	if rank(b) > rank(a) {
		return b
	}
	return a
}

// Evaluate computes a run's verdict: the worst of its signals. t must be
// valid (ParseThresholds and DefaultThresholds return valid ones).
func Evaluate(in Inputs, t Thresholds) (Result, error) {
	if err := t.Validate(); err != nil {
		return Result{}, err
	}
	r := Result{Verdict: Valid, Thresholds: t}
	add := func(s Signal) {
		if s.Unmeasured {
			s.Verdict, s.Value = Degraded, nil
			r.Unmeasured = append(r.Unmeasured, s.Name)
		} else {
			switch s.Verdict {
			case Degraded:
				r.Degraded = append(r.Degraded, s.Name)
			case Invalid:
				r.Invalid = append(r.Invalid, s.Name)
			}
		}
		r.Verdict = Worst(r.Verdict, s.Verdict)
		r.Signals = append(r.Signals, s)
	}
	num := func(v float64) *float64 { return &v }
	unmeasured := func(name, why string) Signal { return Signal{Name: name, Unmeasured: true, Detail: why} }
	noResult := "the aggregator has no result for the run"

	// Ending ABORTED is INVALID whatever else was measured.
	if in.Aborted {
		add(Signal{Name: SignalAborted, Verdict: Invalid, Detail: "the run was aborted"})
	}

	switch {
	case !in.HasResult:
		add(unmeasured(SignalLagP99, noResult))
	case in.LagP99US == nil:
		add(unmeasured(SignalLagP99, "no request was released"))
	default:
		v := *in.LagP99US
		add(Signal{Name: SignalLagP99, Value: num(float64(v)), Unit: "us",
			Verdict: lowerIsBetter(float64(v), float64(t.LagP99ValidUS), float64(t.LagP99DegradedUS))})
	}

	switch {
	case !in.HasResult:
		add(unmeasured(SignalAchievedRate, noResult))
	case in.DispatchWindowNS == nil:
		add(unmeasured(SignalAchievedRate, "workers do not report their dispatch window"))
	case *in.DispatchWindowNS <= 0 || in.TargetRatePerWorker <= 0:
		add(unmeasured(SignalAchievedRate, "no dispatch window or no target rate"))
	default:
		want := in.TargetRatePerWorker * float64(*in.DispatchWindowNS) / 1e9
		ratio := float64(in.Intended) / want
		add(Signal{Name: SignalAchievedRate, Value: num(ratio), Unit: "ratio",
			Verdict: higherIsBetter(ratio, t.RateValid, t.RateDegraded)})
	}

	switch {
	case !in.HasResult:
		add(unmeasured(SignalDropped, noResult))
	case in.Intended <= 0:
		add(unmeasured(SignalDropped, "no request was released"))
	default:
		ratio := float64(in.Dropped) / float64(in.Intended)
		v := Invalid
		switch {
		case in.Dropped == 0:
			v = Valid
		case ratio <= t.DroppedDegraded:
			v = Degraded
		}
		add(Signal{Name: SignalDropped, Value: num(ratio), Unit: "ratio", Verdict: v,
			Detail: fmt.Sprintf("%d of %d intended", in.Dropped, in.Intended)})
	}

	switch {
	case !in.HasResult:
		add(unmeasured(SignalCompleteness, noResult))
	case in.ExpectedSnapshots <= 0:
		add(unmeasured(SignalCompleteness, "no snapshot was expected"))
	default:
		ratio := float64(in.ReceivedSnapshots) / float64(in.ExpectedSnapshots)
		v := Invalid
		switch {
		case in.ReceivedSnapshots >= in.ExpectedSnapshots:
			v = Valid
		case ratio >= t.CompletenessDegraded:
			v = Degraded
		}
		add(Signal{Name: SignalCompleteness, Value: num(ratio), Unit: "ratio", Verdict: v,
			Detail: fmt.Sprintf("%d of %d snapshots", in.ReceivedSnapshots, in.ExpectedSnapshots)})
	}

	if in.CPUP95Percent == nil {
		add(unmeasured(SignalCPU, "workers do not report CPU"))
	} else {
		v := *in.CPUP95Percent
		add(Signal{Name: SignalCPU, Value: num(v), Unit: "percent", Verdict: lowerIsBetter(v, t.CPUValid, t.CPUDegraded)})
	}

	add(clockSpread(in, t))

	if n := len(in.LostWorkers); n > 0 {
		add(Signal{Name: SignalWorkerLost, Verdict: Invalid, Value: num(float64(n)),
			Detail: fmt.Sprintf("lost mid-run: %v", in.LostWorkers)})
	} else {
		add(Signal{Name: SignalWorkerLost, Verdict: Valid, Value: num(0)})
	}

	if in.ProtocolViolations > 0 {
		add(Signal{Name: SignalProtocolViolation, Verdict: Invalid, Value: num(float64(in.ProtocolViolations))})
	} else {
		add(Signal{Name: SignalProtocolViolation, Verdict: Valid, Value: num(0)})
	}
	return r, nil
}

func clockSpread(in Inputs, t Thresholds) Signal {
	if len(in.ClockOffsetsNS) == 0 || len(in.ClockOffsetsNS) != in.Workers {
		return Signal{Name: SignalClockSpread, Unmeasured: true,
			Detail: fmt.Sprintf("%d offsets for %d workers", len(in.ClockOffsetsNS), in.Workers)}
	}
	var lo, hi int64
	for i, o := range in.ClockOffsetsNS {
		if o == nil {
			return Signal{Name: SignalClockSpread, Unmeasured: true, Detail: "a worker did not measure its clock offset"}
		}
		if i == 0 || *o < lo {
			lo = *o
		}
		if i == 0 || *o > hi {
			hi = *o
		}
	}
	spread := float64(hi - lo)
	return Signal{Name: SignalClockSpread, Value: &spread, Unit: "ns",
		Verdict: lowerIsBetter(spread, float64(t.ClockSpreadValidNS), float64(t.ClockSpreadDegradedNS))}
}

func lowerIsBetter(v, valid, degraded float64) Verdict {
	switch {
	case v <= valid:
		return Valid
	case v <= degraded:
		return Degraded
	}
	return Invalid
}

func higherIsBetter(v, valid, degraded float64) Verdict {
	switch {
	case v >= valid:
		return Valid
	case v >= degraded:
		return Degraded
	}
	return Invalid
}
