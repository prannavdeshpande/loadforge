package health

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func i64(v int64) *int64     { return &v }
func f64(v float64) *float64 { return &v }
func offsets(vs ...int64) []*int64 {
	out := make([]*int64, len(vs))
	for i := range vs {
		out[i] = i64(vs[i])
	}
	return out
}

// healthy is a 2-worker, 10s run at 100 req/s per worker where every signal
// is measured and VALID.
func healthy() Inputs {
	return Inputs{
		Workers: 2, HasResult: true,
		LagP99US: i64(400),
		Intended: 2000, TargetRatePerWorker: 100, DispatchWindowNS: i64(2 * 10e9),
		ExpectedSnapshots: 22, ReceivedSnapshots: 22,
		CPUP95Percent:  f64(40),
		ClockOffsetsNS: offsets(-200_000, 300_000),
	}
}

func eval(t *testing.T, in Inputs) Result {
	t.Helper()
	r, err := Evaluate(in, DefaultThresholds())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func signal(t *testing.T, r Result, name string) Signal {
	t.Helper()
	for _, s := range r.Signals {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no signal %s in %+v", name, r.Signals)
	return Signal{}
}

func TestHealthyRunIsValid(t *testing.T) {
	r := eval(t, healthy())
	if r.Verdict != Valid || len(r.Degraded)+len(r.Invalid)+len(r.Unmeasured) != 0 {
		t.Fatalf("healthy run: %+v", r)
	}
}

// Each band edge from design.md §5, on both sides.
func TestSignalBandBoundaries(t *testing.T) {
	for _, c := range []struct {
		name   string
		signal string
		mut    func(*Inputs)
		want   Verdict
	}{
		{"lag exactly 1ms", SignalLagP99, func(in *Inputs) { in.LagP99US = i64(1000) }, Valid},
		{"lag just over 1ms", SignalLagP99, func(in *Inputs) { in.LagP99US = i64(1001) }, Degraded},
		{"lag exactly 10ms", SignalLagP99, func(in *Inputs) { in.LagP99US = i64(10_000) }, Degraded},
		{"lag just over 10ms", SignalLagP99, func(in *Inputs) { in.LagP99US = i64(10_001) }, Invalid},

		{"rate exactly 99%", SignalAchievedRate, func(in *Inputs) { in.Intended = 1980 }, Valid},
		{"rate just under 99%", SignalAchievedRate, func(in *Inputs) { in.Intended = 1979 }, Degraded},
		{"rate exactly 95%", SignalAchievedRate, func(in *Inputs) { in.Intended = 1900 }, Degraded},
		{"rate just under 95%", SignalAchievedRate, func(in *Inputs) { in.Intended = 1899 }, Invalid},
		{"rate above target", SignalAchievedRate, func(in *Inputs) { in.Intended = 2002 }, Valid},

		{"no drops", SignalDropped, func(in *Inputs) { in.Dropped = 0 }, Valid},
		{"one drop", SignalDropped, func(in *Inputs) { in.Dropped = 1 }, Degraded},
		{"drops exactly 0.1%", SignalDropped, func(in *Inputs) { in.Dropped = 2 }, Degraded},
		{"drops over 0.1%", SignalDropped, func(in *Inputs) { in.Dropped = 3 }, Invalid},

		{"all snapshots", SignalCompleteness, func(in *Inputs) { in.ExpectedSnapshots, in.ReceivedSnapshots = 1000, 1000 }, Valid},
		{"one missing", SignalCompleteness, func(in *Inputs) { in.ExpectedSnapshots, in.ReceivedSnapshots = 1000, 999 }, Degraded},
		{"exactly 99%", SignalCompleteness, func(in *Inputs) { in.ExpectedSnapshots, in.ReceivedSnapshots = 1000, 990 }, Degraded},
		{"under 99%", SignalCompleteness, func(in *Inputs) { in.ExpectedSnapshots, in.ReceivedSnapshots = 1000, 989 }, Invalid},

		{"cpu exactly 70%", SignalCPU, func(in *Inputs) { in.CPUP95Percent = f64(70) }, Valid},
		{"cpu over 70%", SignalCPU, func(in *Inputs) { in.CPUP95Percent = f64(70.1) }, Degraded},
		{"cpu exactly 85%", SignalCPU, func(in *Inputs) { in.CPUP95Percent = f64(85) }, Degraded},
		{"cpu over 85%", SignalCPU, func(in *Inputs) { in.CPUP95Percent = f64(85.1) }, Invalid},

		{"spread exactly 1ms", SignalClockSpread, func(in *Inputs) { in.ClockOffsetsNS = offsets(0, 1_000_000) }, Valid},
		{"spread over 1ms", SignalClockSpread, func(in *Inputs) { in.ClockOffsetsNS = offsets(0, 1_000_001) }, Degraded},
		{"spread exactly 5ms", SignalClockSpread, func(in *Inputs) { in.ClockOffsetsNS = offsets(-2_500_000, 2_500_000) }, Degraded},
		{"spread over 5ms", SignalClockSpread, func(in *Inputs) { in.ClockOffsetsNS = offsets(-2_500_000, 2_500_001) }, Invalid},

		{"worker lost", SignalWorkerLost, func(in *Inputs) { in.LostWorkers = []string{"w2"} }, Invalid},
		{"protocol violation", SignalProtocolViolation, func(in *Inputs) { in.ProtocolViolations = 1 }, Invalid},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := healthy()
			c.mut(&in)
			r := eval(t, in)
			if got := signal(t, r, c.signal).Verdict; got != c.want {
				t.Errorf("%s = %s, want %s", c.signal, got, c.want)
			}
			// Every other signal is still VALID, so the run takes this one.
			if r.Verdict != c.want {
				t.Errorf("run verdict = %s, want %s", r.Verdict, c.want)
			}
		})
	}
}

func TestVerdictIsTheWorstSignal(t *testing.T) {
	in := healthy()
	in.LagP99US = i64(5000)    // DEGRADED
	in.ReceivedSnapshots = 21  // 95%: INVALID
	in.CPUP95Percent = f64(80) // DEGRADED
	r := eval(t, in)
	if r.Verdict != Invalid {
		t.Errorf("verdict = %s, want INVALID", r.Verdict)
	}
	if !slices.Equal(r.Invalid, []string{SignalCompleteness}) ||
		!slices.Equal(r.Degraded, []string{SignalLagP99, SignalCPU}) {
		t.Errorf("degraded=%v invalid=%v", r.Degraded, r.Invalid)
	}
}

// An unmeasured signal is never VALID (design.md §6.1).
func TestUnmeasuredSignalsAreDegraded(t *testing.T) {
	for _, c := range []struct {
		name, signal string
		mut          func(*Inputs)
	}{
		{"cpu not reported", SignalCPU, func(in *Inputs) { in.CPUP95Percent = nil }},
		{"one clock offset missing", SignalClockSpread, func(in *Inputs) { in.ClockOffsetsNS[1] = nil }},
		{"no clock offsets", SignalClockSpread, func(in *Inputs) { in.ClockOffsetsNS = nil }},
		{"fewer offsets than workers", SignalClockSpread, func(in *Inputs) { in.ClockOffsetsNS = offsets(0) }},
		{"no dispatch window", SignalAchievedRate, func(in *Inputs) { in.DispatchWindowNS = nil }},
		{"zero dispatch window", SignalAchievedRate, func(in *Inputs) { in.DispatchWindowNS = i64(0) }},
		{"no target rate", SignalAchievedRate, func(in *Inputs) { in.TargetRatePerWorker = 0 }},
		{"nothing released: lag", SignalLagP99, func(in *Inputs) { in.LagP99US = nil }},
		{"nothing released: drops", SignalDropped, func(in *Inputs) { in.Intended = 0 }},
		{"nothing expected", SignalCompleteness, func(in *Inputs) { in.ExpectedSnapshots, in.ReceivedSnapshots = 0, 0 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := healthy()
			c.mut(&in)
			r := eval(t, in)
			s := signal(t, r, c.signal)
			if !s.Unmeasured || s.Verdict != Degraded || s.Value != nil {
				t.Errorf("%s: %+v, want unmeasured and DEGRADED with no value", c.signal, s)
			}
			if r.Verdict == Valid || !slices.Contains(r.Unmeasured, c.signal) {
				t.Errorf("run: verdict %s, unmeasured %v", r.Verdict, r.Unmeasured)
			}
		})
	}
}

func TestNoResultLeavesSnapshotSignalsUnmeasured(t *testing.T) {
	in := healthy()
	in.HasResult = false
	r := eval(t, in)
	for _, name := range []string{SignalLagP99, SignalAchievedRate, SignalDropped, SignalCompleteness} {
		if !slices.Contains(r.Unmeasured, name) {
			t.Errorf("%s measured without a result", name)
		}
	}
	if r.Verdict != Degraded {
		t.Errorf("verdict = %s, want DEGRADED", r.Verdict)
	}
}

func TestAbortedRunIsInvalidEvenIfHealthy(t *testing.T) {
	in := healthy()
	in.Aborted = true
	if r := eval(t, in); r.Verdict != Invalid || !slices.Contains(r.Invalid, SignalAborted) {
		t.Errorf("aborted: %+v", r)
	}
}

func TestDetailRecordsThresholdsAndSignals(t *testing.T) {
	in := healthy()
	in.CPUP95Percent = nil
	r := eval(t, in)
	var got struct {
		Verdict    Verdict    `json:"verdict"`
		Thresholds Thresholds `json:"thresholds"`
		Signals    []Signal   `json:"signals"`
		Unmeasured []string   `json:"unmeasured"`
	}
	if err := json.Unmarshal(r.Detail(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Verdict != Degraded || got.Thresholds != DefaultThresholds() ||
		len(got.Signals) != len(r.Signals) || !slices.Equal(got.Unmeasured, []string{SignalCPU}) {
		t.Errorf("detail = %s", r.Detail())
	}
}

func TestParseThresholds(t *testing.T) {
	for _, raw := range []string{"", "null", "{}", " {} "} {
		got, err := ParseThresholds(json.RawMessage(raw))
		if err != nil || got != DefaultThresholds() {
			t.Errorf("%q: %+v, %v; want the defaults", raw, got, err)
		}
	}

	got, err := ParseThresholds(json.RawMessage(`{"lag_p99_valid_us": 500, "cpu_degraded": 90}`))
	if err != nil {
		t.Fatal(err)
	}
	want := DefaultThresholds()
	want.LagP99ValidUS, want.CPUDegraded = 500, 90
	if got != want {
		t.Errorf("overrides: got %+v, want %+v", got, want)
	}

	for name, raw := range map[string]string{
		"misspelt key":           `{"lag_p99_vaild_us": 500}`,
		"valid looser than deg.": `{"lag_p99_valid_us": 20000}`,
		"rate valid below deg.":  `{"rate_valid": 0.9}`,
		"rate above 1":           `{"rate_valid": 1.5}`,
		"negative drops":         `{"dropped_degraded": -0.1}`,
		"completeness above 1":   `{"completeness_degraded": 1.01}`,
		"cpu above 100":          `{"cpu_degraded": 101}`,
		"spread inverted":        `{"clock_spread_valid_ns": 6000000}`,
		"not an object":          `[1]`,
		"wrong type":             `{"cpu_valid": "high"}`,
		"trailing data":          `{} {}`,
	} {
		if _, err := ParseThresholds(json.RawMessage(raw)); err == nil {
			t.Errorf("%s: accepted %s", name, raw)
		} else if !strings.HasPrefix(err.Error(), "health: thresholds") {
			t.Errorf("%s: error %q lacks the package prefix", name, err)
		}
	}
}

func TestEvaluateRejectsInvalidThresholds(t *testing.T) {
	bad := DefaultThresholds()
	bad.RateValid = 0.5
	if _, err := Evaluate(healthy(), bad); err == nil {
		t.Error("evaluated with inverted rate band")
	}
}

func TestWorst(t *testing.T) {
	for _, c := range [][3]Verdict{
		{Valid, Valid, Valid}, {Valid, Degraded, Degraded}, {Degraded, Valid, Degraded},
		{Degraded, Invalid, Invalid}, {Invalid, Valid, Invalid},
	} {
		if got := Worst(c[0], c[1]); got != c[2] {
			t.Errorf("Worst(%s, %s) = %s, want %s", c[0], c[1], got, c[2])
		}
	}
}
