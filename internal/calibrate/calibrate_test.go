package calibrate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/timingtest"
)

// fakeStepper returns scripted health per rate.
type fakeStepper struct {
	steps func(rate float64) Step
	rates []float64
	err   error
}

func (f *fakeStepper) Step(_ context.Context, rate float64, _ time.Duration) (Step, error) {
	f.rates = append(f.rates, rate)
	if f.err != nil {
		return Step{}, f.err
	}
	return f.steps(rate), nil
}

func healthy(rate float64) Step {
	return Step{Intended: int64(rate), OK: int64(rate), LagP99US: 200, CPUPercent: 10}
}

func ramp() RampConfig {
	return RampConfig{StartRate: 500, Factor: 2, MaxRate: 8000, StepDuration: time.Second, Limits: DefaultLimits()}
}

func TestRampStopsAtTheFirstStepBreakingALimit(t *testing.T) {
	for name, tc := range map[string]struct {
		breakAt  float64
		mut      func(*Step)
		failWith string
	}{
		"dispatch lag": {2000, func(s *Step) { s.LagP99US = 1001 }, "dispatch lag"},
		"drops":        {4000, func(s *Step) { s.Dropped = 1 }, "generator_dropped"},
		"cpu":          {1000, func(s *Step) { s.CPUPercent = 70.1 }, "CPU"},
		"no release":   {2000, func(s *Step) { s.Intended = 0 }, "nothing was released"},
	} {
		f := &fakeStepper{steps: func(rate float64) Step {
			s := healthy(rate)
			if rate >= tc.breakAt {
				tc.mut(&s)
			}
			return s
		}}
		capacity, steps, err := Ramp(context.Background(), f, ramp())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if capacity != tc.breakAt/2 {
			t.Errorf("%s: capacity %v, want the last passing rate %v", name, capacity, tc.breakAt/2)
		}
		last := steps[len(steps)-1]
		if last.Passed || !strings.Contains(last.Failed, tc.failWith) || last.RatePerSec != tc.breakAt {
			t.Errorf("%s: last step %+v", name, last)
		}
		if f.rates[len(f.rates)-1] != tc.breakAt {
			t.Errorf("%s: kept ramping past the failure: %v", name, f.rates)
		}
	}
}

func TestRampLimitsAreInclusive(t *testing.T) {
	f := &fakeStepper{steps: func(rate float64) Step {
		return Step{Intended: 1, LagP99US: 1000, Dropped: 0, CPUPercent: 70}
	}}
	capacity, _, err := Ramp(context.Background(), f, ramp())
	if err != nil || capacity != 8000 {
		t.Errorf("exactly at every bound: capacity %v, %v", capacity, err)
	}
}

func TestRampFirstStepFailingMeansZeroCapacity(t *testing.T) {
	f := &fakeStepper{steps: func(rate float64) Step { s := healthy(rate); s.Dropped = 3; return s }}
	capacity, steps, err := Ramp(context.Background(), f, ramp())
	if err != nil || capacity != 0 || len(steps) != 1 {
		t.Errorf("capacity %v, %d steps, %v", capacity, len(steps), err)
	}
}

func TestRampEndsExactlyAtMaxRate(t *testing.T) {
	c := ramp()
	c.MaxRate = 3000 // 500, 1000, 2000, then 3000 rather than 4000
	f := &fakeStepper{steps: healthy}
	capacity, _, err := Ramp(context.Background(), f, c)
	if err != nil || capacity != 3000 {
		t.Fatalf("capacity %v, %v", capacity, err)
	}
	if want := []float64{500, 1000, 2000, 3000}; len(f.rates) != len(want) || f.rates[3] != 3000 {
		t.Errorf("rates %v, want %v", f.rates, want)
	}
}

func TestRampStepErrorIsReturned(t *testing.T) {
	f := &fakeStepper{err: errors.New("target down")}
	if _, _, err := Ramp(context.Background(), f, ramp()); err == nil || !strings.Contains(err.Error(), "target down") {
		t.Errorf("err %v", err)
	}
}

func TestRampConfigValidate(t *testing.T) {
	for name, mut := range map[string]func(*RampConfig){
		"zero start":     func(c *RampConfig) { c.StartRate = 0 },
		"max below":      func(c *RampConfig) { c.MaxRate = 100 },
		"factor 1":       func(c *RampConfig) { c.Factor = 1 },
		"zero step":      func(c *RampConfig) { c.StepDuration = 0 },
		"zero lag limit": func(c *RampConfig) { c.Limits.LagP99US = 0 },
		"negative drops": func(c *RampConfig) { c.Limits.MaxDropped = -1 },
	} {
		c := ramp()
		mut(&c)
		if c.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func validProfile() Profile {
	return Profile{
		MethodVersion: MethodVersion, HostClass: "c6i-2xl", Hostname: "h1", NumCPU: runtime.NumCPU(),
		GOMAXPROCS: runtime.GOMAXPROCS(0), GoVersion: runtime.Version(), Target: InProcessTarget,
		MeasuredAt: time.Now().UTC().Truncate(time.Second), CapacityRPS: 4000, Limits: DefaultLimits(),
		Steps: []Step{{RatePerSec: 4000, Passed: true}},
	}
}

func TestProfileRoundTripAndHostCheck(t *testing.T) {
	p := validProfile()
	path := filepath.Join(t.TempDir(), "profile.json")
	if err := p.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	got, err := LoadForThisHost(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.CapacityRPS != 4000 || got.HostClass != "c6i-2xl" || !got.MeasuredAt.Equal(p.MeasuredAt) {
		t.Errorf("loaded %+v", got)
	}

	other := validProfile()
	other.NumCPU++
	if err := other.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadForThisHost(path); err == nil || !strings.Contains(err.Error(), "recalibrate") {
		t.Errorf("profile from another host: %v", err)
	}
}

func TestProfileValidateAndStrictDecode(t *testing.T) {
	for name, mut := range map[string]func(*Profile){
		"old method":     func(p *Profile) { p.MethodVersion = MethodVersion + 1 },
		"bad host class": func(p *Profile) { p.HostClass = "a b" },
		"negative cap":   func(p *Profile) { p.CapacityRPS = -1 },
		"no steps":       func(p *Profile) { p.Steps = nil },
		"no cpu counts":  func(p *Profile) { p.NumCPU = 0 },
		"no time":        func(p *Profile) { p.MeasuredAt = time.Time{} },
	} {
		p := validProfile()
		mut(&p)
		if p.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A typo in a hand-edited profile is an error, not a silent default.
	b, _ := json.Marshal(validProfile())
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	m["capacity_rsp"] = 1e9
	b, _ = json.Marshal(m)
	path := filepath.Join(t.TempDir(), "p.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadForThisHost(path); err == nil {
		t.Error("unknown field accepted")
	}
}

func TestProcessCPUAdvances(t *testing.T) {
	a, err := processCPU()
	if err != nil {
		t.Skip(err)
	}
	deadline := time.Now().Add(50 * time.Millisecond)
	x := 0
	for time.Now().Before(deadline) {
		x++
	}
	b, _ := processCPU()
	if b <= a || x == 0 {
		t.Errorf("CPU time %v then %v", a, b)
	}
}

// TestCalibrateAgainstTheInProcessStub runs the real engine: a short
// jitter baseline and a two-step ramp. It checks that everything is
// measured and recorded, not how fast this machine is.
func TestCalibrateAgainstTheInProcessStub(t *testing.T) {
	if testing.Short() {
		t.Skip("generates load for ~4s")
	}
	timingtest.Exclusive(t)
	url, stop, err := StartInProcessStub()
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	s := &EngineStepper{URL: url, Method: "GET", MaxInFlight: 100, QueueDepth: 1000, RequestTimeout: time.Second, Lead: 50 * time.Millisecond}
	p, err := Calibrate(context.Background(), s, Options{
		HostClass: "test", Target: InProcessTarget, JitterRate: 100, JitterDuration: time.Second, Pause: 100 * time.Millisecond,
		Ramp: RampConfig{StartRate: 100, Factor: 2, MaxRate: 200, StepDuration: time.Second, Limits: DefaultLimits()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Jitter.Intended < 99 || p.Jitter.Intended > 101 || p.Jitter.MaxUS < p.Jitter.P99US {
		t.Errorf("jitter %+v", p.Jitter)
	}
	if len(p.Steps) == 0 || len(p.Steps) > 2 {
		t.Fatalf("steps %+v", p.Steps)
	}
	for _, st := range p.Steps {
		want := int64(st.RatePerSec) // one second per step
		if st.Intended < want-1 || st.Intended > want+1 || st.OK+st.Errors+st.Timeouts != st.Intended {
			t.Errorf("step %+v: requests skipped or lost", st)
		}
		if st.CPUPercent <= 0 {
			t.Errorf("step %+v: CPU not measured", st)
		}
	}
	// Capacity is whatever passed; it is never above the last passing rate.
	if p.CapacityRPS != 0 && !p.Steps[0].Passed {
		t.Errorf("capacity %v with a failed first step", p.CapacityRPS)
	}
}
