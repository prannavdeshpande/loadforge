package calibrate

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Stepper generates load at one rate for one duration and reports the
// generator's health over it.
type Stepper interface {
	Step(ctx context.Context, rate float64, d time.Duration) (Step, error)
}

// RampConfig is the capacity ramp: rates StartRate, StartRate×Factor, …
// up to MaxRate, each held for StepDuration.
type RampConfig struct {
	StartRate    float64
	Factor       float64
	MaxRate      float64
	StepDuration time.Duration
	Limits       Limits
}

// DefaultRamp is 500, 1000, 2000, … req/s, 10s per step, up to 100k.
func DefaultRamp() RampConfig {
	return RampConfig{StartRate: 500, Factor: 2, MaxRate: 100_000, StepDuration: 10 * time.Second, Limits: DefaultLimits()}
}

// Validate checks the ramp.
func (c RampConfig) Validate() error {
	switch {
	case c.StartRate <= 0 || c.MaxRate < c.StartRate:
		return fmt.Errorf("calibrate: rates must satisfy 0 < start (%v) ≤ max (%v)", c.StartRate, c.MaxRate)
	case c.Factor <= 1:
		return fmt.Errorf("calibrate: ramp factor %v must be above 1", c.Factor)
	case c.StepDuration <= 0:
		return errors.New("calibrate: step duration must be positive")
	case c.Limits.LagP99US <= 0 || c.Limits.CPUPercent <= 0 || c.Limits.MaxDropped < 0:
		return errors.New("calibrate: limits must be positive (drops non-negative)")
	}
	return nil
}

// judge applies the stop rule to a measured step.
func (l Limits) judge(s *Step) {
	switch {
	case s.Intended == 0:
		s.Failed = "nothing was released"
	case s.LagP99US > l.LagP99US:
		s.Failed = fmt.Sprintf("dispatch lag p99 %dµs > %dµs", s.LagP99US, l.LagP99US)
	case s.Dropped > l.MaxDropped:
		s.Failed = fmt.Sprintf("%d generator_dropped > %d", s.Dropped, l.MaxDropped)
	case s.CPUPercent > l.CPUPercent:
		s.Failed = fmt.Sprintf("CPU %.1f%% > %.1f%%", s.CPUPercent, l.CPUPercent)
	}
	s.Passed = s.Failed == ""
}

// Ramp steps the rate up until a step breaks a limit or MaxRate has
// passed, and returns the last passing rate (0 if none passed) with every
// step measured. Capacity is the highest rate actually sustained, never
// an interpolation.
func Ramp(ctx context.Context, st Stepper, c RampConfig) (float64, []Step, error) {
	if err := c.Validate(); err != nil {
		return 0, nil, err
	}
	var (
		capacity float64
		steps    []Step
	)
	for rate := c.StartRate; ; rate *= c.Factor {
		rate = min(rate, c.MaxRate)
		s, err := st.Step(ctx, rate, c.StepDuration)
		if err != nil {
			return capacity, steps, fmt.Errorf("calibrate: step at %v req/s: %w", rate, err)
		}
		s.RatePerSec, s.DurationMS = rate, c.StepDuration.Milliseconds()
		c.Limits.judge(&s)
		steps = append(steps, s)
		if !s.Passed || rate >= c.MaxRate {
			if s.Passed {
				capacity = rate
			}
			return capacity, steps, nil
		}
		capacity = rate
	}
}
