// Package calibrate measures a host's generator capacity and scheduler
// jitter baseline (design.md §9). The result is a Profile: written to
// Postgres (calibration_profiles) and/or a JSON file that a worker loads
// with -profile, so that it reports a measured capacity in its PREPARE ack
// and refuses a rate above it.
package calibrate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"time"

	"github.com/loadforge/loadforge/internal/transport"
)

// MethodVersion identifies how a profile was measured. Bump it when the
// ramp, its stop rule or what is measured changes, so profiles measured
// differently are never compared as equals.
const MethodVersion = 1

// InProcessTarget is Profile.Target for a run against the in-process stub.
const InProcessTarget = "in-process"

// Profile is one calibration of one host.
type Profile struct {
	MethodVersion int       `json:"method_version"`
	HostClass     string    `json:"host_class"`
	Hostname      string    `json:"hostname"`
	NumCPU        int       `json:"num_cpu"`
	GOMAXPROCS    int       `json:"gomaxprocs"`
	GoVersion     string    `json:"go_version"`
	Target        string    `json:"target"` // InProcessTarget or the -target URL
	MeasuredAt    time.Time `json:"measured_at"`

	Jitter Jitter `json:"jitter"`

	// CapacityRPS is the highest per-worker rate at which the generator
	// stayed within every limit; 0 if even the first step failed.
	CapacityRPS float64 `json:"capacity_rps"`
	Limits      Limits  `json:"limits"`
	Steps       []Step  `json:"steps"`
}

// Jitter is the scheduler's dispatch lag at a low, steady rate: what the
// host adds to every schedule when it is not loaded.
type Jitter struct {
	RatePerSec float64 `json:"rate_per_sec"`
	DurationMS int64   `json:"duration_ms"`
	Intended   int64   `json:"intended"`
	P50US      int64   `json:"p50_us"`
	P99US      int64   `json:"p99_us"`
	P999US     int64   `json:"p999_us"`
	MaxUS      int64   `json:"max_us"`
}

// Limits is the stop rule of the capacity ramp. A step passes only if its
// dispatch-lag p99, drops and CPU are all within them.
type Limits struct {
	LagP99US   int64   `json:"lag_p99_us"`  // the VALID dispatch-lag bound
	MaxDropped int64   `json:"max_dropped"` // the VALID drop bound
	CPUPercent float64 `json:"cpu_percent"` // the VALID worker CPU bound
}

// DefaultLimits are the VALID bounds of the verdict engine (design.md §5):
// capacity is the rate a worker can sustain and still be judged VALID.
func DefaultLimits() Limits { return Limits{LagP99US: 1000, MaxDropped: 0, CPUPercent: 70} }

// Step is one rate of the capacity ramp.
type Step struct {
	RatePerSec float64 `json:"rate_per_sec"`
	DurationMS int64   `json:"duration_ms"`
	Intended   int64   `json:"intended"`
	OK         int64   `json:"ok"`
	Errors     int64   `json:"errors"`
	Timeouts   int64   `json:"timeouts"`
	Dropped    int64   `json:"dropped"`
	LagP99US   int64   `json:"lag_p99_us"`
	LatencyP99 int64   `json:"latency_p99_us"`
	CPUPercent float64 `json:"cpu_percent"` // process CPU ÷ (wall × GOMAXPROCS)
	Passed     bool    `json:"passed"`
	Failed     string  `json:"failed,omitempty"` // which limit, when not passed
}

// Validate checks a profile's shape.
func (p *Profile) Validate() error {
	switch {
	case p.MethodVersion != MethodVersion:
		return fmt.Errorf("calibrate: profile method version %d, this binary measures with %d: recalibrate", p.MethodVersion, MethodVersion)
	case transport.ValidateID("host_class", p.HostClass) != nil:
		return transport.ValidateID("host_class", p.HostClass)
	case p.NumCPU <= 0 || p.GOMAXPROCS <= 0:
		return errors.New("calibrate: profile without CPU counts")
	case p.CapacityRPS < 0 || math.IsNaN(p.CapacityRPS) || math.IsInf(p.CapacityRPS, 0):
		return fmt.Errorf("calibrate: capacity %v is not a non-negative number", p.CapacityRPS)
	case len(p.Steps) == 0:
		return errors.New("calibrate: profile has no ramp steps")
	case p.MeasuredAt.IsZero():
		return errors.New("calibrate: profile without measured_at")
	}
	return nil
}

// CheckHost refuses a profile measured on a host with a different CPU
// count or GOMAXPROCS: its capacity says nothing about this one.
func (p *Profile) CheckHost(numCPU, gomaxprocs int) error {
	if p.NumCPU != numCPU || p.GOMAXPROCS != gomaxprocs {
		return fmt.Errorf("calibrate: profile for host class %s was measured with %d CPUs and GOMAXPROCS %d, this host has %d and %d: recalibrate",
			p.HostClass, p.NumCPU, p.GOMAXPROCS, numCPU, gomaxprocs)
	}
	return nil
}

// LoadForThisHost reads a profile file, validates it and checks it was
// measured on a host like this one.
func LoadForThisHost(path string) (*Profile, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: the operator names the profile file
	if err != nil {
		return nil, err
	}
	var p Profile
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("calibrate: %s: %w", path, err)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if err := p.CheckHost(runtime.NumCPU(), runtime.GOMAXPROCS(0)); err != nil {
		return nil, err
	}
	return &p, nil
}

// WriteFile writes the profile as indented JSON.
func (p *Profile) WriteFile(path string) error {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o600)
}
