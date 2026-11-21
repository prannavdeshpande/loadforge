package calibrate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/stubserver"
)

// EngineStepper runs the real open-loop engine, with the HTTP client a
// worker uses, against URL. Process CPU is measured around each step, so
// with the in-process stub it includes the stub's own work: capacity is
// then conservative.
type EngineStepper struct {
	URL            string
	Method         string
	MaxInFlight    int
	QueueDepth     int
	RequestTimeout time.Duration
	// Lead is how far ahead of now t0 is set, so the step starts on schedule.
	Lead time.Duration

	client *http.Client
}

func (s *EngineStepper) httpClient() *http.Client {
	if s.client == nil {
		s.client = &http.Client{Transport: &http.Transport{
			Proxy:               nil, // straight to the target, like a worker
			MaxIdleConns:        s.MaxInFlight,
			MaxIdleConnsPerHost: s.MaxInFlight,
			IdleConnTimeout:     90 * time.Second,
		}}
	}
	return s.client
}

// measured is one engine run's totals and the CPU it used.
type measured struct {
	total      engine.Interval
	cpuPercent float64
}

func (s *EngineStepper) run(ctx context.Context, rate float64, d time.Duration) (measured, error) {
	eng, err := engine.New(engine.Config{
		Rate: rate, Duration: d, MaxInFlight: s.MaxInFlight, QueueDepth: s.QueueDepth,
		RequestTimeout: s.RequestTimeout, DrainTimeout: s.RequestTimeout + time.Second,
	}, &engine.HTTPDoer{Client: s.httpClient(), Method: s.Method, URL: s.URL})
	if err != nil {
		return measured{}, err
	}
	cpu0, err := processCPU()
	if err != nil {
		return measured{}, err
	}
	start := time.Now()
	res, err := eng.Run(ctx, start.Add(s.Lead))
	if err != nil {
		return measured{}, err
	}
	wall := time.Since(start)
	cpu1, err := processCPU()
	if err != nil {
		return measured{}, err
	}
	cpu := 100 * float64(cpu1-cpu0) / (float64(wall) * float64(runtime.GOMAXPROCS(0)))
	return measured{total: res.Total, cpuPercent: cpu}, nil
}

// Step implements Stepper.
func (s *EngineStepper) Step(ctx context.Context, rate float64, d time.Duration) (Step, error) {
	m, err := s.run(ctx, rate, d)
	if err != nil {
		return Step{}, err
	}
	t := m.total
	return Step{
		Intended: t.Intended, OK: t.OK, Errors: t.Errors, Timeouts: t.Timeouts, Dropped: t.Dropped,
		LagP99US: t.DispatchLag.ValueAtQuantile(99), LatencyP99: t.Latency.ValueAtQuantile(99),
		CPUPercent: m.cpuPercent,
	}, nil
}

// Jitter measures dispatch lag at a low, steady rate.
func (s *EngineStepper) Jitter(ctx context.Context, rate float64, d time.Duration) (Jitter, error) {
	m, err := s.run(ctx, rate, d)
	if err != nil {
		return Jitter{}, err
	}
	lag := m.total.DispatchLag
	if m.total.Intended == 0 {
		return Jitter{}, errors.New("calibrate: jitter baseline released nothing")
	}
	return Jitter{
		RatePerSec: rate, DurationMS: d.Milliseconds(), Intended: m.total.Intended,
		P50US: lag.ValueAtQuantile(50), P99US: lag.ValueAtQuantile(99), P999US: lag.ValueAtQuantile(99.9), MaxUS: lag.Max(),
	}, nil
}

// Options is a whole calibration.
type Options struct {
	HostClass      string
	Target         string // InProcessTarget or the target URL, for the record
	JitterRate     float64
	JitterDuration time.Duration
	Ramp           RampConfig
	// Pause lets in-flight work, idle connections and the GC settle
	// between measurements, so one step's tail does not count in the next.
	Pause time.Duration
}

// Calibrate measures the jitter baseline on an idle generator, then ramps
// to capacity, and returns the profile.
func Calibrate(ctx context.Context, s *EngineStepper, o Options) (*Profile, error) {
	host, _ := os.Hostname()
	p := &Profile{
		MethodVersion: MethodVersion, HostClass: o.HostClass, Hostname: host,
		NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), GoVersion: runtime.Version(),
		Target: o.Target, Limits: o.Ramp.Limits,
	}
	if o.JitterRate <= 0 || o.JitterDuration <= 0 {
		return nil, errors.New("calibrate: jitter rate and duration must be positive")
	}
	if err := o.Ramp.Validate(); err != nil {
		return nil, err
	}
	j, err := s.Jitter(ctx, o.JitterRate, o.JitterDuration)
	if err != nil {
		return nil, fmt.Errorf("calibrate: jitter baseline: %w", err)
	}
	p.Jitter = j
	if err := pause(ctx, o.Pause); err != nil {
		return nil, err
	}
	p.CapacityRPS, p.Steps, err = Ramp(ctx, pausing{s, o.Pause}, o.Ramp)
	if err != nil {
		return nil, err
	}
	p.MeasuredAt = time.Now().UTC()
	return p, p.Validate()
}

// pausing waits before every step after the first.
type pausing struct {
	Stepper
	d time.Duration
}

func (p pausing) Step(ctx context.Context, rate float64, d time.Duration) (Step, error) {
	if err := pause(ctx, p.d); err != nil {
		return Step{}, err
	}
	return p.Stepper.Step(ctx, rate, d)
}

func pause(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StartInProcessStub serves a zero-latency stub on a loopback port and
// returns its URL. It shares this process's CPU with the generator, which
// makes the measured capacity conservative.
func StartInProcessStub() (url string, stop func(), err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: stubserver.New(0), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String() + "/", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}
