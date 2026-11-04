package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Bound on dispatch lag p99 when the dispatcher is healthy. Well above
// normal timer jitter so -race and CI noise don't flake, and far below the
// stalls the tests inject.
const healthyLagP99 = 25 * time.Millisecond

const startLead = 50 * time.Millisecond

type doerFunc func(ctx context.Context) error

func (f doerFunc) Do(ctx context.Context) error { return f(ctx) }

// sleepDoer completes after d, or fails when ctx ends first.
func sleepDoer(d time.Duration) Doer {
	return doerFunc(func(ctx context.Context) error {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

// gateDoer blocks every request until gate is closed.
func gateDoer(gate <-chan struct{}) Doer {
	return doerFunc(func(ctx context.Context) error {
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

// run executes cfg starting at t0; tests pass t0 = now + startLead so that
// executor start-up isn't counted as dispatch lag, as the START barrier
// provides in production.
func run(t *testing.T, t0 time.Time, cfg Config, d Doer, setup func(*Engine)) Interval {
	t.Helper()
	e, err := New(cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	if setup != nil {
		setup(e)
	}
	res, err := e.Run(context.Background(), t0)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	checkAccounting(t, res)
	return res
}

func checkAccounting(t *testing.T, r Interval) {
	t.Helper()
	sum := r.OK + r.Errors + r.Timeouts + r.Dropped + r.Unfinished
	if sum != r.Intended {
		t.Errorf("ok %d + err %d + timeout %d + dropped %d + unfinished %d = %d, want intended %d",
			r.OK, r.Errors, r.Timeouts, r.Dropped, r.Unfinished, sum, r.Intended)
	}
	if completed := r.OK + r.Errors + r.Timeouts; r.Latency.TotalCount() != completed {
		t.Errorf("latency histogram has %d values, want one per completed request (%d)", r.Latency.TotalCount(), completed)
	}
	if r.DispatchLag.TotalCount() != r.Intended {
		t.Errorf("lag histogram has %d values, want one per released request (%d)", r.DispatchLag.TotalCount(), r.Intended)
	}
}

func dur(us int64) time.Duration { return time.Duration(us) * time.Microsecond }

func p(h *hdrhistogram.Histogram, q float64) time.Duration { return dur(h.ValueAtQuantile(q)) }

func TestDispatcherDoesNotWaitOnResponses(t *testing.T) {
	// Every response is held until 1.2s, past the end of the 1s schedule. A
	// closed-loop dispatcher would release one request; this one must release
	// all 200 on time.
	t0 := time.Now().Add(startLead)
	gate := make(chan struct{})
	time.AfterFunc(time.Until(t0.Add(1200*time.Millisecond)), func() { close(gate) })

	res := run(t, t0, Config{
		Rate: 200, Duration: time.Second,
		MaxInFlight: 500, QueueDepth: 0,
		RequestTimeout: 5 * time.Second, DrainTimeout: 5 * time.Second,
	}, gateDoer(gate), nil)

	if res.Intended != 200 || res.OK != 200 {
		t.Errorf("intended=%d ok=%d, want 200 and 200", res.Intended, res.OK)
	}
	if lag := p(res.DispatchLag, 99); lag > healthyLagP99 {
		t.Errorf("dispatch lag p99 = %v, want ≤ %v: the dispatcher waited on responses", lag, healthyLagP99)
	}
	// Request 0 was intended at t0 and completed at ~1.2s.
	if max := dur(res.Latency.Max()); max < 1150*time.Millisecond {
		t.Errorf("latency max = %v, want ≥ 1.15s (measured from intended send time)", max)
	}
}

func TestOverdueRequestsReleasedNotSkipped(t *testing.T) {
	// The dispatcher itself stalls for 200ms once, 300ms into the run.
	var stalled atomic.Bool
	t0 := time.Now().Add(startLead)
	res := run(t, t0, Config{
		Rate: 100, Duration: time.Second,
		MaxInFlight: 50, QueueDepth: 100,
		RequestTimeout: time.Second, DrainTimeout: time.Second,
	}, sleepDoer(time.Millisecond), func(e *Engine) {
		e.sleepUntil = func(ctx context.Context, at time.Time) error {
			if at.Sub(t0) >= 300*time.Millisecond && stalled.CompareAndSwap(false, true) {
				time.Sleep(200 * time.Millisecond)
			}
			return sleepUntil(ctx, at)
		}
	})

	if res.Intended != 100 || res.OK != 100 || res.Dropped != 0 {
		t.Errorf("intended=%d ok=%d dropped=%d, want 100/100/0: overdue requests were skipped", res.Intended, res.OK, res.Dropped)
	}
	if max := dur(res.DispatchLag.Max()); max < 190*time.Millisecond {
		t.Errorf("dispatch lag max = %v, want ≥ 190ms: the dispatcher stall was not recorded", max)
	}
	// The stall delayed sending, so it must also show in latency.
	if max := dur(res.Latency.Max()); max < 190*time.Millisecond {
		t.Errorf("latency max = %v, want ≥ 190ms", max)
	}
}

func TestLatencyMeasuredFromIntendedTime(t *testing.T) {
	// One executor, 50ms service time, a request every 10ms: requests queue
	// behind the cap. Request k completes at ~(k+1)·50ms but was intended at
	// k·10ms, so latency grows to ~2s. Measuring from the actual send would
	// report a flat 50ms.
	res := run(t, time.Now().Add(startLead), Config{
		Rate: 100, Duration: 500 * time.Millisecond,
		MaxInFlight: 1, QueueDepth: 100,
		RequestTimeout: time.Second, DrainTimeout: 5 * time.Second,
	}, sleepDoer(50*time.Millisecond), nil)

	if res.OK != 50 {
		t.Fatalf("ok=%d, want 50", res.OK)
	}
	if max := dur(res.Latency.Max()); max < 1800*time.Millisecond {
		t.Errorf("latency max = %v, want ≥ 1.8s", max)
	}
	if med := p(res.Latency, 50); med < 800*time.Millisecond {
		t.Errorf("latency p50 = %v, want ≥ 0.8s", med)
	}
	if max := dur(res.SendDelay.Max()); max < 1700*time.Millisecond {
		t.Errorf("send delay max = %v, want ≥ 1.7s: queueing behind the cap not recorded", max)
	}
	// Queueing is not a scheduler problem and must not show up as lag.
	if lag := p(res.DispatchLag, 99); lag > healthyLagP99 {
		t.Errorf("dispatch lag p99 = %v, want ≤ %v", lag, healthyLagP99)
	}
}

func TestQueueOverflowCountsGeneratorDropped(t *testing.T) {
	// 1 in flight + 5 queued; the other 44 of 50 requests overflow.
	t0 := time.Now().Add(startLead)
	gate := make(chan struct{})
	time.AfterFunc(time.Until(t0.Add(700*time.Millisecond)), func() { close(gate) })

	res := run(t, t0, Config{
		Rate: 100, Duration: 500 * time.Millisecond,
		MaxInFlight: 1, QueueDepth: 5,
		RequestTimeout: time.Second, DrainTimeout: time.Second,
	}, gateDoer(gate), nil)

	if res.Intended != 50 || res.OK != 6 || res.Dropped != 44 {
		t.Errorf("intended=%d ok=%d dropped=%d, want 50/6/44", res.Intended, res.OK, res.Dropped)
	}
}

func TestOutcomesAreClassifiedAndAllRecorded(t *testing.T) {
	var n atomic.Int64
	slow := sleepDoer(time.Second)
	d := doerFunc(func(ctx context.Context) error {
		switch n.Add(1) % 3 {
		case 0:
			return nil
		case 1:
			return errors.New("boom")
		default:
			return slow.Do(ctx) // exceeds RequestTimeout
		}
	})
	res := run(t, time.Now().Add(startLead), Config{
		Rate: 100, Duration: 300 * time.Millisecond,
		MaxInFlight: 50, QueueDepth: 50,
		RequestTimeout: 20 * time.Millisecond, DrainTimeout: time.Second,
	}, d, nil)

	if res.OK != 10 || res.Errors != 10 || res.Timeouts != 10 {
		t.Errorf("ok=%d err=%d timeout=%d, want 10 each", res.OK, res.Errors, res.Timeouts)
	}
	// Timeouts are in the latency histogram at their elapsed time.
	if max := dur(res.Latency.Max()); max < 20*time.Millisecond {
		t.Errorf("latency max = %v, want ≥ 20ms (timeouts recorded)", max)
	}
}

func TestDrainTimeoutCountsUnfinished(t *testing.T) {
	// 20 requests of 100ms through one executor need 2s; drain allows 200ms.
	res := run(t, time.Now().Add(startLead), Config{
		Rate: 100, Duration: 200 * time.Millisecond,
		MaxInFlight: 1, QueueDepth: 100,
		RequestTimeout: 150 * time.Millisecond, DrainTimeout: 200 * time.Millisecond,
	}, sleepDoer(100*time.Millisecond), nil)

	if res.Intended != 20 || res.Unfinished == 0 || res.OK == 0 {
		t.Errorf("intended=%d ok=%d unfinished=%d, want 20 intended with some ok and some unfinished",
			res.Intended, res.OK, res.Unfinished)
	}
}

func TestAbortStopsDispatching(t *testing.T) {
	e, err := New(Config{
		Rate: 100, Duration: time.Hour,
		MaxInFlight: 10, QueueDepth: 10,
		RequestTimeout: time.Second, DrainTimeout: time.Second,
	}, sleepDoer(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	res, err := e.Run(ctx, time.Now())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error = %v, want deadline exceeded", err)
	}
	checkAccounting(t, res)
	if res.Unfinished == 0 {
		t.Error("unfinished = 0, want the aborted in-flight requests counted")
	}
}

func TestConfigValidation(t *testing.T) {
	ok := Config{Rate: 1, Duration: time.Second, MaxInFlight: 1, RequestTimeout: time.Second, DrainTimeout: time.Second}
	if _, err := New(ok, sleepDoer(0)); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mut := range map[string]func(*Config){
		"rate":          func(c *Config) { c.Rate = 0 },
		"duration":      func(c *Config) { c.Duration = 0 },
		"in-flight":     func(c *Config) { c.MaxInFlight = 0 },
		"queue":         func(c *Config) { c.QueueDepth = -1 },
		"drain<request": func(c *Config) { c.DrainTimeout = c.RequestTimeout / 2 },
	} {
		c := ok
		mut(&c)
		if _, err := New(c, sleepDoer(0)); err == nil {
			t.Errorf("%s: invalid config accepted", name)
		}
	}
}
