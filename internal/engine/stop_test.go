package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// rotateOnGrid rotates e at t0 + k·period, as the worker's snapshot emitter
// does, until Rotate reports the run finished. The returned channel yields
// every interval once rotation has ended.
func rotateOnGrid(e *Engine, t0 time.Time, period time.Duration) <-chan []Interval {
	out := make(chan []Interval, 1)
	go func() {
		var ivs []Interval
		for k := 1; ; k++ {
			time.Sleep(time.Until(t0.Add(time.Duration(k) * period)))
			iv, ok := e.Rotate()
			if !ok {
				out <- ivs
				return
			}
			ivs = append(ivs, iv)
		}
	}()
	return out
}

// checkIntervalsCoverRun asserts that the rotated intervals followed by
// res.Final tile [t0, Total.End] with no gap or overlap, and that their sum
// is exactly res.Total. It returns the sum, which is what the aggregator
// sees.
func checkIntervalsCoverRun(t *testing.T, t0 time.Time, rotated []Interval, res Result) Interval {
	t.Helper()
	ivs := append(rotated, res.Final)
	if !ivs[0].Start.Equal(t0) {
		t.Errorf("first interval starts %v after t0, want at t0", ivs[0].Start.Sub(t0))
	}
	for k := 1; k < len(ivs); k++ {
		if !ivs[k].Start.Equal(ivs[k-1].End) {
			t.Errorf("interval %d starts at %v, previous ended at %v: gap or overlap",
				k, ivs[k].Start.Sub(t0), ivs[k-1].End.Sub(t0))
		}
	}
	if !res.Final.End.Equal(res.Total.End) {
		t.Errorf("final interval ends %v before the run", res.Total.End.Sub(res.Final.End))
	}

	sum := *newInterval()
	for k := range ivs {
		sum.merge(&ivs[k])
	}
	got := [...]int64{sum.Intended, sum.OK, sum.Errors, sum.Timeouts, sum.Dropped, sum.Unfinished, sum.Clamped,
		sum.Latency.TotalCount(), sum.SendDelay.TotalCount(), sum.DispatchLag.TotalCount()}
	tot := res.Total
	want := [...]int64{tot.Intended, tot.OK, tot.Errors, tot.Timeouts, tot.Dropped, tot.Unfinished, tot.Clamped,
		tot.Latency.TotalCount(), tot.SendDelay.TotalCount(), tot.DispatchLag.TotalCount()}
	if got != want {
		t.Errorf("intervals sum to %v, want totals %v (intended ok err timeout dropped unfinished clamped |lat| |send| |lag|)", got, want)
	}
	if sum.Latency.Max() != tot.Latency.Max() {
		t.Errorf("intervals' latency max = %v, totals' = %v", dur(sum.Latency.Max()), dur(tot.Latency.Max()))
	}
	return sum
}

func TestIntervalsTileTheRun(t *testing.T) {
	e, err := New(Config{
		Rate: 200, Duration: time.Second,
		MaxInFlight: 50, QueueDepth: 50,
		RequestTimeout: time.Second, DrainTimeout: time.Second,
	}, sleepDoer(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.Rotate(); ok {
		t.Error("Rotate before Run reported an interval")
	}

	t0 := time.Now().Add(startLead)
	// 300ms does not divide the 1s run, so the last grid rotation is at
	// 900ms and the requests of the last ~100ms can only arrive in Final.
	rotated := rotateOnGrid(e, t0, 300*time.Millisecond)
	res, err := e.Run(context.Background(), t0)
	if err != nil {
		t.Fatal(err)
	}
	ivs := <-rotated
	checkAccounting(t, res.Total)
	if len(ivs) != 3 {
		t.Fatalf("got %d rotated intervals, want 3 for a 1s run rotated every 300ms", len(ivs))
	}
	checkIntervalsCoverRun(t, t0, ivs, res)
	if res.Total.Intended != 200 {
		t.Errorf("intended = %d, want 200", res.Total.Intended)
	}
	if res.Final.Intended < 15 || res.Final.OK == 0 {
		t.Error("final interval is empty; the end of the run would be lost")
	}
	if _, ok := e.Rotate(); ok {
		t.Error("Rotate after Run reported an interval after Final")
	}
}

func TestStopDrainsInFlightRequests(t *testing.T) {
	// Every response is held until 500ms; Stop comes at 300ms. Stopping must
	// not cut off the held requests: they complete during the drain and keep
	// their full latency.
	t0 := time.Now().Add(startLead)
	gate := make(chan struct{})
	time.AfterFunc(time.Until(t0.Add(500*time.Millisecond)), func() { close(gate) })

	e, err := New(Config{
		Rate: 100, Duration: time.Hour,
		MaxInFlight: 100, QueueDepth: 0,
		RequestTimeout: 5 * time.Second, DrainTimeout: 5 * time.Second,
	}, gateDoer(gate))
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(time.Until(t0.Add(300*time.Millisecond)), e.Stop)
	out, err := e.Run(context.Background(), t0)
	if err != nil {
		t.Fatalf("Run after Stop: %v, want nil", err)
	}
	res := out.Total
	checkAccounting(t, res)

	if res.Intended < 29 || res.Intended > 32 {
		t.Errorf("intended = %d, want ≈ 30 (requests intended before the 300ms stop)", res.Intended)
	}
	if res.OK != res.Intended || res.Unfinished != 0 {
		t.Errorf("ok=%d unfinished=%d of %d intended: Stop cut off in-flight requests", res.OK, res.Unfinished, res.Intended)
	}
	if max := dur(res.Latency.Max()); max < 450*time.Millisecond {
		t.Errorf("latency max = %v, want ≥ 450ms (request 0 waited for the gate)", max)
	}
}

func TestStopReleasesRequestsTheDispatcherIsBehindOn(t *testing.T) {
	// The dispatcher freezes for 200ms from 300ms, and Stop arrives at 400ms
	// while it is frozen. The ~10 requests intended in [300ms, 400ms) were
	// due before the stop: they must be released late with their lag
	// recorded, not dropped from the count.
	var stalled atomic.Bool
	t0 := time.Now().Add(startLead)
	e, err := New(Config{
		Rate: 100, Duration: time.Hour,
		MaxInFlight: 50, QueueDepth: 50,
		RequestTimeout: time.Second, DrainTimeout: time.Second,
	}, sleepDoer(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	e.sleepUntil = func(ctx context.Context, at time.Time) error {
		if at.Sub(t0) >= 300*time.Millisecond && stalled.CompareAndSwap(false, true) {
			time.Sleep(200 * time.Millisecond)
		}
		return sleepUntil(ctx, at)
	}
	time.AfterFunc(time.Until(t0.Add(400*time.Millisecond)), e.Stop)
	out, err := e.Run(context.Background(), t0)
	if err != nil {
		t.Fatal(err)
	}
	res := out.Total
	checkAccounting(t, res)

	if res.Intended < 39 || res.Intended > 42 {
		t.Errorf("intended = %d, want ≈ 40: requests due before Stop were not released", res.Intended)
	}
	if max := dur(res.DispatchLag.Max()); max < 190*time.Millisecond {
		t.Errorf("dispatch lag max = %v, want ≥ 190ms: the dispatcher stall was hidden by Stop", max)
	}
}

func TestStopBeforeRunReleasesNothing(t *testing.T) {
	e, err := New(Config{
		Rate: 100, Duration: time.Hour,
		MaxInFlight: 1, RequestTimeout: time.Second, DrainTimeout: time.Second,
	}, sleepDoer(0))
	if err != nil {
		t.Fatal(err)
	}
	e.Stop()
	e.Stop() // idempotent
	t0 := time.Now().Add(time.Second)
	out, err := e.Run(context.Background(), t0)
	if err != nil {
		t.Fatal(err)
	}
	checkAccounting(t, out.Total)
	if out.Total.Intended != 0 {
		t.Errorf("intended = %d, want 0", out.Total.Intended)
	}
	// Run returns at once without waiting for t0, and the empty final
	// interval is [t0, t0], not one that ends before it starts.
	if !out.Final.Start.Equal(t0) || !out.Final.End.Equal(t0) || !out.Total.End.Equal(t0) {
		t.Errorf("final = [%v, %v] relative to t0, want [0, 0]", out.Final.Start.Sub(t0), out.Final.End.Sub(t0))
	}
	e.Stop() // after Run: no effect, no panic
}
