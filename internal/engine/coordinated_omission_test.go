package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/loadforge/loadforge/internal/stubserver"
	"github.com/loadforge/loadforge/internal/timingtest"
)

// The standard coordinated-omission scenario (CLAUDE.md, workflow rule 2):
// 100 req/s for 6s against a 1ms server that freezes for 2s starting 1.5s in.
//
// Requests intended during the stall, t_i ∈ [1.5s, 3.5s), all complete at
// ~3.5s, so their true latencies are spread evenly from ~2s down to ~0. That
// is 200 of 600 requests, so:
//   - p99 (the top 6 values) is ≈ 1.95s
//   - ≈ 100 requests (t_i ∈ [1.5s, 2.5s]) see ≥ 1s
const (
	coRate     = 100
	coDuration = 6 * time.Second
	coStallAt  = 1500 * time.Millisecond
	coStall    = 2 * time.Second
	coLatency  = time.Millisecond
)

func startStalledServer(t *testing.T, t0 time.Time) *httptest.Server {
	t.Helper()
	stub := stubserver.New(coLatency)
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)
	timer := time.AfterFunc(time.Until(t0.Add(coStallAt)), func() { stub.Stall(coStall) })
	t.Cleanup(func() { timer.Stop() })
	return srv
}

func newClient(t *testing.T) *http.Client {
	tr := &http.Transport{MaxIdleConns: 1000, MaxIdleConnsPerHost: 1000}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr}
}

func countAtLeast(h *hdrhistogram.Histogram, d time.Duration) int64 {
	var n int64
	for _, b := range h.Distribution() {
		if b.From >= d.Microseconds() {
			n += b.Count
		}
	}
	return n
}

func TestOpenLoop_TwoSecondStallAppearsInTail(t *testing.T) {
	if testing.Short() {
		t.Skip("runs for ~12s")
	}
	timingtest.Exclusive(t) // asserts on dispatch lag: never alongside another such test
	for _, c := range []struct {
		name                    string
		maxInFlight, queueDepth int
	}{
		// Every request is sent on time and held open by the server.
		{"ample capacity", 1000, 0},
		// 50 requests block in flight; the other ~150 wait in the local
		// queue and are sent only after the stall. Their latency is only
		// correct if it is measured from t_i, not from the actual send.
		{"in-flight cap reached", 50, 1000},
	} {
		t.Run(c.name, func(t *testing.T) {
			testStallAppearsInTail(t, c.maxInFlight, c.queueDepth)
		})
	}
}

func testStallAppearsInTail(t *testing.T, maxInFlight, queueDepth int) {
	t0 := time.Now().Add(100 * time.Millisecond)
	srv := startStalledServer(t, t0)

	e, err := New(Config{
		Rate: coRate, Duration: coDuration,
		MaxInFlight: maxInFlight, QueueDepth: queueDepth,
		RequestTimeout: 10 * time.Second, DrainTimeout: 10 * time.Second,
	}, &HTTPDoer{Client: newClient(t), Method: http.MethodGet, URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	out, err := e.Run(context.Background(), t0)
	if err != nil {
		t.Fatal(err)
	}
	res := out.Total
	checkAccounting(t, res)

	want := int64(coRate * coDuration / time.Second)
	if res.Intended != want || res.OK != want {
		t.Errorf("intended=%d ok=%d, want %d each: requests were skipped or failed", res.Intended, res.OK, want)
	}
	if p99 := p(res.Latency, 99); p99 < 1800*time.Millisecond {
		t.Errorf("latency p99 = %v, want ≥ 1.8s: the 2s stall is hidden", p99)
	}
	if max := dur(res.Latency.Max()); max < 1950*time.Millisecond || max > 2200*time.Millisecond {
		t.Errorf("latency max = %v, want ≈ 2s (1.95s–2.2s)", max)
	}
	if n := countAtLeast(res.Latency, time.Second); n < 90 || n > 115 {
		t.Errorf("%d requests ≥ 1s, want ≈ 100 (90–115): the stall's shape is wrong", n)
	}
	if p50 := p(res.Latency, 50); p50 > 100*time.Millisecond {
		t.Errorf("latency p50 = %v, want < 100ms: requests outside the stall should be fast", p50)
	}
	// The server stalled, not the generator: lag must stay healthy.
	if lag := p(res.DispatchLag, 99); lag > healthyLagP99 {
		t.Errorf("dispatch lag p99 = %v, want ≤ %v: the server stall leaked into the dispatcher", lag, healthyLagP99)
	}
	t.Logf("open loop: p50=%v p99=%v max=%v ≥1s=%d lag p99=%v send-delay max=%v",
		p(res.Latency, 50), p(res.Latency, 99), dur(res.Latency.Max()), countAtLeast(res.Latency, time.Second),
		p(res.DispatchLag, 99), dur(res.SendDelay.Max()))
}

// TestClosedLoop_HidesStall_Contrast runs the same scenario through a naive
// closed-loop generator that measures from the actual send time. It shows
// that the open-loop assertions above catch coordinated omission: here the
// stall is a single slow sample and p99 looks healthy.
func TestClosedLoop_HidesStall_Contrast(t *testing.T) {
	if testing.Short() {
		t.Skip("runs for ~6s")
	}
	timingtest.Exclusive(t) // asserts on dispatch lag: never alongside another such test
	t0 := time.Now().Add(100 * time.Millisecond)
	srv := startStalledServer(t, t0)
	doer := &HTTPDoer{Client: newClient(t), Method: http.MethodGet, URL: srv.URL}
	h := NewHistogram()

	time.Sleep(time.Until(t0))
	ticker := time.NewTicker(time.Second / coRate) // drops ticks while blocked
	defer ticker.Stop()
	for time.Since(t0) < coDuration {
		<-ticker.C
		sent := time.Now()
		if err := doer.Do(context.Background()); err != nil {
			t.Fatal(err)
		}
		record(h, time.Since(sent)) // the bug: measured from the actual send
	}

	if h.TotalCount() >= coRate*int64(coDuration/time.Second)-100 {
		t.Errorf("closed loop sent %d requests; expected it to fall ~200 short during the stall", h.TotalCount())
	}
	if p99 := p(h, 99); p99 > 100*time.Millisecond {
		t.Errorf("closed-loop p99 = %v; expected it to hide the stall (< 100ms)", p99)
	}
	t.Logf("closed loop: sent=%d p99=%v max=%v", h.TotalCount(), p(h, 99), dur(h.Max()))
}

// TestGracefulStopDuringStall_RecordsStall stops the run in the middle of the
// standard stall, as STOP or the dead-man's switch does. The requests held by
// the frozen server must drain and record their full latency rather than be
// cut off, and the stall must be visible in the per-interval histograms the
// worker publishes, not only in the run totals.
//
// Stop at 2.5s: requests intended in [0, 2.5s) are released, ≈ 250. The 100
// intended during the stall, t_i ∈ [1.5s, 2.5s), complete at ~3.5s with
// latencies from ~2s down to ~1s, so p99 (the top 2–3 values) is ≈ 1.98s.
func TestGracefulStopDuringStall_RecordsStall(t *testing.T) {
	if testing.Short() {
		t.Skip("runs for ~8s")
	}
	timingtest.Exclusive(t) // asserts on dispatch lag: never alongside another such test
	for _, c := range []struct {
		name                    string
		maxInFlight, queueDepth int
	}{
		{"ample capacity", 1000, 0},
		// At the stop, 50 requests are blocked in flight and ~50 wait in
		// the queue. Draining must still execute the queued ones.
		{"in-flight cap reached", 50, 1000},
	} {
		t.Run(c.name, func(t *testing.T) {
			testGracefulStopDuringStall(t, c.maxInFlight, c.queueDepth)
		})
	}
}

func testGracefulStopDuringStall(t *testing.T, maxInFlight, queueDepth int) {
	const stopAt = 2500 * time.Millisecond
	t0 := time.Now().Add(100 * time.Millisecond)
	srv := startStalledServer(t, t0)

	e, err := New(Config{
		Rate: coRate, Duration: time.Hour,
		MaxInFlight: maxInFlight, QueueDepth: queueDepth,
		RequestTimeout: 10 * time.Second, DrainTimeout: 10 * time.Second,
	}, &HTTPDoer{Client: newClient(t), Method: http.MethodGet, URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	stop := time.AfterFunc(time.Until(t0.Add(stopAt)), e.Stop)
	t.Cleanup(func() { stop.Stop() })
	rotated := rotateOnGrid(e, t0, time.Second)

	res, err := e.Run(context.Background(), t0)
	if err != nil {
		t.Fatalf("Run after Stop: %v, want nil", err)
	}
	checkAccounting(t, res.Total)
	merged := checkIntervalsCoverRun(t, t0, <-rotated, res)

	if merged.Intended < 248 || merged.Intended > 252 {
		t.Errorf("intended = %d, want ≈ 250 (released before the stop at %v)", merged.Intended, stopAt)
	}
	if merged.OK != merged.Intended || merged.Unfinished != 0 || merged.Dropped != 0 {
		t.Errorf("ok=%d unfinished=%d dropped=%d of %d: Stop cut off requests held by the stall",
			merged.OK, merged.Unfinished, merged.Dropped, merged.Intended)
	}
	if p99 := p(merged.Latency, 99); p99 < 1800*time.Millisecond {
		t.Errorf("latency p99 = %v, want ≥ 1.8s: the 2s stall is hidden", p99)
	}
	if max := dur(merged.Latency.Max()); max < 1950*time.Millisecond || max > 2200*time.Millisecond {
		t.Errorf("latency max = %v, want ≈ 2s (1.95s–2.2s)", max)
	}
	if n := countAtLeast(merged.Latency, time.Second); n < 90 || n > 110 {
		t.Errorf("%d requests ≥ 1s, want ≈ 100 (90–110): the stall's shape is wrong", n)
	}
	if end := res.Final.End.Sub(t0); end < coStallAt+coStall {
		t.Errorf("run ended at %v, before the stall cleared at %v: it did not drain", end, coStallAt+coStall)
	}
	if lag := p(merged.DispatchLag, 99); lag > healthyLagP99 {
		t.Errorf("dispatch lag p99 = %v, want ≤ %v", lag, healthyLagP99)
	}
	t.Logf("graceful stop: intended=%d p99=%v max=%v ≥1s=%d final=[%v, %v]",
		merged.Intended, p(merged.Latency, 99), dur(merged.Latency.Max()),
		countAtLeast(merged.Latency, time.Second), res.Final.Start.Sub(t0), res.Final.End.Sub(t0))
}
