package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/transport"
)

// memPub records every delivered snapshot. Publishing can be held with
// block, delayed, or made to fail.
type memPub struct {
	mu    sync.Mutex
	keys  []string
	snaps []*transport.Snapshot
	at    []time.Time // delivery times

	block chan struct{} // if set, Publish waits for it to close
	delay time.Duration
	fail  bool
}

func (p *memPub) Publish(ctx context.Context, key, value []byte) error {
	if p.block != nil {
		select {
		case <-p.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("broker unavailable")
	}
	s, err := transport.DecodeSnapshot(value)
	if err != nil {
		return err
	}
	p.keys = append(p.keys, string(key))
	p.snaps = append(p.snaps, s)
	p.at = append(p.at, time.Now())
	return nil
}

func (p *memPub) delivered() []*transport.Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*transport.Snapshot(nil), p.snaps...)
}

// gridRotator returns one interval per call, each holding one OK request,
// and records when it was called. slowOnce delays the given call.
type gridRotator struct {
	mu       sync.Mutex
	calls    []time.Time
	last     time.Time
	slowCall int
	slowFor  time.Duration
	stopped  bool
}

func (g *gridRotator) Rotate() (engine.Interval, bool) {
	g.mu.Lock()
	n := len(g.calls) + 1
	stopped := g.stopped
	g.mu.Unlock()
	if stopped {
		return engine.Interval{}, false
	}
	if n == g.slowCall {
		time.Sleep(g.slowFor)
	}
	now := time.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, now)
	iv := oneOK(g.last, now)
	g.last = now
	return iv, true
}

func (g *gridRotator) stop() engine.Interval {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stopped = true
	now := time.Now()
	return oneOK(g.last, now)
}

func oneOK(start, end time.Time) engine.Interval {
	iv := engine.Interval{Start: start, End: end,
		Latency: engine.NewHistogram(), SendDelay: engine.NewHistogram(), DispatchLag: engine.NewHistogram()}
	_ = iv.Latency.RecordValue(1000)
	_ = iv.SendDelay.RecordValue(10)
	_ = iv.DispatchLag.RecordValue(5)
	iv.Intended, iv.OK = 1, 1
	return iv
}

const testPeriod = 50 * time.Millisecond

func testEmitter(pub Publisher, buffer int) *Emitter {
	return NewEmitter(context.Background(), "run-1", "w1", pub, EmitterConfig{Period: testPeriod, Buffer: buffer, PublishTimeout: time.Second})
}

// checkStream asserts that snaps is seq 0..n-1 with exactly the last final.
func checkStream(t *testing.T, snaps []*transport.Snapshot, reason transport.StopReason) {
	t.Helper()
	if len(snaps) == 0 {
		t.Fatal("no snapshots delivered")
	}
	var want uint64
	for i, s := range snaps {
		if s.Seq != want {
			t.Fatalf("snapshot %d has seq %d: stream is not contiguous", i, s.Seq)
		}
		want++
		if last := i == len(snaps)-1; s.Final != last {
			t.Errorf("snapshot %d: final=%v, want %v", i, s.Final, last)
		}
	}
	if got := snaps[len(snaps)-1].Reason; got != reason {
		t.Errorf("final reason = %q, want %q", got, reason)
	}
}

func TestEmitterOneSnapshotPerGridPointEvenWhenLate(t *testing.T) {
	pub := &memPub{}
	em := testEmitter(pub, 16)
	// The 3rd rotation takes 2.5 periods. The 4th and 5th grid points are
	// then overdue and must still get their own snapshots, not be merged.
	g := &gridRotator{slowCall: 3, slowFor: 5 * testPeriod / 2}
	t0 := time.Now()
	g.last = t0
	em.Start(g, t0)
	time.Sleep(8*testPeriod + testPeriod/2)
	final := g.stop()
	seq, err := em.Finish(context.Background(), final, transport.ReasonStop)
	if err != nil {
		t.Fatal(err)
	}

	snaps := pub.delivered()
	checkStream(t, snaps, transport.ReasonStop)
	if seq != 8 || len(snaps) != 9 {
		t.Errorf("final seq %d with %d snapshots, want 8 rotations + final = seq 8", seq, len(snaps))
	}
	// Rotation k (1-based) happens no earlier than its grid point.
	for k, at := range g.calls {
		if grid := t0.Add(time.Duration(k+1) * testPeriod); at.Before(grid) {
			t.Errorf("rotation %d at %v, before its grid point %v", k+1, at.Sub(t0), grid.Sub(t0))
		}
	}
	// Labels are wall(t0) + monotonic elapsed time: the first starts at
	// exactly t0's wall reading and each starts where the previous ended.
	if !snaps[0].IntervalStart.Equal(t0.Round(0)) {
		t.Errorf("first label %v, want t0's wall time %v", snaps[0].IntervalStart, t0.Round(0))
	}
	for i := 1; i < len(snaps); i++ {
		if !snaps[i].IntervalStart.Equal(snaps[i-1].IntervalEnd) {
			t.Errorf("snapshot %d starts at %v, previous ended at %v", i, snaps[i].IntervalStart, snaps[i-1].IntervalEnd)
		}
	}
	var ok int64
	for _, s := range snaps {
		ok += s.OK
	}
	if ok != int64(len(snaps)) {
		t.Errorf("snapshots carry %d requests, want %d: an interval was lost", ok, len(snaps))
	}
}

func TestEmitterRotationDoesNotWaitOnPublisher(t *testing.T) {
	pub := &memPub{block: make(chan struct{})}
	em := testEmitter(pub, 2)
	g := &gridRotator{}
	t0 := time.Now()
	g.last = t0
	em.Start(g, t0)
	time.Sleep(10*testPeriod + testPeriod/2)

	// With the publisher stuck, rotation still ran on every grid point.
	g.mu.Lock()
	rotations := len(g.calls)
	g.mu.Unlock()
	if rotations != 10 {
		t.Fatalf("%d rotations in 10 periods with the publisher blocked, want 10", rotations)
	}
	// 1 snapshot held by the publisher + 2 buffered; the other 7 dropped.
	// Rotate records the call before emit encodes and drops the snapshot,
	// so give the 10th emit a moment to finish rather than racing it. The
	// 11th grid point is a full period away.
	for deadline := time.Now().Add(testPeriod / 2); em.Failed() < 7 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if f := em.Failed(); f != 7 {
		t.Errorf("failed = %d, want 7", f)
	}

	close(pub.block)
	seq, err := em.Finish(context.Background(), g.stop(), transport.ReasonStop)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 10 {
		t.Errorf("final seq = %d, want 10: dropped snapshots must still use up their seq", seq)
	}
	snaps := pub.delivered()
	got := make([]uint64, len(snaps))
	for i, s := range snaps {
		got[i] = s.Seq
	}
	// Delivered in order, the dropped ones leaving gaps the aggregator sees.
	want := []uint64{0, 1, 2, 10}
	if len(got) != len(want) {
		t.Fatalf("delivered seqs %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivered seqs %v, want %v", got, want)
		}
	}
}

func TestEmitterFinishReportsUndeliveredFinal(t *testing.T) {
	pub := &memPub{fail: true}
	em := testEmitter(pub, 4)
	now := time.Now()
	seq, err := em.Finish(context.Background(), engine.Interval{Start: now, End: now}, transport.ReasonDeadman)
	if err == nil {
		t.Error("Finish succeeded although the final snapshot was not delivered")
	}
	if seq != 0 || em.Failed() != 1 {
		t.Errorf("seq=%d failed=%d, want 0 and 1", seq, em.Failed())
	}
}

func TestEmitterFinishHonoursDeadline(t *testing.T) {
	pub := &memPub{block: make(chan struct{})}
	defer close(pub.block)
	em := testEmitter(pub, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	now := time.Now()
	if _, err := em.Finish(ctx, engine.Interval{Start: now, End: now}, transport.ReasonStop); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Finish error = %v, want deadline exceeded", err)
	}
	if d := time.Since(begin); d > 500*time.Millisecond {
		t.Errorf("Finish took %v with a 100ms deadline", d)
	}
}

func TestEmitterUnstartedRunHasOneEmptyFinal(t *testing.T) {
	pub := &memPub{}
	em := testEmitter(pub, 4)
	now := time.Now()
	if _, err := em.Finish(context.Background(), engine.Interval{Start: now, End: now}, transport.ReasonStop); err != nil {
		t.Fatal(err)
	}
	snaps := pub.delivered()
	checkStream(t, snaps, transport.ReasonStop)
	if s := snaps[0]; s.Intended != 0 || s.Latency.TotalCount() != 0 {
		t.Errorf("final snapshot of an unstarted run carries data: %+v", s)
	}
	if pub.keys[0] != "run-1:w1" {
		t.Errorf("key = %q, want run-1:w1", pub.keys[0])
	}
}

// IntervalNS must come from the monotonic readings the engine stamped, not
// from the wall-clock labels the emitter writes.
func TestEmitterIntervalNSIsMonotonicLength(t *testing.T) {
	pub := &memPub{}
	em := testEmitter(pub, 4)
	t0 := time.Now()
	g := &gridRotator{last: t0}
	em.Start(g, t0)
	time.Sleep(3*testPeriod + testPeriod/2)
	final := g.stop()
	if _, err := em.Finish(context.Background(), final, transport.ReasonStop); err != nil {
		t.Fatal(err)
	}
	snaps := pub.delivered()
	checkStream(t, snaps, transport.ReasonStop)

	g.mu.Lock()
	ends := append(append([]time.Time{t0}, g.calls...), final.End)
	g.mu.Unlock()
	if len(ends) != len(snaps)+1 {
		t.Fatalf("%d intervals for %d snapshots", len(ends)-1, len(snaps))
	}
	var total int64
	for i, s := range snaps {
		want := int64(ends[i+1].Sub(ends[i]))
		if s.IntervalNS != want {
			t.Errorf("seq %d: interval_ns = %d, want %d", s.Seq, s.IntervalNS, want)
		}
		total += s.IntervalNS
	}
	// The intervals tile the run, so they sum to its monotonic length.
	if want := int64(final.End.Sub(t0)); total != want {
		t.Errorf("interval_ns sums to %d, run lasted %d", total, want)
	}
}

func TestEmitterUnstartedRunHasZeroIntervalNS(t *testing.T) {
	pub := &memPub{}
	em := testEmitter(pub, 4)
	now := time.Now()
	if _, err := em.Finish(context.Background(), engine.Interval{Start: now, End: now}, transport.ReasonStop); err != nil {
		t.Fatal(err)
	}
	if s := pub.delivered(); len(s) != 1 || s[0].IntervalNS != 0 {
		t.Fatalf("unstarted run: %+v", s)
	}
}
