package worker

import (
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/timingtest"
	"github.com/loadforge/loadforge/internal/transport"
)

// TestSnapshotsCarryTheStall is the standard coordinated-omission case
// (CLAUDE.md, workflow rule 2) end to end through the worker: 100 req/s, the
// server freezes for 2s from 1.5s, STOP at 4s. Merging the published
// snapshots, as the aggregator does, must show the stall: the ~200 requests
// intended during it complete at ~3.5s with latencies from ~2s down to ~0,
// so ≈ 100 are ≥ 1s and p99 (the top 4 of 400) is ≈ 1.96s.
func TestSnapshotsCarryTheStall(t *testing.T) {
	if testing.Short() {
		t.Skip("runs for ~5s")
	}
	timingtest.Exclusive(t) // asserts on dispatch lag: never alongside another such test
	h := newHarness(t, &memPub{}, func(c *Config) { c.SnapshotPeriod = time.Second })
	expectStatus(t, h.send(h.prepare("run-co", 1, 1, func(p *transport.Prepare) {
		p.Rate, p.MaxInFlight, p.QueueDepth = 100, 50, 1000 // the cap is reached during the stall
		p.RequestTimeoutMS, p.DrainTimeoutMS = 10_000, 10_000
	})), transport.StatusReady)
	h.heartbeats("run-co", 1)

	start := cmd(transport.CmdStart, "run-co", 2, 1)
	start.Start.StartAt = time.Now().Add(200 * time.Millisecond)
	expectStatus(t, h.send(start), transport.StatusArmed)
	t0 := time.Now().Add(time.Until(start.Start.StartAt))
	stall := time.AfterFunc(time.Until(t0.Add(1500*time.Millisecond)), func() { h.stub.Stall(2 * time.Second) })
	defer stall.Stop()

	time.Sleep(time.Until(t0.Add(4 * time.Second)))
	a := h.send(cmd(transport.CmdStop, "run-co", 3, 1))
	expectStatus(t, a, transport.StatusStopped)

	snaps := h.pub.delivered()
	checkStream(t, snaps, transport.ReasonStop)
	if len(snaps) < 4 {
		t.Fatalf("%d snapshots for a 4s run, want ≥ 4", len(snaps))
	}
	lat, lag := engine.NewHistogram(), engine.NewHistogram()
	var intended, ok, unfinished int64
	for _, s := range snaps {
		lat.Merge(s.Latency)
		lag.Merge(s.DispatchLag)
		intended += s.Intended
		ok += s.OK
		unfinished += s.Unfinished
	}
	us := func(v int64) time.Duration { return time.Duration(v) * time.Microsecond }

	if intended < 398 || intended > 402 || ok != intended || unfinished != 0 {
		t.Errorf("intended=%d ok=%d unfinished=%d, want ≈ 400 all ok", intended, ok, unfinished)
	}
	if p99 := us(lat.ValueAtQuantile(99)); p99 < 1800*time.Millisecond {
		t.Errorf("merged p99 = %v, want ≥ 1.8s: the stall is hidden", p99)
	}
	if max := us(lat.Max()); max < 1950*time.Millisecond || max > 2200*time.Millisecond {
		t.Errorf("merged max = %v, want ≈ 2s", max)
	}
	var slow int64
	for _, b := range lat.Distribution() {
		if b.From >= time.Second.Microseconds() {
			slow += b.Count
		}
	}
	if slow < 90 || slow > 115 {
		t.Errorf("%d requests ≥ 1s, want ≈ 100", slow)
	}
	// The stall is the server's: the generator kept its schedule.
	if p := us(lag.ValueAtQuantile(99)); p > 25*time.Millisecond {
		t.Errorf("dispatch lag p99 = %v: the server stall leaked into the dispatcher", p)
	}
	// The stall must land in the snapshots covering it, not only in the
	// last one: the interval holding ~3.5s has the slow completions.
	var stallSnap *transport.Snapshot
	for _, s := range snaps {
		if !s.IntervalStart.After(start.Start.StartAt.Add(3500*time.Millisecond)) &&
			s.IntervalEnd.After(start.Start.StartAt.Add(3500*time.Millisecond)) {
			stallSnap = s
		}
	}
	if stallSnap == nil || us(stallSnap.Latency.Max()) < 1900*time.Millisecond {
		t.Errorf("the snapshot covering 3.5s does not hold the stalled completions")
	}
	t.Logf("snapshots=%d intended=%d p99=%v max=%v ≥1s=%d lag p99=%v",
		len(snaps), intended, us(lat.ValueAtQuantile(99)), us(lat.Max()), slow, us(lag.ValueAtQuantile(99)))
}
