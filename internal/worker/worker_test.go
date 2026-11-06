package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/loadforge/loadforge/internal/stubserver"
	"github.com/loadforge/loadforge/internal/transport"
)

// Worker timings scaled down so the tests run in well under a second each.
const (
	testDeadMan   = 300 * time.Millisecond
	testHeartbeat = 50 * time.Millisecond
	silence       = 200 * time.Millisecond // how long a test waits to conclude "no reply"
)

func startNATS(t *testing.T) *nats.Conn {
	t.Helper()
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server did not start")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

type harness struct {
	t      *testing.T
	nc     *nats.Conn // the test plays the controller on this connection
	pub    *memPub
	w      *Worker
	target string
	stub   *stubserver.Server
}

func newHarness(t *testing.T, pub *memPub, mut ...func(*Config)) *harness {
	t.Helper()
	nc := startNATS(t)
	stub := stubserver.New(time.Millisecond)
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)

	cfg := Config{
		ID: "w1", RateCap: 10_000,
		DeadManTimeout: testDeadMan, HeartbeatInterval: testHeartbeat,
		SnapshotPeriod: 100 * time.Millisecond, PublishTimeout: time.Second, FinalTimeout: 5 * time.Second,
	}
	for _, m := range mut {
		m(&cfg)
	}
	w, err := New(cfg, nc, pub)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, nc: nc, pub: pub, w: w, target: srv.URL, stub: stub}
}

func (h *harness) prepare(runID string, seq, fence uint64, mut ...func(*transport.Prepare)) transport.Command {
	p := &transport.Prepare{
		Workers: []string{"w1"}, Method: "GET", TargetURL: h.target,
		Rate: 200, DurationMS: 60_000, MaxInFlight: 100, QueueDepth: 100,
		RequestTimeoutMS: 2000, DrainTimeoutMS: 3000,
	}
	for _, m := range mut {
		m(p)
	}
	return transport.Command{Type: transport.CmdPrepare, RunID: runID, CmdSeq: seq, FenceToken: fence,
		Deadline: time.Now().Add(5 * time.Second), Prepare: p}
}

func cmd(typ transport.CmdType, runID string, seq, fence uint64) transport.Command {
	c := transport.Command{Type: typ, RunID: runID, CmdSeq: seq, FenceToken: fence, Deadline: time.Now().Add(5 * time.Second)}
	switch typ {
	case transport.CmdStart:
		c.Start = &transport.Start{StartAt: time.Now().Add(100 * time.Millisecond)}
	case transport.CmdRate:
		c.Rate = &transport.Rate{Rate: 10, EffectiveAt: time.Now().Add(time.Second)}
	}
	return c
}

// send issues c and returns the raw reply, or nil if none came within wait.
func (h *harness) sendRaw(c transport.Command, wait time.Duration) []byte {
	h.t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		h.t.Fatal(err)
	}
	m, err := h.nc.Request(transport.CmdSubject(c.RunID), b, wait)
	if err != nil {
		return nil
	}
	return m.Data
}

// send issues c and decodes the ack, failing the test if none arrives.
func (h *harness) send(c transport.Command) transport.Ack {
	h.t.Helper()
	raw := h.sendRaw(c, 10*time.Second) // a PREPARE may spend up to ~4.5s warming up
	if raw == nil {
		h.t.Fatalf("%s seq %d: no reply", c.Type, c.CmdSeq)
	}
	var a transport.Ack
	if err := json.Unmarshal(raw, &a); err != nil {
		h.t.Fatal(err)
	}
	return a
}

func (h *harness) expectSilence(c transport.Command) {
	h.t.Helper()
	if raw := h.sendRaw(c, silence); raw != nil {
		h.t.Fatalf("%s seq %d fence %d: got reply %s, want none", c.Type, c.CmdSeq, c.FenceToken, raw)
	}
}

func expectStatus(t *testing.T, a transport.Ack, want transport.AckStatus) {
	t.Helper()
	if a.Status != want {
		t.Fatalf("ack %+v: status %q, want %q", a, a.Status, want)
	}
}

// heartbeats publishes the controller heartbeat for runID until the test
// ends or the returned function is called.
// heartbeats publishes heartbeats every testHeartbeat until stop is
// called. stop returns when no more can be sent, with the time the last one
// was published (zero if none was).
func (h *harness) heartbeats(runID string, fence uint64) (stop func() time.Time) {
	done, exited := make(chan struct{}), make(chan struct{})
	var last time.Time // written by the goroutine, read after it exits
	go func() {
		defer close(exited)
		tk := time.NewTicker(testHeartbeat)
		defer tk.Stop()
		b, _ := json.Marshal(transport.Heartbeat{RunID: runID, FenceToken: fence})
		for {
			select {
			case <-tk.C:
				if h.nc.Publish(transport.RunHeartbeatSubject(runID), b) == nil {
					last = time.Now()
				}
			case <-done:
				return
			}
		}
	}()
	var once sync.Once
	stop = func() time.Time {
		once.Do(func() { close(done) })
		<-exited
		return last
	}
	h.t.Cleanup(func() { stop() })
	return stop
}

// waitFinal waits for the final snapshot of a run and returns its stream.
func (h *harness) waitFinal(timeout time.Duration) []*transport.Snapshot {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snaps := h.pub.delivered()
		if n := len(snaps); n > 0 && snaps[n-1].Final {
			return snaps
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("no final snapshot within %v", timeout)
	return nil
}

func TestPrepareAcksReadyWithSignalsUnmeasured(t *testing.T) {
	h := newHarness(t, &memPub{})
	raw := h.sendRaw(h.prepare("run-1", 1, 1), 5*time.Second)
	var a transport.Ack
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, a, transport.StatusReady)
	if a.WorkerID != "w1" || a.RunID != "run-1" || a.CmdSeq != 1 {
		t.Errorf("ack %+v does not identify the worker, run and command", a)
	}
	if a.ClockOffsetNS != nil || a.CapacityRPS != nil {
		t.Errorf("ack reports clock offset %v / capacity %v, but neither is measured", a.ClockOffsetNS, a.CapacityRPS)
	}
}

func TestPrepareNacksWhatTheWorkerCannotRun(t *testing.T) {
	h := newHarness(t, &memPub{}, func(c *Config) { c.RateCap = 1000 })
	n := 0
	for name, mut := range map[string]func(*transport.Prepare){
		"over the rate cap":     func(p *transport.Prepare) { p.Rate = 1001 },
		"drain below timeout":   func(p *transport.Prepare) { p.DrainTimeoutMS = p.RequestTimeoutMS - 1 },
		"no in-flight capacity": func(p *transport.Prepare) { p.MaxInFlight = 0 },
		"bad url":               func(p *transport.Prepare) { p.TargetURL = "not a url" },
	} {
		n++
		a := h.send(h.prepare(fmt.Sprintf("run-%d", n), 1, 1, mut))
		expectStatus(t, a, transport.StatusNack)
		if a.Detail == "" {
			t.Errorf("%s: NACK without a detail", name)
		}
	}
	// None of the refused runs holds the worker.
	expectStatus(t, h.send(h.prepare("run-ok", 1, 1)), transport.StatusReady)
}

func TestWorkerNotListedStaysSilent(t *testing.T) {
	h := newHarness(t, &memPub{})
	h.expectSilence(h.prepare("run-1", 1, 1, func(p *transport.Prepare) { p.Workers = []string{"w2", "w3"} }))
	h.expectSilence(cmd(transport.CmdStart, "run-1", 2, 1))
	h.expectSilence(cmd(transport.CmdStop, "run-1", 3, 1))
	// An unknown run's commands other than PREPARE are not ours either.
	h.expectSilence(cmd(transport.CmdStart, "run-2", 1, 1))
}

func TestRepeatedCmdSeqGetsTheSameAck(t *testing.T) {
	h := newHarness(t, &memPub{})
	prep := h.prepare("run-1", 1, 1)
	first := h.sendRaw(prep, 5*time.Second)
	again := h.sendRaw(prep, 5*time.Second)
	if string(first) != string(again) {
		t.Errorf("repeated PREPARE: %s, then %s", first, again)
	}
	start := cmd(transport.CmdStart, "run-1", 2, 1)
	expectStatus(t, h.send(start), transport.StatusArmed)
	// A retry of the same START is answered "armed" again, not "not
	// prepared", and does not start a second run.
	expectStatus(t, h.send(start), transport.StatusArmed)

	stop := cmd(transport.CmdStop, "run-1", 3, 1)
	a1 := h.send(stop)
	a2 := h.send(stop)
	expectStatus(t, a1, transport.StatusStopped)
	if a2.FinalSeq == nil || *a2.FinalSeq != *a1.FinalSeq {
		t.Errorf("repeated STOP: final_seq %v then %v", a1.FinalSeq, a2.FinalSeq)
	}
	checkStream(t, h.pub.delivered(), transport.ReasonStop)
}

func TestStaleFenceAndOldCmdSeqAreIgnored(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 5)), transport.StatusReady)
	h.heartbeats("run-1", 6)

	h.expectSilence(cmd(transport.CmdStart, "run-1", 2, 4)) // older controller
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-1", 5, 6)), transport.StatusArmed)
	h.expectSilence(cmd(transport.CmdStop, "run-1", 6, 5)) // the token went up to 6
	h.expectSilence(cmd(transport.CmdStop, "run-1", 3, 6)) // cmd_seq went up to 5
	h.expectSilence(cmd(transport.CmdStop, "run-1", 1, 6)) // seq 1 was PREPARE: violation

	// The ignored STOPs did not stop the run.
	time.Sleep(300 * time.Millisecond)
	if snaps := h.pub.delivered(); len(snaps) == 0 || snaps[len(snaps)-1].Final {
		t.Fatalf("run not running after ignored STOPs: %d snapshots", len(snaps))
	}
	expectStatus(t, h.send(cmd(transport.CmdStop, "run-1", 6, 6)), transport.StatusStopped)
}

func TestStartRefusals(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	late := cmd(transport.CmdStart, "run-1", 2, 1)
	late.Start.StartAt = time.Now().Add(-time.Millisecond)
	a := h.send(late)
	expectStatus(t, a, transport.StatusNack)
	t.Logf("late START: %s", a.Detail)

	// The worker is busy with run-1, so it refuses run-2, and then a START
	// for the refused run.
	busy := h.send(h.prepare("run-2", 1, 1))
	expectStatus(t, busy, transport.StatusNack)
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-2", 2, 1)), transport.StatusNack)
	expectStatus(t, h.send(cmd(transport.CmdStop, "run-2", 3, 1)), transport.StatusNack)

	// A second PREPARE for a prepared run is refused too.
	expectStatus(t, h.send(h.prepare("run-1", 3, 1)), transport.StatusNack)
}

func TestRateIsNackedAsUnsupported(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	a := h.send(cmd(transport.CmdRate, "run-1", 2, 1))
	expectStatus(t, a, transport.StatusNack)
	if a.Detail != "unsupported" {
		t.Errorf("detail = %q, want unsupported", a.Detail)
	}
}

func TestFullRunPublishesContiguousStreamAndFinalSeq(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	h.heartbeats("run-1", 1)
	start := cmd(transport.CmdStart, "run-1", 2, 1)
	expectStatus(t, h.send(start), transport.StatusArmed)
	time.Sleep(650 * time.Millisecond)
	a := h.send(cmd(transport.CmdStop, "run-1", 3, 1))
	expectStatus(t, a, transport.StatusStopped)

	snaps := h.pub.delivered()
	checkStream(t, snaps, transport.ReasonStop)
	if a.FinalSeq == nil || *a.FinalSeq != snaps[len(snaps)-1].Seq {
		t.Fatalf("STOP ack final_seq %v, last snapshot seq %d", a.FinalSeq, snaps[len(snaps)-1].Seq)
	}
	// The ack came only after the final snapshot was delivered, so the
	// aggregator can rely on final_seq.
	if len(snaps) < 5 {
		t.Errorf("%d snapshots for ~550ms at a 100ms period, want ≥ 5", len(snaps))
	}
	var sum struct{ intended, ok, err, timeout, dropped, unfinished, lat int64 }
	for i, s := range snaps {
		if h.pub.keys[i] != "run-1:w1" {
			t.Errorf("snapshot %d key %q", i, h.pub.keys[i])
		}
		if i > 0 && !s.IntervalStart.Equal(snaps[i-1].IntervalEnd) {
			t.Errorf("snapshot %d starts at %v, previous ended at %v", i, s.IntervalStart, snaps[i-1].IntervalEnd)
		}
		sum.intended += s.Intended
		sum.ok += s.OK
		sum.err += s.Errors
		sum.timeout += s.Timeouts
		sum.dropped += s.Dropped
		sum.unfinished += s.Unfinished
		sum.lat += s.Latency.TotalCount()
	}
	if done := sum.ok + sum.err + sum.timeout + sum.dropped + sum.unfinished; done != sum.intended || sum.ok != sum.intended {
		t.Errorf("snapshots sum to intended=%d ok=%d err=%d timeout=%d dropped=%d unfinished=%d",
			sum.intended, sum.ok, sum.err, sum.timeout, sum.dropped, sum.unfinished)
	}
	if sum.intended < 100 || sum.intended > 120 {
		t.Errorf("intended = %d, want ≈ 110 (200 req/s for ~550ms)", sum.intended)
	}
	// t0 is the barrier: the first interval is labelled with start_at.
	if d := snaps[0].IntervalStart.Sub(start.Start.StartAt); d < -5*time.Millisecond || d > 5*time.Millisecond {
		t.Errorf("first interval starts %v from start_at, want within 5ms", d)
	}
}

func TestRunThatCompletesKeepsItsFinalSeqForStop(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1, func(p *transport.Prepare) { p.DurationMS = 250 })), transport.StatusReady)
	h.heartbeats("run-1", 1)
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-1", 2, 1)), transport.StatusArmed)
	snaps := h.waitFinal(5 * time.Second)
	checkStream(t, snaps, transport.ReasonCompleted)

	a := h.send(cmd(transport.CmdStop, "run-1", 3, 1))
	expectStatus(t, a, transport.StatusStopped)
	if *a.FinalSeq != snaps[len(snaps)-1].Seq {
		t.Errorf("final_seq %d, want %d", *a.FinalSeq, snaps[len(snaps)-1].Seq)
	}
	// The worker is free for the next run.
	expectStatus(t, h.send(h.prepare("run-2", 1, 2)), transport.StatusReady)
}

func TestStopBeforeStartPublishesOneEmptyFinal(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	a := h.send(cmd(transport.CmdStop, "run-1", 2, 1))
	expectStatus(t, a, transport.StatusStopped)
	snaps := h.pub.delivered()
	checkStream(t, snaps, transport.ReasonStop)
	if *a.FinalSeq != 0 || snaps[0].Intended != 0 {
		t.Errorf("final_seq=%d intended=%d, want 0 and 0", *a.FinalSeq, snaps[0].Intended)
	}
}

func TestStopBetweenStartAndBarrier(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	start := cmd(transport.CmdStart, "run-1", 2, 1)
	start.Start.StartAt = time.Now().Add(2 * time.Second)
	expectStatus(t, h.send(start), transport.StatusArmed)
	a := h.send(cmd(transport.CmdStop, "run-1", 3, 1))
	expectStatus(t, a, transport.StatusStopped)
	snaps := h.pub.delivered()
	checkStream(t, snaps, transport.ReasonStop)
	if snaps[0].Intended != 0 {
		t.Errorf("intended = %d, want 0: load was sent after STOP", snaps[0].Intended)
	}
}

func TestDeadManStopsLoadWithoutHeartbeats(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	stopHB := h.heartbeats("run-1", 1)
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-1", 2, 1)), transport.StatusArmed)

	// Heartbeats keep it running past the timeout.
	time.Sleep(2 * testDeadMan)
	if snaps := h.pub.delivered(); len(snaps) > 0 && snaps[len(snaps)-1].Final {
		t.Fatal("run stopped despite heartbeats")
	}
	// Measure from the last heartbeat actually sent, which can be up to
	// testHeartbeat before stopHB returns. The worker receives it no
	// earlier, so its switch must not fire sooner than testDeadMan after.
	lastHB := stopHB()
	if lastHB.IsZero() {
		t.Fatal("no heartbeat was sent")
	}
	snaps := h.waitFinal(5 * time.Second)
	checkStream(t, snaps, transport.ReasonDeadman)
	if d := time.Since(lastHB); d < testDeadMan {
		t.Errorf("stopped %v after the last heartbeat, before the %v timeout", d, testDeadMan)
	}
	// Late STOP from a controller that comes back still gets the final seq.
	a := h.send(cmd(transport.CmdStop, "run-1", 3, 1))
	expectStatus(t, a, transport.StatusStopped)
}

func TestDeadManIgnoresStaleHeartbeats(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 3)), transport.StatusReady)
	h.heartbeats("run-1", 2) // a controller that lost the lease
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-1", 2, 3)), transport.StatusArmed)
	checkStream(t, h.waitFinal(5*time.Second), transport.ReasonDeadman)
}

func TestDeadManTripsBeforeStart(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	checkStream(t, h.waitFinal(5*time.Second), transport.ReasonDeadman)
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-1", 2, 1)), transport.StatusNack)
}

func TestStopAckWaitsForFinalSnapshotDelivery(t *testing.T) {
	pub := &memPub{delay: 300 * time.Millisecond}
	h := newHarness(t, pub)
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	h.heartbeats("run-1", 1)
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-1", 2, 1)), transport.StatusArmed)
	time.Sleep(250 * time.Millisecond)
	a := h.send(cmd(transport.CmdStop, "run-1", 3, 1))
	ackAt := time.Now()
	expectStatus(t, a, transport.StatusStopped)

	pub.mu.Lock()
	defer pub.mu.Unlock()
	last := len(pub.snaps) - 1
	if last < 0 || !pub.snaps[last].Final || pub.snaps[last].Seq != *a.FinalSeq {
		t.Fatalf("STOP acked final_seq %d before that snapshot was delivered", *a.FinalSeq)
	}
	if pub.at[last].After(ackAt) {
		t.Errorf("final snapshot delivered %v after the ack", pub.at[last].Sub(ackAt))
	}
}

func TestStopAckReportsUndeliveredFinal(t *testing.T) {
	h := newHarness(t, &memPub{fail: true}, func(c *Config) { c.FinalTimeout = 500 * time.Millisecond })
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	a := h.send(cmd(transport.CmdStop, "run-1", 2, 1))
	expectStatus(t, a, transport.StatusStopped)
	if a.Detail == "" {
		t.Error("STOP ack does not say the final snapshot was lost")
	}
}

func TestCloseStopsTheRunWithReasonShutdown(t *testing.T) {
	h := newHarness(t, &memPub{})
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	h.heartbeats("run-1", 1)
	expectStatus(t, h.send(cmd(transport.CmdStart, "run-1", 2, 1)), transport.StatusArmed)
	time.Sleep(250 * time.Millisecond)
	if err := h.w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkStream(t, h.pub.delivered(), transport.ReasonShutdown)
	h.expectSilence(h.prepare("run-2", 1, 1)) // unsubscribed
}

func TestWorkerHeartbeatNamesItsRun(t *testing.T) {
	h := newHarness(t, &memPub{})
	sub, err := h.nc.SubscribeSync(transport.WorkerHeartbeatSubject("w1"))
	if err != nil {
		t.Fatal(err)
	}
	next := func() transport.WorkerHeartbeat {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var hb transport.WorkerHeartbeat
		if err := json.Unmarshal(m.Data, &hb); err != nil {
			t.Fatal(err)
		}
		return hb
	}
	if hb := next(); hb.WorkerID != "w1" || hb.RunID != "" {
		t.Errorf("idle heartbeat %+v", hb)
	}
	expectStatus(t, h.send(h.prepare("run-1", 1, 1)), transport.StatusReady)
	_ = next() // may have been sent before PREPARE
	if hb := next(); hb.RunID != "run-1" {
		t.Errorf("heartbeat %+v while prepared, want run_id run-1", hb)
	}
}
