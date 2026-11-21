package controlplane

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
	"github.com/loadforge/loadforge/internal/worker"
)

func TestWorkerLostMidRunAborts(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	id := h.create(func(r *RunRequest) { r.DurationMS = 30_000 })
	h.waitState(id, runstate.Running, 10*time.Second)
	time.Sleep(200 * time.Millisecond)
	silent := time.Now()
	h.wconns["w2"].Close() // w2 vanishes: no heartbeats, no acks

	run := h.waitTerminal(id, 15*time.Second)
	if run.State != runstate.Aborted || run.Verdict != health.Invalid {
		t.Fatalf("state %s verdict %s", run.State, run.Verdict)
	}
	ws := h.workerRows(id)
	if !ws["w2"].Lost || ws["w1"].Lost {
		t.Errorf("lost flags: w1 %v w2 %v", ws["w1"].Lost, ws["w2"].Lost)
	}
	if ws["w1"].FinalSeq == nil {
		t.Error("w1 was not stopped with a final_seq")
	}
	if f := h.finalSnapshot(id, "w1"); f == nil || f.Reason != transport.ReasonStop {
		t.Errorf("w1 final %+v", f)
	}
	var lostAt time.Time
	for _, e := range h.events(id) {
		if e.Kind == store.EventWorkerLost {
			lostAt = e.At
		}
	}
	if lostAt.IsZero() || lostAt.Sub(silent) > h.cfg.WorkerSilence+time.Second {
		t.Errorf("worker_lost recorded at %v after going silent", lostAt.Sub(silent))
	}
	if !run.FinalizeRequested {
		t.Error("finalize not requested")
	}
	var detail map[string]any
	_ = json.Unmarshal(run.VerdictDetail, &detail)
	if detail["verdict"] != string(health.Invalid) || detail["abort_reason"] == nil {
		t.Errorf("verdict_detail %s", run.VerdictDetail)
	}
}

// After the lease is lost the controller must stop heartbeats and write
// nothing more; the run is then recovered under a higher fence.
func TestLeaseLossStopsHeartbeatsAndWrites(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	id := h.create(func(r *RunRequest) { r.DurationMS = 30_000 })
	orig := h.run(id).FenceToken
	hbs := h.recordHeartbeats(id)
	h.waitState(id, runstate.Running, 10*time.Second)
	time.Sleep(200 * time.Millisecond)

	lostAt := time.Now()
	h.leases.expire(store.RunLeaseKey(id)) // e.g. Redis restarted

	run := h.waitTerminal(id, 20*time.Second)
	if run.State != runstate.Aborted {
		t.Fatalf("state %s", run.State)
	}
	if last := hbs.lastAt(orig); last.Sub(lostAt) > h.cfg.RenewInterval+2*h.cfg.HeartbeatInterval {
		t.Errorf("heartbeats with the lost fence continued %v after the lease was lost", last.Sub(lostAt))
	}
	// Nothing was written with the original token once the loss was noticed
	// (at the next renewal at the latest).
	for _, w := range h.st.writesSince(id, lostAt.Add(h.cfg.RenewInterval+50*time.Millisecond)) {
		if w.token == orig && w.ok {
			t.Errorf("write %q accepted with the lost token %d", w.op, orig)
		}
	}
	var recovered bool
	for _, e := range h.events(id) {
		if e.Kind == store.EventRecovered && e.FenceToken > orig {
			recovered = true
		}
	}
	if !recovered {
		t.Error("run was not recovered under a higher fence")
	}
}

// If Redis is unreachable, renewals fail, the heartbeat gate closes, and
// workers stop before the lease could have expired.
func TestRedisUnreachableClosesTheHeartbeatGate(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	id := h.create(func(r *RunRequest) { r.DurationMS = 30_000 })
	orig := h.run(id).FenceToken
	hbs := h.recordHeartbeats(id)
	h.waitState(id, runstate.Running, 10*time.Second)
	time.Sleep(200 * time.Millisecond)

	failAt := time.Now()
	h.leases.setFailRenew(true)
	// The last confirmed renewal was sent in [failAt − RenewInterval,
	// failAt], so the lease cannot expire before failAt − RenewInterval +
	// LeaseTTL. Every worker must have stopped by then.
	expiryBound := failAt.Add(h.cfg.LeaseTTL - h.cfg.RenewInterval)
	for _, w := range h.workerIDs() {
		f := h.waitFinal(id, w, time.Until(expiryBound))
		if f.Reason != transport.ReasonDeadman && f.Reason != transport.ReasonStop {
			t.Errorf("%s final reason %s", w, f.Reason)
		}
	}
	run := h.waitTerminal(id, 20*time.Second)
	if run.State != runstate.Aborted {
		t.Fatalf("state %s", run.State)
	}
	// Heartbeats end within the gate of the last confirmation, which is no
	// later than failAt.
	if last := hbs.lastAt(orig); last.Sub(failAt) > h.cfg.HeartbeatGate()+h.cfg.HeartbeatInterval {
		t.Errorf("heartbeats continued %v after renewals started failing (gate %v)", last.Sub(failAt), h.cfg.HeartbeatGate())
	}
	if r := lastReason(h.events(id)); !contains(r, "heartbeat") {
		t.Errorf("reason %q", r)
	}
}

// A guarded write that comes back stale makes the driver drop the run at
// once: no more commands with its token.
func TestStaleFenceStopsTheDriver(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	id := h.create(func(r *RunRequest) { r.DurationMS = 1500 })
	orig := h.run(id).FenceToken
	rec := h.recordCommands(id)
	h.waitState(id, runstate.Running, 10*time.Second)
	newer := h.st.bumpFence(id) // another controller wrote with a higher token

	run := h.waitTerminal(id, 20*time.Second)
	if run.State != runstate.Aborted {
		t.Fatalf("state %s", run.State)
	}
	for _, c := range rec.all() {
		if c.FenceToken == orig && c.Type == transport.CmdStop {
			t.Errorf("STOP sent with the stale token %d", orig)
		}
	}
	for _, e := range h.events(id) {
		if e.Kind == store.EventTransition && e.ToState == runstate.Aborted && e.FenceToken <= newer {
			t.Errorf("ABORTED written with token %d, not a recovery token above %d", e.FenceToken, newer)
		}
	}
}

func TestOrphanRecovery(t *testing.T) {
	h := newHarness(t, harnessOpts{noStart: true})
	ctx := context.Background()
	prep := transport.Prepare{Workers: []string{"w1", "w2"}, Method: "GET", TargetURL: h.target, Rate: 100, DurationMS: 30_000,
		MaxInFlight: 50, QueueDepth: 100, RequestTimeoutMS: 500, DrainTimeoutMS: 500}

	// A controller with token 5 created "orphan", prepared and started its
	// workers, logged its commands, then crashed.
	h.st.seedRun("orphan", runstate.Running, 5, prep)
	for _, c := range []struct {
		seq uint64
		typ string
	}{{1, "PREPARE"}, {2, "START"}} {
		_ = h.st.AppendEvent(ctx, "orphan", 5, store.Event{Kind: store.EventCommand, CmdSeq: c.seq, Detail: mustJSON(map[string]string{"type": c.typ})})
	}
	dead := &deadController{h: h, runID: "orphan", fence: 5}
	dead.send(transport.Command{Type: transport.CmdPrepare, CmdSeq: 1, Prepare: &prep}, transport.StatusReady)
	dead.send(transport.Command{Type: transport.CmdStart, CmdSeq: 2, Start: &transport.Start{StartAt: time.Now().Add(200 * time.Millisecond)}}, transport.StatusArmed)
	stopBeats := dead.heartbeat()
	time.Sleep(500 * time.Millisecond)
	stopBeats() // crash

	// A PENDING orphan: created, never prepared.
	h.st.seedRun("pending", runstate.Pending, 6, prep)
	// A run whose controller is alive and holds its lease.
	h.st.seedRun("held", runstate.Running, 7, prep)
	_, _ = h.leases.Acquire(ctx, store.RunLeaseKey("held"), 7)

	h.c = h.startController(h.nc, h.st, h.leases)

	run := h.waitTerminal("orphan", 15*time.Second)
	if run.State != runstate.Aborted || run.Verdict != health.Invalid || run.FenceToken <= 7 {
		t.Fatalf("orphan: state %s verdict %s fence %d", run.State, run.Verdict, run.FenceToken)
	}
	seqs, types := commands(h.events("orphan"))
	if !slices.Equal(seqs, []uint64{1, 2, 3}) || types[2] != "STOP" {
		t.Errorf("orphan commands %v %v: recovery STOP must continue at seq 3", seqs, types)
	}
	for w, row := range h.workerRows("orphan") {
		if row.FinalSeq == nil {
			t.Errorf("orphan %s: STOP ack with final_seq not recorded (STOP ignored as a repeat?)", w)
		}
	}
	if !run.FinalizeRequested {
		t.Error("orphan: finalize not requested")
	}

	p := h.waitTerminal("pending", 5*time.Second)
	if p.State != runstate.Aborted {
		t.Errorf("pending orphan: %s", p.State)
	}
	if seqs, _ := commands(h.events("pending")); len(seqs) != 0 {
		t.Errorf("pending orphan was sent commands %v", seqs)
	}

	time.Sleep(2 * h.cfg.OrphanSweep)
	if s := h.run("held").State; s != runstate.Running {
		t.Errorf("a run with a live lease was touched: %s", s)
	}
}

// deadController plays a controller that sends a few commands and dies.
type deadController struct {
	h     *harness
	runID string
	fence uint64
}

func (d *deadController) send(c transport.Command, want transport.AckStatus) {
	d.h.t.Helper()
	c.RunID, c.FenceToken, c.Deadline = d.runID, d.fence, time.Now().Add(5*time.Second)
	members := c.Prepare
	if members == nil {
		members = &transport.Prepare{Workers: d.h.workerIDs()}
	}
	res, err := fanout(context.Background(), d.h.nc, d.h.cfg, c, members.Workers, time.Now().Add(5*time.Second), nil)
	if err != nil || len(res.missing) > 0 {
		d.h.t.Fatalf("%s: %v, missing %v", c.Type, err, res.missing)
	}
	for w, a := range res.acks {
		if a.Status != want {
			d.h.t.Fatalf("%s from %s: %+v", c.Type, w, a)
		}
	}
}

func (d *deadController) heartbeat() (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(d.h.cfg.HeartbeatInterval)
		defer t.Stop()
		b := mustJSON(transport.Heartbeat{RunID: d.runID, FenceToken: d.fence})
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = d.h.nc.Publish(transport.RunHeartbeatSubject(d.runID), b)
			}
		}
	}()
	stop = func() { once.Do(func() { close(done) }) }
	d.h.t.Cleanup(stop)
	return stop
}

// pausableStore blocks every guarded write while paused, ignoring the
// context, like a controller process that is stopped (SIGSTOP).
type pausableStore struct {
	Store
	mu     sync.Mutex
	paused chan struct{} // closed when resumed; nil when running
}

func (p *pausableStore) pause() {
	p.mu.Lock()
	p.paused = make(chan struct{})
	p.mu.Unlock()
}

func (p *pausableStore) resume() {
	p.mu.Lock()
	if p.paused != nil {
		close(p.paused)
		p.paused = nil
	}
	p.mu.Unlock()
}

func (p *pausableStore) wait() {
	p.mu.Lock()
	ch := p.paused
	p.mu.Unlock()
	if ch != nil {
		<-ch
	}
}

func (p *pausableStore) Transition(ctx context.Context, t store.Transition) error {
	p.wait()
	return p.Store.Transition(ctx, t)
}

func (p *pausableStore) AppendEvent(ctx context.Context, id string, tok uint64, e store.Event) error {
	p.wait()
	return p.Store.AppendEvent(ctx, id, tok, e)
}

func (p *pausableStore) RecordStopAck(ctx context.Context, id string, tok uint64, a transport.Ack) error {
	p.wait()
	return p.Store.RecordStopAck(ctx, id, tok, a)
}

func (p *pausableStore) MarkWorkerLost(ctx context.Context, id string, tok uint64, w string, d json.RawMessage) error {
	p.wait()
	return p.Store.MarkWorkerLost(ctx, id, tok, w, d)
}

func (p *pausableStore) RequestFinalize(ctx context.Context, id string, tok uint64) error {
	p.wait()
	return p.Store.RequestFinalize(ctx, id, tok)
}

// pausableLeaser blocks renewals while its store is paused.
type pausableLeaser struct {
	Leaser
	p *pausableStore
}

func (l *pausableLeaser) Renew(ctx context.Context, key string, token uint64) (bool, error) {
	l.p.wait()
	return l.Leaser.Renew(ctx, key, token)
}

// Controller A pauses mid-run. Its gate closes, its workers stop, its lease
// expires and B recovers the run under a higher fence. When A resumes, its
// pending write is rejected as stale and it sends nothing more.
func TestPausedControllerIsFencedOut(t *testing.T) {
	h := newHarness(t, harnessOpts{noStart: true})
	ps := &pausableStore{Store: h.st}
	a := h.startController(h.nc, ps, &pausableLeaser{Leaser: h.leases, p: ps})
	h.c = a
	h.waitSeen(h.workerIDs()...)
	id := h.create(func(r *RunRequest) { r.DurationMS = 30_000 })
	tokA := h.run(id).FenceToken
	rec := h.recordCommands(id)
	h.waitState(id, runstate.Running, 10*time.Second)
	time.Sleep(200 * time.Millisecond)
	ps.pause()
	pausedAt := time.Now()

	// Workers trip their dead-man's switch before A's lease expires.
	for _, w := range h.workerIDs() {
		f := h.waitFinal(id, w, 5*time.Second)
		if f.Reason != transport.ReasonDeadman {
			t.Errorf("%s final reason %s, want deadman", w, f.Reason)
		}
	}
	if el := time.Since(pausedAt); el >= h.cfg.LeaseTTL {
		t.Errorf("workers stopped %v after the pause, not before the %v TTL", el, h.cfg.LeaseTTL)
	}

	h.startController(connect(t, h.url), h.st, h.leases) // controller B
	run := h.waitTerminal(id, 20*time.Second)
	if run.State != runstate.Aborted || run.FenceToken <= tokA {
		t.Fatalf("state %s fence %d (A had %d)", run.State, run.FenceToken, tokA)
	}
	resumedAt := time.Now()
	ps.resume()
	h.eventually(5*time.Second, "A to drop the run", func() bool { return !a.driving(id) })

	for _, w := range h.st.writesSince(id, resumedAt) {
		if w.token == tokA && w.ok {
			t.Errorf("A's write %q accepted after B took over", w.op)
		}
	}
	for _, c := range rec.all() {
		if c.FenceToken == tokA && c.at.After(resumedAt) {
			t.Errorf("A sent %s seq %d after resuming", c.Type, c.CmdSeq)
		}
	}
}

func TestFinalizeWaitsThenForces(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.agg.setMode("force") // the aggregator only finalizes when asked
	id := h.create()
	h.waitState(id, runstate.Stopping, 10*time.Second)
	stoppingAt := time.Now()
	run := h.waitTerminal(id, 15*time.Second)
	if run.State != runstate.Completed {
		t.Fatalf("state %s reason %q", run.State, lastReason(h.events(id)))
	}
	var forcedAt time.Time
	for _, e := range h.events(id) {
		if e.Kind == store.EventFinalizeRequested {
			forcedAt = e.At
		}
	}
	if forcedAt.IsZero() || forcedAt.Sub(stoppingAt) < h.cfg.FinalizeWait-100*time.Millisecond {
		t.Errorf("finalize requested %v after STOPPING, want after the %v wait", forcedAt.Sub(stoppingAt), h.cfg.FinalizeWait)
	}
}

func TestNeverCompletesWithoutAResult(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.agg.setMode("off") // aggregator down
	id := h.create()
	h.waitState(id, runstate.Stopping, 10*time.Second)
	time.Sleep(2*h.cfg.FinalizeWait + 500*time.Millisecond)
	run := h.run(id)
	if run.State != runstate.Stopping || !run.FinalizeRequested {
		t.Fatalf("state %s finalize_requested %v; want to keep waiting in STOPPING", run.State, run.FinalizeRequested)
	}
	h.agg.setMode("auto") // aggregator back
	if run := h.waitTerminal(id, 10*time.Second); run.State != runstate.Completed {
		t.Fatalf("state %s", run.State)
	}
}

func TestDeadmanEndedStreamAborts(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	id := h.create()
	// As if w2 heard no heartbeat (a one-way partition) and stopped early.
	h.st.mu.Lock()
	h.st.reasonOverride[id] = map[string]transport.StopReason{"w2": transport.ReasonDeadman}
	h.st.mu.Unlock()
	run := h.waitTerminal(id, 15*time.Second)
	if run.State != runstate.Aborted || !contains(lastReason(h.events(id)), "w2 stopped early") {
		t.Fatalf("state %s reason %q", run.State, lastReason(h.events(id)))
	}
}

func TestStopAckDeadlineMissedAborts(t *testing.T) {
	block := make(chan struct{})
	h := newHarness(t, harnessOpts{workerFn: func(id string, c *worker.Config) {
		if id == "w2" {
			c.FinalTimeout = 30 * time.Second // waits for the final snapshot far past the STOP deadline
		}
	}})
	h.pubs["w2"].mu.Lock()
	h.pubs["w2"].block = block
	h.pubs["w2"].mu.Unlock()
	t.Cleanup(func() { close(block) })
	id := h.create()
	run := h.waitTerminal(id, 20*time.Second)
	if run.State != runstate.Aborted || !contains(lastReason(h.events(id)), "no STOP ack from [w2]") {
		t.Fatalf("state %s reason %q", run.State, lastReason(h.events(id)))
	}
	if !run.FinalizeRequested {
		t.Error("finalize not requested")
	}
	// The STOP deadline passed, so the abort did not wait through another.
	if n := len(slices.DeleteFunc(func() []string { _, ts := commands(h.events(id)); return ts }(), func(s string) bool { return s != "STOP" })); n != 1 {
		t.Errorf("%d STOP commands, want 1", n)
	}
}

// TestControlledRunCoversStall is the coordinated-omission check for runs
// the controller drives: the target stalls for 1s mid-run, and the merged
// latency the run reports must cover the stall instead of hiding it.
func TestControlledRunCoversStall(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	const stall = time.Second
	h := newHarness(t, harnessOpts{}) // takes the timing lock first
	id := h.create(func(r *RunRequest) {
		r.RatePerWorker, r.DurationMS, r.RequestTimeoutMS, r.DrainTimeoutMS = 100, 3000, 2000, 2500
	})
	h.waitState(id, runstate.Running, 10*time.Second)
	time.Sleep(h.cfg.StartLead + time.Second) // start_at, then one second of load
	h.stub.Stall(stall)

	run := h.waitTerminal(id, 20*time.Second)
	if run.State != runstate.Completed {
		t.Fatalf("state %s reason %q", run.State, lastReason(h.events(id)))
	}
	res, err := h.st.GetResult(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	// A request intended at the start of the stall waits all of it.
	if res.Max < (stall - 20*time.Millisecond).Microseconds() {
		t.Errorf("max latency %dµs hides the %v stall", res.Max, stall)
	}
	// About a third of the requests were intended during the stall, with
	// latencies spread across it, so p99 sits near its end.
	if res.P99 < (900 * time.Millisecond).Microseconds() {
		t.Errorf("p99 %dµs does not reflect the stall", res.P99)
	}
	if want := int64(2 * 100 * 3); res.Intended < want-2 || res.Intended > want+2 {
		t.Errorf("intended %d, want %d: requests were skipped", res.Intended, want)
	}
	if res.ReceivedSnapshots != res.ExpectedSnapshots {
		t.Errorf("snapshots %d of %d", res.ReceivedSnapshots, res.ExpectedSnapshots)
	}
}
