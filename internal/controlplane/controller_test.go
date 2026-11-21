package controlplane

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/safety"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
	"github.com/loadforge/loadforge/internal/worker"
)

func TestDefaultConfigHoldsTheTimingInvariants(t *testing.T) {
	c := DefaultConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if g := c.HeartbeatGate(); g != 4*time.Second {
		t.Errorf("heartbeat gate = %v, want 4s (10s TTL − 5s dead-man − 1s margin)", g)
	}
	// The last heartbeat goes out before the gate closes and workers trip
	// at most DeadManTimeout later: that must be before the lease expires.
	if c.HeartbeatGate()+safety.DeadManTimeout >= safety.LeaseTTL {
		t.Error("workers could still be running when the lease expires")
	}
	// One failed renewal must not trip the workers: the next confirmation
	// comes 2×RenewInterval after the last, and heartbeats resume then.
	if gap := 2*c.RenewInterval - c.HeartbeatGate(); gap >= c.DeadManTimeout {
		t.Errorf("one missed renewal leaves a %v heartbeat gap, at or above the %v dead-man timeout", gap, c.DeadManTimeout)
	}
	if err := testConfig().Validate(); err != nil {
		t.Fatalf("test config: %v", err)
	}
}

func TestConfigRejectsBrokenInvariants(t *testing.T) {
	for name, mut := range map[string]func(*Config){
		"dead-man not below TTL":     func(c *Config) { c.DeadManTimeout = c.LeaseTTL },
		"gate not above renew":       func(c *Config) { c.RenewInterval = 4 * time.Second },
		"no gate margin":             func(c *Config) { c.GateMargin = 0 },
		"heartbeat too slow":         func(c *Config) { c.HeartbeatInterval = 3 * time.Second },
		"ack margin beyond lead":     func(c *Config) { c.StartAckMargin = c.StartLead },
		"sweep slower than TTL":      func(c *Config) { c.OrphanSweep = 2 * c.LeaseTTL },
		"retry backoff below retry":  func(c *Config) { c.MaxRetryInterval = c.RetryInterval / 2 },
		"zero prepare timeout":       func(c *Config) { c.PrepareTimeout = 0 },
		"negative finalize interval": func(c *Config) { c.FinalizePoll = -time.Second },
	} {
		c := DefaultConfig()
		mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestHappyPathCompletesWithVerdict(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	id := h.create()
	created := h.run(id)
	run := h.waitTerminal(id, 15*time.Second)

	if run.State != runstate.Completed {
		t.Fatalf("state %s, reason %q", run.State, lastReason(h.events(id)))
	}
	want := []runstate.State{runstate.Pending, runstate.Preparing, runstate.Ready, runstate.Running, runstate.Stopping, runstate.Completed}
	if got := states(h.events(id)); !slices.Equal(got, want) {
		t.Errorf("states %v, want %v", got, want)
	}
	seqs, types := commands(h.events(id))
	if !slices.Equal(seqs, []uint64{1, 2, 3}) || !slices.Equal(types, []string{"PREPARE", "START", "STOP"}) {
		t.Errorf("commands %v %v", seqs, types)
	}
	for _, e := range h.events(id) {
		if e.FenceToken != created.FenceToken {
			t.Errorf("event %s carries fence %d, want the lease's %d", e.Kind, e.FenceToken, created.FenceToken)
		}
	}
	for wid, w := range h.workerRows(id) {
		if w.PrepareStatus != transport.StatusReady || w.FinalSeq == nil || w.Lost {
			t.Errorf("worker %s: %+v", wid, w)
		}
		f := h.finalSnapshot(id, wid)
		if f == nil || f.Seq != *w.FinalSeq {
			t.Errorf("worker %s: final snapshot %+v, final_seq %v", wid, f, w.FinalSeq)
		}
	}

	// The verdict is the health engine's on the stored inputs, and it is
	// never VALID while CPU, clock offset and dispatch window are unreported.
	in, th, _ := h.st.VerdictInputs(context.Background(), id)
	res, err := health.Evaluate(in, th)
	if err != nil {
		t.Fatal(err)
	}
	if run.Verdict != res.Verdict || run.Verdict == health.Valid {
		t.Errorf("verdict %s, health engine says %s", run.Verdict, res.Verdict)
	}
	for _, s := range []string{health.SignalCPU, health.SignalClockSpread, health.SignalAchievedRate} {
		if !slices.Contains(res.Unmeasured, s) {
			t.Errorf("%s not listed as unmeasured: %v", s, res.Unmeasured)
		}
	}
	if res.Verdict == health.Invalid && !slices.Equal(res.Invalid, []string{health.SignalLagP99}) {
		t.Errorf("INVALID for something other than a loaded test machine's dispatch lag: %v", res.Invalid)
	}
	if _, held, _ := h.leases.Holder(context.Background(), store.RunLeaseKey(id)); held {
		t.Error("lease still held after COMPLETED")
	}
	h.eventually(time.Second, "driver to exit", func() bool { return !h.c.driving(id) })
}

func TestValidateRejectsWithoutCreatingAnything(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	cases := []struct {
		name   string
		mut    func(*RunRequest)
		status int
	}{
		{"no workers", func(r *RunRequest) { r.Workers = nil }, http.StatusBadRequest},
		{"bad target", func(r *RunRequest) { r.TargetURL = "ftp://x" }, http.StatusBadRequest},
		{"bad id", func(r *RunRequest) { r.ID = "a.b" }, http.StatusBadRequest},
		{"drain below request timeout", func(r *RunRequest) { r.DrainTimeoutMS = 100 }, http.StatusBadRequest},
		{"unknown threshold", func(r *RunRequest) { r.Thresholds = []byte(`{"nope":1}`) }, http.StatusBadRequest},
		{"rate over cap", func(r *RunRequest) { r.RatePerWorker = 20_000 }, http.StatusUnprocessableEntity},
		{"total over cap", func(r *RunRequest) {
			r.RatePerWorker = 9_000
			r.Workers = []string{"w1", "w2", "x", "y", "z", "q", "r", "s", "t", "u", "v", "a"}
		}, http.StatusUnprocessableEntity},
		{"too long", func(r *RunRequest) { r.DurationMS = int64(2 * time.Minute / time.Millisecond) }, http.StatusUnprocessableEntity},
		{"never seen", func(r *RunRequest) { r.Workers = []string{"w1", "nobody"} }, http.StatusConflict},
	}
	for _, tc := range cases {
		_, err := h.c.CreateRun(context.Background(), h.request(tc.mut))
		var re *RequestError
		if !errors.As(err, &re) || re.Status != tc.status {
			t.Errorf("%s: err %v, want status %d", tc.name, err, tc.status)
		}
	}
	// A worker whose heartbeat names a run is busy.
	h.c.reg.mu.Lock()
	h.c.reg.seen["w2"] = sighting{at: time.Now(), runID: "other"}
	h.c.reg.mu.Unlock()
	if _, err := h.c.CreateRun(context.Background(), h.request()); err == nil || !contains(err.Error(), "busy") {
		t.Errorf("busy worker: %v", err)
	}
	// A worker heard from too long ago is refused.
	h.c.reg.mu.Lock()
	h.c.reg.seen["stale"] = sighting{at: time.Now().Add(-time.Hour)}
	h.c.reg.mu.Unlock()
	if _, err := h.c.CreateRun(context.Background(), h.request(func(r *RunRequest) { r.Workers = []string{"w1", "stale"} })); err == nil || !contains(err.Error(), "ago") {
		t.Errorf("stale worker: %v", err)
	}
	if runs, _ := h.st.ActiveRuns(context.Background()); len(runs) != 0 {
		t.Errorf("refused requests left runs: %v", runs)
	}
	h.leases.mu.Lock()
	n := len(h.leases.leases)
	h.leases.mu.Unlock()
	if n != 0 {
		t.Errorf("refused requests left %d leases", n)
	}
}

func TestPrepareNackAbortsAndStopsTheReadyWorker(t *testing.T) {
	h := newHarness(t, harnessOpts{workerFn: func(id string, c *worker.Config) {
		if id == "w2" {
			c.RateCap = 10 // refuses the 20 req/s run
		}
	}})
	id := h.create()
	run := h.waitTerminal(id, 15*time.Second)
	if run.State != runstate.Aborted || run.Verdict != health.Invalid {
		t.Fatalf("state %s verdict %s", run.State, run.Verdict)
	}
	if r := lastReason(h.events(id)); !contains(r, "w2 refused PREPARE") {
		t.Errorf("reason %q", r)
	}
	if slices.Contains(states(h.events(id)), runstate.Ready) {
		t.Error("reached READY with a refusing worker")
	}
	// w1 prepared, so the abort's STOP made it publish its one empty final.
	f := h.waitFinal(id, "w1", 5*time.Second)
	if f.Reason != transport.ReasonStop || f.Intended != 0 {
		t.Errorf("w1 final %+v", f)
	}
	if !h.run(id).FinalizeRequested {
		t.Error("finalize not requested for an aborted run")
	}
}

func TestPrepareDeadlineAborts(t *testing.T) {
	h := newHarness(t, harnessOpts{workers: 1})
	h.addGhost("silent", func(*ghost, transport.Command, int) *transport.Ack { return nil })
	h.waitSeen("silent")
	start := time.Now()
	id := h.create(func(r *RunRequest) { r.Workers = []string{"w1", "silent"} })
	run := h.waitTerminal(id, 15*time.Second)
	if run.State != runstate.Aborted {
		t.Fatalf("state %s", run.State)
	}
	if r := lastReason(h.events(id)); !contains(r, "no PREPARE ack from [silent]") {
		t.Errorf("reason %q", r)
	}
	if el := time.Since(start); el < h.cfg.PrepareTimeout {
		t.Errorf("aborted after %v, before the %v PREPARE deadline", el, h.cfg.PrepareTimeout)
	}
}

func TestRetryReusesCmdSeqAndStartNackAborts(t *testing.T) {
	h := newHarness(t, harnessOpts{workers: 1})
	g := h.addGhost("g1", func(_ *ghost, c transport.Command, n int) *transport.Ack {
		switch c.Type {
		case transport.CmdPrepare:
			if n == 1 {
				return nil // drop the first delivery: the controller must re-send
			}
			return &transport.Ack{Status: transport.StatusReady}
		case transport.CmdStart:
			return &transport.Ack{Status: transport.StatusNack, Detail: "scripted refusal"}
		default:
			return &transport.Ack{Status: transport.StatusNack, Detail: "not prepared"}
		}
	})
	h.waitSeen("g1")
	id := h.create(func(r *RunRequest) { r.Workers = []string{"w1", "g1"} })
	run := h.waitTerminal(id, 15*time.Second)
	if run.State != runstate.Aborted || !contains(lastReason(h.events(id)), "g1 refused START") {
		t.Fatalf("state %s reason %q", run.State, lastReason(h.events(id)))
	}
	var prepares int
	for _, c := range g.commands() {
		if c.Type == transport.CmdPrepare {
			prepares++
			if c.CmdSeq != 1 {
				t.Errorf("PREPARE re-sent with cmd_seq %d, want 1", c.CmdSeq)
			}
		}
	}
	if prepares < 2 {
		t.Errorf("PREPARE delivered %d times, want a re-send", prepares)
	}
	if !slices.Contains(states(h.events(id)), runstate.Ready) {
		t.Error("never reached READY after the re-sent PREPARE was acked")
	}
}

func TestStopViaAPIWhileRunning(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	id := h.create(func(r *RunRequest) { r.DurationMS = 30_000 })
	h.waitState(id, runstate.Running, 10*time.Second)
	time.Sleep(300 * time.Millisecond)
	if err := h.c.RequestStop(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	run := h.waitTerminal(id, 15*time.Second)
	if run.State != runstate.Completed {
		t.Fatalf("state %s reason %q", run.State, lastReason(h.events(id)))
	}
	var sawStopRequested bool
	for _, e := range h.events(id) {
		sawStopRequested = sawStopRequested || e.Kind == store.EventStopRequested
	}
	if !sawStopRequested {
		t.Error("no stop_requested event")
	}
	for _, w := range h.workerIDs() {
		if f := h.finalSnapshot(id, w); f == nil || f.Reason != transport.ReasonStop {
			t.Errorf("%s final %+v", w, f)
		}
	}
	if err := h.c.RequestStop(context.Background(), id); !errors.Is(err, store.ErrStateConflict) {
		t.Errorf("stop of a terminal run: %v", err)
	}
	if err := h.c.RequestStop(context.Background(), "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("stop of an unknown run: %v", err)
	}
}

func TestStopRequestedThroughAnotherInstance(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	id := h.create(func(r *RunRequest) { r.DurationMS = 30_000 })
	h.waitState(id, runstate.Running, 10*time.Second)
	// Only Postgres learns of it, as when the API call lands elsewhere.
	if err := h.st.RequestStop(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if run := h.waitTerminal(id, 15*time.Second); run.State != runstate.Completed {
		t.Fatalf("state %s reason %q", run.State, lastReason(h.events(id)))
	}
}

func TestStopBeforeStartAborts(t *testing.T) {
	h := newHarness(t, harnessOpts{workers: 1})
	h.addGhost("slow", func(_ *ghost, c transport.Command, _ int) *transport.Ack {
		if c.Type == transport.CmdPrepare {
			time.Sleep(time.Second)
			return &transport.Ack{Status: transport.StatusReady}
		}
		return &transport.Ack{Status: transport.StatusNack, Detail: "not prepared"}
	})
	h.waitSeen("slow")
	id := h.create(func(r *RunRequest) { r.Workers = []string{"w1", "slow"} })
	h.waitState(id, runstate.Preparing, 5*time.Second)
	if err := h.c.RequestStop(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	run := h.waitTerminal(id, 15*time.Second)
	if run.State != runstate.Aborted || !contains(lastReason(h.events(id)), "stop requested in PREPARING") {
		t.Fatalf("state %s reason %q", run.State, lastReason(h.events(id)))
	}
	if f := h.waitFinal(id, "w1", 5*time.Second); f.Reason != transport.ReasonStop {
		t.Errorf("w1 final %+v", f)
	}
}
