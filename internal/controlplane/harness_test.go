package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/stubserver"
	"github.com/loadforge/loadforge/internal/timingtest"
	"github.com/loadforge/loadforge/internal/transport"
	"github.com/loadforge/loadforge/internal/worker"
)

// testConfig scales every controller timing down about tenfold while
// keeping the invariants: gate = 2s − 1s − 200ms = 800ms, above the 500ms
// renew interval, and gate + dead-man (1.8s) below the 2s TTL.
func testConfig() Config {
	return Config{
		InstanceID:           "test",
		LeaseTTL:             2 * time.Second,
		RenewInterval:        500 * time.Millisecond,
		HeartbeatInterval:    100 * time.Millisecond,
		DeadManTimeout:       time.Second,
		GateMargin:           200 * time.Millisecond,
		WorkerSilence:        time.Second,
		LivenessCheck:        50 * time.Millisecond,
		PrepareTimeout:       3 * time.Second,
		StartLead:            500 * time.Millisecond,
		StartAckMargin:       150 * time.Millisecond,
		FinalSnapshotTimeout: 2 * time.Second,
		StopMargin:           500 * time.Millisecond,
		RetryInterval:        100 * time.Millisecond,
		MaxRetryInterval:     400 * time.Millisecond,
		FinalizeWait:         time.Second,
		FinalizePoll:         20 * time.Millisecond,
		StopPoll:             100 * time.Millisecond,
		OrphanSweep:          300 * time.Millisecond,
		StoreTimeout:         2 * time.Second,
		Limits:               Limits{MaxRatePerWorker: 10_000, MaxTotalRate: 100_000, MaxDuration: time.Minute, MaxWorkers: 10},
	}
}

type harness struct {
	t      *testing.T
	url    string // NATS
	nc     *nats.Conn
	cfg    Config
	st     *fakeStore
	leases *fakeLeaser
	agg    *fakeAggregator
	stub   *stubserver.Server
	target string

	workers map[string]*worker.Worker
	wconns  map[string]*nats.Conn
	pubs    map[string]*memPub
	c       *Controller
}

type harnessOpts struct {
	workers  int
	noStart  bool
	cfg      func(*Config)
	workerFn func(id string, c *worker.Config)
}

func startNATS(t *testing.T) string {
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
	return ns.ClientURL()
}

func connect(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// newHarness takes the machine-wide timing lock first: these tests run
// real workers and assert on sub-second scaled timings (the heartbeat gate,
// worker silence, STOP deadlines), and their load must not overlap other
// packages' dispatch-lag tests either (internal/timingtest).
func newHarness(t *testing.T, o harnessOpts) *harness {
	t.Helper()
	timingtest.Exclusive(t)
	if o.workers == 0 {
		o.workers = 2
	}
	url := startNATS(t)
	stub := stubserver.New(time.Millisecond)
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)

	cfg := testConfig()
	if o.cfg != nil {
		o.cfg(&cfg)
	}
	h := &harness{t: t, url: url, nc: connect(t, url), cfg: cfg, st: newFakeStore(), leases: newFakeLeaser(cfg.LeaseTTL),
		stub: stub, target: srv.URL, workers: map[string]*worker.Worker{}, wconns: map[string]*nats.Conn{}, pubs: map[string]*memPub{}}
	h.agg = &fakeAggregator{st: h.st, pubs: h.pubs, mode: "auto"}
	stop := make(chan struct{})
	go h.agg.loop(stop)
	t.Cleanup(func() { close(stop) })

	for i := 1; i <= o.workers; i++ {
		h.addWorker(fmt.Sprintf("w%d", i), o.workerFn)
	}
	if !o.noStart {
		h.c = h.startController(h.nc, h.st, h.leases)
		h.waitSeen(h.workerIDs()...)
	}
	return h
}

func (h *harness) addWorker(id string, fn func(string, *worker.Config)) {
	h.t.Helper()
	wc := worker.Config{
		ID: id, RateCap: 10_000, DeadManTimeout: h.cfg.DeadManTimeout, HeartbeatInterval: 50 * time.Millisecond,
		SnapshotPeriod: 250 * time.Millisecond, PublishTimeout: time.Second, FinalTimeout: h.cfg.FinalSnapshotTimeout,
	}
	if fn != nil {
		fn(id, &wc)
	}
	nc := connect(h.t, h.url)
	pub := &memPub{}
	w, err := worker.New(wc, nc, pub)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.Close(ctx)
	})
	h.workers[id], h.wconns[id], h.pubs[id] = w, nc, pub
}

func (h *harness) workerIDs() []string {
	var ids []string
	for i := 1; i <= len(h.workers); i++ {
		ids = append(ids, fmt.Sprintf("w%d", i))
	}
	return ids
}

func (h *harness) startController(nc *nats.Conn, st Store, leases Leaser) *Controller {
	h.t.Helper()
	c, err := New(h.cfg, st, leases, nc, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})
	return c
}

// waitSeen waits until the controller has heard from every worker.
func (h *harness) waitSeen(ids ...string) {
	h.t.Helper()
	h.eventually(3*time.Second, "workers seen", func() bool {
		for _, id := range ids {
			if _, _, ok := h.c.reg.Last(id); !ok {
				return false
			}
		}
		return true
	})
}

// request is a light run: these tests check the protocol, and they run
// alongside other packages' timing tests, which a busy machine would fail.
func (h *harness) request(mut ...func(*RunRequest)) RunRequest {
	r := RunRequest{
		Workers: h.workerIDs(), Method: "GET", TargetURL: h.target, RatePerWorker: 20, DurationMS: 1000,
		MaxInFlight: 50, QueueDepth: 100, RequestTimeoutMS: 500, DrainTimeoutMS: 500,
	}
	for _, m := range mut {
		m(&r)
	}
	return r
}

func (h *harness) create(mut ...func(*RunRequest)) string {
	h.t.Helper()
	run, err := h.c.CreateRun(context.Background(), h.request(mut...))
	if err != nil {
		h.t.Fatalf("CreateRun: %v", err)
	}
	return run.ID
}

func (h *harness) eventually(timeout time.Duration, what string, ok func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *harness) run(id string) store.Run {
	h.t.Helper()
	r, err := h.st.GetRun(context.Background(), id)
	if err != nil {
		h.t.Fatal(err)
	}
	return r
}

func (h *harness) waitState(id string, want runstate.State, timeout time.Duration) store.Run {
	h.t.Helper()
	h.eventually(timeout, fmt.Sprintf("run %s to reach %s", id, want), func() bool { return h.run(id).State == want })
	return h.run(id)
}

func (h *harness) waitTerminal(id string, timeout time.Duration) store.Run {
	h.t.Helper()
	h.eventually(timeout, "run "+id+" to end", func() bool { return h.run(id).State.Terminal() })
	return h.run(id)
}

func (h *harness) events(id string) []store.StoredEvent {
	evs, _ := h.st.Events(context.Background(), id)
	return evs
}

func (h *harness) workerRows(id string) map[string]store.RunWorker {
	ws, _ := h.st.RunWorkers(context.Background(), id)
	out := map[string]store.RunWorker{}
	for _, w := range ws {
		out[w.WorkerID] = w
	}
	return out
}

// lastReason is the reason in the last transition event's detail.
func lastReason(evs []store.StoredEvent) string {
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Kind == store.EventTransition {
			var d struct {
				Reason string `json:"reason"`
			}
			_ = json.Unmarshal(evs[i].Detail, &d)
			return d.Reason
		}
	}
	return ""
}

func states(evs []store.StoredEvent) []runstate.State {
	var out []runstate.State
	for _, e := range evs {
		if e.ToState != "" {
			out = append(out, e.ToState)
		}
	}
	return out
}

func commands(evs []store.StoredEvent) (seqs []uint64, types []string) {
	for _, e := range evs {
		if e.Kind == store.EventCommand {
			var d struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal(e.Detail, &d)
			seqs, types = append(seqs, e.CmdSeq), append(types, d.Type)
		}
	}
	return
}

func (h *harness) finalSnapshot(runID, workerID string) *transport.Snapshot {
	for _, s := range h.pubs[workerID].run(runID) {
		if s.Final {
			return s
		}
	}
	return nil
}

func (h *harness) waitFinal(runID, workerID string, timeout time.Duration) *transport.Snapshot {
	h.t.Helper()
	var s *transport.Snapshot
	h.eventually(timeout, "final snapshot from "+workerID, func() bool { s = h.finalSnapshot(runID, workerID); return s != nil })
	return s
}

// hbRecorder records controller heartbeats for a run.
type hbRecorder struct {
	mu   sync.Mutex
	last map[uint64]time.Time // by fence token
}

func (h *harness) recordHeartbeats(runID string) *hbRecorder {
	h.t.Helper()
	r := &hbRecorder{last: map[uint64]time.Time{}}
	sub, err := h.nc.Subscribe(transport.RunHeartbeatSubject(runID), func(m *nats.Msg) {
		var hb transport.Heartbeat
		if json.Unmarshal(m.Data, &hb) == nil {
			r.mu.Lock()
			r.last[hb.FenceToken] = time.Now()
			r.mu.Unlock()
		}
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = sub.Unsubscribe() })
	_ = h.nc.Flush()
	return r
}

func (r *hbRecorder) lastAt(fence uint64) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last[fence]
}

// cmdRecorder records every command sent for a run.
type cmdRecorder struct {
	mu   sync.Mutex
	cmds []recordedCmd
}

type recordedCmd struct {
	transport.Command
	at time.Time
}

func (h *harness) recordCommands(runID string) *cmdRecorder {
	h.t.Helper()
	r := &cmdRecorder{}
	sub, err := h.nc.Subscribe(transport.CmdSubject(runID), func(m *nats.Msg) {
		var c transport.Command
		if json.Unmarshal(m.Data, &c) == nil {
			r.mu.Lock()
			r.cmds = append(r.cmds, recordedCmd{c, time.Now()})
			r.mu.Unlock()
		}
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = sub.Unsubscribe() })
	_ = h.nc.Flush()
	return r
}

func (r *cmdRecorder) all() []recordedCmd {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedCmd(nil), r.cmds...)
}

// ghost is a scripted worker: it sends heartbeats like a worker and
// answers commands with whatever reply returns (nil means silence).
type ghost struct {
	id   string
	mu   sync.Mutex
	seen []transport.Command
}

func (h *harness) addGhost(id string, reply func(g *ghost, c transport.Command, n int) *transport.Ack) *ghost {
	h.t.Helper()
	g := &ghost{id: id}
	nc := connect(h.t, h.url)
	sub, err := nc.Subscribe(transport.CmdSubjectAll, func(m *nats.Msg) {
		var c transport.Command
		if json.Unmarshal(m.Data, &c) != nil {
			return
		}
		if c.Type == transport.CmdPrepare && (c.Prepare == nil || !c.Prepare.Lists(id)) {
			return
		}
		g.mu.Lock()
		g.seen = append(g.seen, c)
		n := 0
		for _, s := range g.seen {
			if s.Type == c.Type && s.CmdSeq == c.CmdSeq {
				n++
			}
		}
		g.mu.Unlock()
		if a := reply(g, c, n); a != nil {
			a.WorkerID, a.RunID, a.CmdSeq = id, c.RunID, c.CmdSeq
			_ = m.Respond(mustJSON(a))
		}
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = sub.Unsubscribe() })
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		b := mustJSON(transport.WorkerHeartbeat{WorkerID: id})
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				_ = nc.Publish(transport.WorkerHeartbeatSubject(id), b)
			}
		}
	}()
	h.t.Cleanup(func() { close(stop) })
	_ = nc.Flush()
	return g
}

func (g *ghost) commands() []transport.Command {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]transport.Command(nil), g.seen...)
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
