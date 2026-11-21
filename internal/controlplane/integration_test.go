//go:build integration

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/loadforge/loadforge/internal/aggregator"
	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/kafkatest"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/stubserver"
	"github.com/loadforge/loadforge/internal/timingtest"
	"github.com/loadforge/loadforge/internal/transport"
	"github.com/loadforge/loadforge/internal/worker"
)

// These tests run the whole system on the compose stack (CLAUDE.md):
// controllers, real workers, the aggregator, Postgres, Redis, NATS and
// Kafka, with production timings. Each test uses its own Postgres schema.

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func baseDSN() string {
	return env("LF_PG_DSN", "postgres://loadforge:loadforge@localhost:5432/loadforge?sslmode=disable")
}

func brokers() []string { return strings.Split(env("LF_KAFKA", "localhost:9092"), ",") }

type itEnv struct {
	t       *testing.T
	prefix  string
	dsn     string // with search_path set to the test's schema
	st      *store.Store
	rdb     *redis.Client
	natsURL string
	stub    *stubserver.Server
	target  string
	pubs    map[string]*teePub
	wconns  map[string]*nats.Conn
}

func newEnv(t *testing.T) *itEnv {
	t.Helper()
	ctx := context.Background()
	prefix := fmt.Sprintf("cp%d", time.Now().UnixNano())
	admin, err := pgxpool.New(ctx, baseDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("Postgres (is the stack up?): %v", err)
	}
	schema := "lf_" + prefix
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })
	e := &itEnv{t: t, prefix: prefix, dsn: baseDSN() + "&search_path=" + schema,
		natsURL: env("LF_NATS_URL", nats.DefaultURL), pubs: map[string]*teePub{}, wconns: map[string]*nats.Conn{}}
	e.st, err = store.Open(ctx, e.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.st.Close)
	if err := e.st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	e.rdb = redis.NewClient(&redis.Options{Addr: env("LF_REDIS", "localhost:6379")})
	t.Cleanup(func() { _ = e.rdb.Close() })
	if err := e.rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis (is the stack up?): %v", err)
	}

	// The aggregator: one consumer and one merger on this schema.
	group := "lf-it-" + prefix
	kafkatest.StartGroupAtEnd(t, brokers(), group, transport.SnapshotTopic)
	cl, err := aggregator.NewKafkaClient(brokers(), kgo.ConsumerGroup(group), kgo.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	background(t, func(ctx context.Context) error { return aggregator.Consume(ctx, cl, aggregator.NewIngester(e.st)) })
	agg := aggregator.New(e.st, aggregator.NewRedisProgress(e.rdb), aggregator.Config{Period: 200 * time.Millisecond, MaxDirty: 10_000})
	background(t, agg.Run)

	e.stub = stubserver.New(time.Millisecond)
	srv := httptest.NewServer(e.stub)
	t.Cleanup(srv.Close)
	e.target = srv.URL
	return e
}

func background(t *testing.T, fn func(ctx context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("background: %v", err)
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

func (e *itEnv) connect() *nats.Conn {
	e.t.Helper()
	nc, err := nats.Connect(e.natsURL)
	if err != nil {
		e.t.Fatalf("NATS (is the stack up?): %v", err)
	}
	e.t.Cleanup(nc.Close)
	return nc
}

// teePub publishes to Kafka and records what was delivered, and when.
type teePub struct {
	*worker.KafkaPublisher
	mu    sync.Mutex
	snaps []*transport.Snapshot
	at    []time.Time
}

func (p *teePub) Publish(ctx context.Context, key, value []byte) error {
	if err := p.KafkaPublisher.Publish(ctx, key, value); err != nil {
		return err
	}
	if s, err := transport.DecodeSnapshot(value); err == nil {
		p.mu.Lock()
		p.snaps, p.at = append(p.snaps, s), append(p.at, time.Now())
		p.mu.Unlock()
	}
	return nil
}

func (p *teePub) final(runID string) (*transport.Snapshot, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, s := range p.snaps {
		if s.RunID == runID && s.Final {
			return s, p.at[i]
		}
	}
	return nil, time.Time{}
}

// addWorkers starts n workers with production timings.
func (e *itEnv) addWorkers(n int) []string {
	e.t.Helper()
	var ids []string
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("%s-w%d", e.prefix, i)
		kp, err := worker.NewKafkaPublisher(brokers())
		if err != nil {
			e.t.Fatal(err)
		}
		e.t.Cleanup(kp.Close)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = kp.Ping(ctx)
		cancel()
		if err != nil {
			e.t.Fatalf("Kafka (is the stack up?): %v", err)
		}
		pub := &teePub{KafkaPublisher: kp}
		nc := e.connect()
		w, err := worker.New(worker.Config{ID: id, RateCap: 10_000}, nc, pub)
		if err != nil {
			e.t.Fatal(err)
		}
		e.t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			_ = w.Close(ctx)
		})
		e.pubs[id], e.wconns[id] = pub, nc
		ids = append(ids, id)
	}
	return ids
}

func itConfig() Config {
	cfg := DefaultConfig()
	cfg.InstanceID = "it"
	cfg.Limits = Limits{MaxRatePerWorker: 5000, MaxTotalRate: 50_000, MaxDuration: 10 * time.Minute, MaxWorkers: 20}
	return cfg
}

// controller starts an in-process controller with production timings.
func (e *itEnv) controller() *Controller {
	e.t.Helper()
	c, err := New(itConfig(), e.st, store.NewLeases(e.rdb), e.connect(), NewRedisLive(e.rdb))
	if err != nil {
		e.t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})
	return c
}

func (e *itEnv) request(workers []string, mut ...func(*RunRequest)) RunRequest {
	// 50 req/s per worker, as in the worker's own stall test: every worker
	// shares this one -race test process with the aggregator, and the
	// verdict must judge the generator, not an overloaded test binary.
	r := RunRequest{ID: e.prefix + "-run", Workers: workers, Method: "GET", TargetURL: e.target, RatePerWorker: 50,
		DurationMS: 10_000, MaxInFlight: 500, QueueDepth: 1000, RequestTimeoutMS: 3000, DrainTimeoutMS: 4000}
	for _, m := range mut {
		m(&r)
	}
	return r
}

func (e *itEnv) waitSeen(c *Controller, ids []string) {
	e.t.Helper()
	until(e.t, 10*time.Second, "workers seen by the controller", func() bool {
		for _, id := range ids {
			if at, _, ok := c.reg.Last(id); !ok || time.Since(at) > 2*time.Second {
				return false
			}
		}
		return true
	})
}

func until(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *itEnv) state(id string) runstate.State {
	r, err := e.st.GetRun(context.Background(), id)
	if err != nil {
		return ""
	}
	return r.State
}

func (e *itEnv) waitResult(id string, timeout time.Duration) store.ResultRow {
	e.t.Helper()
	var res store.ResultRow
	until(e.t, timeout, "run_results for "+id, func() bool {
		r, err := e.st.GetResult(context.Background(), id)
		res = r
		return err == nil
	})
	return res
}

func TestIntegrationStoreContract(t *testing.T) {
	storeContract(t, newEnv(t).st)
}

// TestIntegrationFullRunWithStall drives a 3-worker run through the HTTP
// API while the target stalls for 2s, and checks the run ends COMPLETED
// with an honest verdict and latencies that cover the stall (CLAUDE.md
// workflow rule 2).
func TestIntegrationFullRunWithStall(t *testing.T) {
	timingtest.Exclusive(t)
	e := newEnv(t)
	ids := e.addWorkers(3)
	c := e.controller()
	e.waitSeen(c, ids)
	api := httptest.NewServer(c.Handler())
	t.Cleanup(api.Close)

	var created struct{ ID string }
	if code := doJSON(t, "POST", api.URL+"/runs", e.request(ids), &created); code != http.StatusAccepted {
		t.Fatalf("POST /runs: %d", code)
	}
	id := created.ID
	until(t, 20*time.Second, "RUNNING", func() bool { return e.state(id) == runstate.Running })
	time.Sleep(itConfig().StartLead + 4*time.Second) // start_at, then 4s of load
	const stall = 2 * time.Second
	e.stub.Stall(stall)

	var sawLive bool
	var v RunView
	until(t, 60*time.Second, "a verdict", func() bool {
		v = RunView{}
		doJSON(t, "GET", api.URL+"/runs/"+id, nil, &v)
		if v.Verdict == "" && (v.Results != nil || v.Diagnostics != nil) {
			t.Fatalf("results shown without a verdict in state %s", v.State)
		}
		if !sawLive && v.State == string(runstate.Running) {
			var lv LiveView
			doJSON(t, "GET", api.URL+"/runs/"+id+"/live", nil, &lv)
			if lv.Progress != nil {
				sawLive = true
				if !lv.Provisional || lv.Verdict != "PENDING" || lv.Progress["verdict"] != "PENDING" {
					t.Errorf("live view %+v", lv)
				}
			}
		}
		return v.Verdict != ""
	})
	if !sawLive {
		t.Error("never saw live progress while RUNNING")
	}
	if v.State != string(runstate.Completed) {
		t.Fatalf("state %s, events %+v", v.State, v.Events)
	}

	in, th, err := e.st.VerdictInputs(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	res, _ := health.Evaluate(in, th)
	if in.LagP99US != nil {
		t.Logf("dispatch lag p99 = %dµs", *in.LagP99US)
	}
	if v.Verdict != string(health.Degraded) || len(res.Invalid) != 0 || len(res.Unmeasured) != 3 {
		t.Errorf("verdict %s, invalid %v, unmeasured %v", v.Verdict, res.Invalid, res.Unmeasured)
	}
	r := v.Results
	if r == nil {
		t.Fatal("no results on a DEGRADED run")
	}
	if r.ExpectedSnapshots == 0 || r.ReceivedSnapshots != r.ExpectedSnapshots || r.RejectedSnapshots != 0 {
		t.Errorf("snapshots %d/%d, %d rejected", r.ReceivedSnapshots, r.ExpectedSnapshots, r.RejectedSnapshots)
	}
	if want := int64(3 * 50 * 10); r.Intended < want-3 || r.Intended > want+3 {
		t.Errorf("intended %d, want %d", r.Intended, want)
	}
	if r.MaxUS < (stall - 20*time.Millisecond).Microseconds() {
		t.Errorf("max %dµs hides the %v stall", r.MaxUS, stall)
	}
	// 20% of requests were intended during the stall, spread across it.
	if r.P99US < (1800 * time.Millisecond).Microseconds() {
		t.Errorf("p99 %dµs does not reflect the stall", r.P99US)
	}
	fence := v.Events[0].FenceToken
	for _, ev := range v.Events {
		if ev.FenceToken != fence {
			t.Errorf("event %s carries fence %d, want %d", ev.Kind, ev.FenceToken, fence)
		}
	}
	for _, w := range v.Workers {
		if w.FinalSeq == nil || w.Lost || w.PrepareStatus != string(transport.StatusReady) {
			t.Errorf("worker %+v", w)
		}
	}
	if _, held, _ := store.NewLeases(e.rdb).Holder(context.Background(), store.RunLeaseKey(id)); held {
		t.Error("lease still held")
	}
}

func TestIntegrationWorkerLost(t *testing.T) {
	timingtest.Exclusive(t) // runs real load: never alongside a lag-sensitive test
	e := newEnv(t)
	ids := e.addWorkers(2)
	c := e.controller()
	e.waitSeen(c, ids)
	run, err := c.CreateRun(context.Background(), e.request(ids, func(r *RunRequest) { r.DurationMS = 60_000 }))
	if err != nil {
		t.Fatal(err)
	}
	until(t, 20*time.Second, "RUNNING", func() bool { return e.state(run.ID) == runstate.Running })
	time.Sleep(3 * time.Second)
	e.wconns[ids[1]].Close()

	until(t, 60*time.Second, "ABORTED", func() bool { return e.state(run.ID) == runstate.Aborted })
	ws, _ := e.st.RunWorkers(context.Background(), run.ID)
	if !ws[1].Lost || ws[0].Lost || ws[0].FinalSeq == nil {
		t.Errorf("workers %+v", ws)
	}
	e.waitResult(run.ID, 30*time.Second)
	v, err := c.View(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v.Verdict != string(health.Invalid) || v.Results != nil || v.Diagnostics == nil {
		t.Errorf("view verdict %s results %v diagnostics %v", v.Verdict, v.Results, v.Diagnostics)
	}
	if !slices.ContainsFunc(v.Events, func(ev EventView) bool { return ev.Kind == store.EventWorkerLost }) {
		t.Error("no worker_lost event")
	}
}
