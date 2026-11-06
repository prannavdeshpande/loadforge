//go:build integration

package aggregator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/kafkatest"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
	"github.com/loadforge/loadforge/internal/worker"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dsn() string {
	return env("LF_PG_DSN", "postgres://loadforge:loadforge@localhost:5432/loadforge?sslmode=disable")
}

func brokers() []string { return strings.Split(env("LF_KAFKA", "localhost:9092"), ",") }

// itStore is a migrated Store in a fresh schema.
func itStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Ping(ctx); err != nil {
		t.Fatalf("Postgres (is the stack up?): %v", err)
	}
	schema := fmt.Sprintf("lf_agg_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })
	st, err := store.Open(ctx, dsn()+"&search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return st
}

// startAtEnd positions group at the topic's current end (see kafkatest).
// Call it before publishing.
func startAtEnd(t *testing.T, group string) {
	t.Helper()
	kafkatest.StartGroupAtEnd(t, brokers(), group, transport.SnapshotTopic)
}

// itConsumer is a consumer in group, which startAtEnd has positioned.
func itConsumer(t *testing.T, group string) *kgo.Client {
	t.Helper()
	cl, err := NewKafkaClient(brokers(), kgo.ConsumerGroup(group), kgo.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func itPublisher(t *testing.T) *worker.KafkaPublisher {
	t.Helper()
	pub, err := worker.NewKafkaPublisher(brokers())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pub.Ping(ctx); err != nil {
		t.Fatalf("Kafka (is the stack up?): %v", err)
	}
	return pub
}

func publish(t *testing.T, pub *worker.KafkaPublisher, snaps ...*transport.Snapshot) {
	t.Helper()
	for _, s := range snaps {
		b, err := transport.EncodeSnapshot(s)
		if err != nil {
			t.Fatal(err)
		}
		if err := pub.Publish(context.Background(), transport.SnapshotKey(s.RunID, s.WorkerID), b); err != nil {
			t.Fatal(err)
		}
	}
}

// runningRun creates a run with the given workers, prepares them (clock
// offsets unmeasured, as workers report today) and starts it.
func runningRun(t *testing.T, st *store.Store, id string, workers ...string) uint64 {
	t.Helper()
	ctx := context.Background()
	tok, err := st.NextFenceToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, store.NewRun{ID: id, Config: []byte(`{"rate":3}`), Thresholds: []byte(`{}`), Workers: workers, Fence: tok}); err != nil {
		t.Fatal(err)
	}
	tr := func(from, to runstate.State) {
		if err := st.Transition(ctx, store.Transition{RunID: id, Fence: tok, From: from, To: to}); err != nil {
			t.Fatalf("%s → %s: %v", from, to, err)
		}
	}
	tr(runstate.Pending, runstate.Preparing)
	for _, w := range workers {
		if err := st.RecordPrepareAck(ctx, id, tok, transport.Ack{WorkerID: w, RunID: id, CmdSeq: 1, Status: transport.StatusReady}); err != nil {
			t.Fatal(err)
		}
	}
	tr(runstate.Preparing, runstate.Ready)
	tr(runstate.Ready, runstate.Running)
	return tok
}

func stopRun(t *testing.T, st *store.Store, id string, tok uint64, finals map[string]uint64) {
	t.Helper()
	ctx := context.Background()
	if err := st.Transition(ctx, store.Transition{RunID: id, Fence: tok, From: runstate.Running, To: runstate.Stopping}); err != nil {
		t.Fatal(err)
	}
	for w, f := range finals {
		if err := st.RecordStopAck(ctx, id, tok, transport.Ack{WorkerID: w, RunID: id, CmdSeq: 3, Status: transport.StatusStopped, FinalSeq: &f}); err != nil {
			t.Fatal(err)
		}
	}
}

// liveWait bounds how long a test waits for the pipeline to make progress:
// consumer-group joins, fetch waits and tick periods, on a loaded laptop or
// WSL. It is a deadline for liveness only; a test that passes returns as
// soon as its condition holds, and nothing asserts on how long that took.
const liveWait = 60 * time.Second

// ttlSet reports whether a PTTL reading shows the progress TTL applied.
// Redis expires keys by its wall clock, which WSL2 and Docker Desktop VMs
// step by seconds when they resync, so a reading can exceed what was set.
// The bound catches a missing TTL or a unit error (seconds as
// milliseconds is 1000×), not small drift.
func ttlSet(ttl time.Duration) bool { return ttl > 0 && ttl <= 2*ProgressTTL }

// waitProgress polls key until done holds and returns the hash, or fails
// with the last hash seen.
func waitProgress(t *testing.T, rdb *redis.Client, key string, timeout time.Duration, done func(map[string]string) bool) map[string]string {
	t.Helper()
	var last map[string]string
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		f, err := rdb.HGetAll(context.Background(), key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if last = f; done(f) {
			return f
		}
	}
	t.Fatalf("progress %s not reached after %v; last %v", key, timeout, last)
	return nil
}

func waitResult(t *testing.T, st *store.Store, id string, timeout time.Duration) store.ResultRow {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		r, err := st.GetResult(context.Background(), id)
		if err == nil {
			return r
		}
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
	}
	t.Fatalf("no result for %s after %v", id, timeout)
	return store.ResultRow{}
}

// background runs fn until the test ends.
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

// TestIntegrationPipeline runs two workers' streams through Kafka, one
// consumer and two aggregators at once into Postgres and Redis, and takes
// the verdict the controller would.
func TestIntegrationPipeline(t *testing.T) {
	st := itStore(t)
	pub := itPublisher(t)
	rdb := redis.NewClient(&redis.Options{Addr: env("LF_REDIS", "localhost:6379")})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Redis (is the stack up?): %v", err)
	}
	run := fmt.Sprintf("it-pipe-%d", time.Now().UnixNano())
	tok := runningRun(t, st, run, "w1", "w2")
	group := "lf-it-" + run
	startAtEnd(t, group)
	cl := itConsumer(t, group)
	t.Cleanup(cl.Close)
	background(t, func(ctx context.Context) error { return Consume(ctx, cl, NewIngester(st)) })
	// Two aggregators on one database: every write is monotonic or
	// written once, so they cannot disagree.
	for range 2 {
		agg := New(st, NewRedisProgress(rdb), Config{Period: 100 * time.Millisecond})
		background(t, agg.Run)
	}

	for seq := uint64(0); seq < 3; seq++ {
		publish(t, pub, snap(run, "w1", seq, 1000, 2000, 3000), snap(run, "w2", seq, 1500, 2500, 400_000))
	}
	// Progress is cached while the run is merged, marked provisional. Wait
	// for all six snapshots, not just three seconds: a second exists as
	// soon as its first worker's snapshot is merged.
	key := ProgressKey(run)
	live := waitProgress(t, rdb, key, liveWait, func(f map[string]string) bool { return f["snapshots_merged"] == "6" })
	if live["seconds_merged"] != "3" || live["provisional"] != "true" || live["verdict"] != PendingVerdict || live["ok"] != "18" || live["finalized"] != "false" {
		t.Fatalf("live progress = %v", live)
	}
	if _, err := st.GetResult(context.Background(), run); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("finalized a RUNNING run: %v", err)
	}

	publish(t, pub, final(snap(run, "w1", 3, 1000)), final(snap(run, "w2", 3, 2000)))
	stopRun(t, st, run, tok, map[string]uint64{"w1": 3, "w2": 3})
	res := waitResult(t, st, run, liveWait)

	if res.ExpectedSnapshots != 8 || res.ReceivedSnapshots != 8 || res.RejectedSnapshots != 0 ||
		res.Intended != 20 || res.OK != 20 || res.IntervalNSTotal != 8e9 {
		t.Errorf("result = %+v", res)
	}
	if res.Max < 400_000 || res.P50 < 1500 || res.P50 > 2600 {
		t.Errorf("percentiles = %+v", res.Percentiles)
	}
	secs, err := st.Seconds(context.Background(), run)
	if err != nil || len(secs) != 4 {
		t.Fatalf("seconds = %d, %v", len(secs), err)
	}
	for _, s := range secs {
		if s.SnapshotsMerged != 2 {
			t.Errorf("second %d merged %d snapshots", s.Seq, s.SnapshotsMerged)
		}
	}
	if f := waitProgress(t, rdb, key, liveWait, func(f map[string]string) bool { return f["finalized"] == "true" }); f["verdict"] != PendingVerdict || f["snapshots_merged"] != "8" {
		t.Errorf("progress after finalizing: %v", f)
	}
	if ttl := rdb.PTTL(context.Background(), key).Val(); !ttlSet(ttl) {
		t.Errorf("progress TTL = %v", ttl)
	}

	// The controller's step: evaluate and complete. Nothing reports CPU,
	// clock offsets or the dispatch window yet, so the honest verdict is
	// DEGRADED, never VALID.
	in, th, err := st.VerdictInputs(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	v, err := health.Evaluate(in, th)
	if err != nil {
		t.Fatal(err)
	}
	if v.Verdict != health.Degraded || len(v.Invalid) != 0 || len(v.Unmeasured) != 3 {
		t.Errorf("verdict = %s, invalid %v, unmeasured %v", v.Verdict, v.Invalid, v.Unmeasured)
	}
	if err := st.Transition(context.Background(), store.Transition{RunID: run, Fence: tok, From: runstate.Stopping, To: runstate.Completed,
		Verdict: v.Verdict, VerdictDetail: v.Detail()}); err != nil {
		t.Fatal(err)
	}
}

// Two aggregators publish the same run, each reading Postgres and then
// writing Redis. A slow one must not replace newer progress with older,
// and nothing may replace a finalized hash with an unfinalized one.
func TestIntegrationProgressIgnoresStaleWrites(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: env("LF_REDIS", "localhost:6379")})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis (is the stack up?): %v", err)
	}
	run := fmt.Sprintf("it-stale-%d", time.Now().UnixNano())
	key := ProgressKey(run)
	t.Cleanup(func() { rdb.Del(context.Background(), key) })
	sink := NewRedisProgress(rdb)
	put := func(merged, ok int64, finalized bool) {
		t.Helper()
		p := LiveProgress{Finalized: finalized, UpdatedAt: time.Now()}
		p.SnapshotsMerged, p.OK = merged, ok
		if err := sink.Put(ctx, run, p); err != nil {
			t.Fatal(err)
		}
	}
	check := func(merged, ok, finalized string) {
		t.Helper()
		f := rdb.HGetAll(ctx, key).Val()
		if f["snapshots_merged"] != merged || f["ok"] != ok || f["finalized"] != finalized || f["provisional"] != "true" {
			t.Errorf("progress = %v, want snapshots_merged=%s ok=%s finalized=%s", f, merged, ok, finalized)
		}
		if ttl := rdb.PTTL(ctx, key).Val(); !ttlSet(ttl) {
			t.Errorf("TTL = %v", ttl)
		}
	}

	put(6, 18, false)
	put(5, 15, false) // a slower aggregator's older read
	check("6", "18", "false")
	put(6, 18, false) // the same read again is not stale
	check("6", "18", "false")
	put(8, 20, true)
	put(8, 20, false) // merged before the result, published after it
	check("8", "20", "true")
}

// crashingStore commits the n-th snapshot insert to Postgres and then
// reports a failure, as if the process died between the database commit
// and the Kafka offset commit.
type crashingStore struct {
	*store.Store
	after   int64
	inserts atomic.Int64
	crashed chan struct{}
	once    sync.Once
}

var errCrash = errors.New("simulated crash after the insert committed")

func (c *crashingStore) InsertSnapshot(ctx context.Context, r store.SnapshotRow) (bool, error) {
	if c.inserts.Load() >= c.after {
		c.once.Do(func() { close(c.crashed) })
		<-ctx.Done()
		return false, ctx.Err()
	}
	ok, err := c.Store.InsertSnapshot(ctx, r)
	if err == nil && c.inserts.Add(1) == c.after {
		c.once.Do(func() { close(c.crashed) })
		return false, errCrash
	}
	return ok, err
}

// A crash between the Postgres commit and the offset commit means
// redelivery, and redelivery must not double-count.
func TestIntegrationCrashRedeliversWithoutDoubleCounting(t *testing.T) {
	st := itStore(t)
	pub := itPublisher(t)
	run := fmt.Sprintf("it-crash-%d", time.Now().UnixNano())
	tok := runningRun(t, st, run, "w1")
	group := "lf-it-" + run
	startAtEnd(t, group)

	const n = 12
	for seq := uint64(0); seq < n-1; seq++ {
		publish(t, pub, snap(run, "w1", seq, 1000*toI64(seq+1)))
	}
	publish(t, pub, final(snap(run, "w1", n-1, 5000)))

	// First aggregator: dies after storing the 5th snapshot.
	cs := &crashingStore{Store: st, after: 5, crashed: make(chan struct{})}
	cl1 := itConsumer(t, group)
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan struct{})
	go func() { defer close(done1); _ = Consume(ctx1, cl1, NewIngester(cs)) }()
	select {
	case <-cs.crashed:
	case <-time.After(liveWait):
		t.Fatal("first consumer never reached the crash point")
	}
	cancel1()
	<-done1
	cl1.Close()

	// Second aggregator picks up from the last committed offset.
	cl2 := itConsumer(t, group)
	t.Cleanup(cl2.Close)
	background(t, func(ctx context.Context) error { return Consume(ctx, cl2, NewIngester(st)) })
	background(t, New(st, nil, Config{Period: 100 * time.Millisecond}).Run)
	stopRun(t, st, run, tok, map[string]uint64{"w1": n - 1})
	res := waitResult(t, st, run, liveWait)

	if res.ExpectedSnapshots != n || res.ReceivedSnapshots != n || res.Intended != n || res.OK != n || res.RejectedSnapshots != 0 {
		t.Errorf("result after crash = %+v, want %d snapshots counted once", res, n)
	}
}

// Redis is only a cache: with it unreachable, results are still written.
func TestIntegrationRedisDownDoesNotBlockResults(t *testing.T) {
	st := itStore(t)
	pub := itPublisher(t)
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	run := fmt.Sprintf("it-noredis-%d", time.Now().UnixNano())
	tok := runningRun(t, st, run, "w1")
	group := "lf-it-" + run
	startAtEnd(t, group)
	cl := itConsumer(t, group)
	t.Cleanup(cl.Close)
	background(t, func(ctx context.Context) error { return Consume(ctx, cl, NewIngester(st)) })
	background(t, New(st, NewRedisProgress(rdb), Config{Period: 100 * time.Millisecond}).Run)

	publish(t, pub, snap(run, "w1", 0, 100), final(snap(run, "w1", 1, 200)))
	stopRun(t, st, run, tok, map[string]uint64{"w1": 1})
	if res := waitResult(t, st, run, liveWait); res.ReceivedSnapshots != 2 {
		t.Errorf("result = %+v", res)
	}
}
