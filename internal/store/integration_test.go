//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/safety"
	"github.com/loadforge/loadforge/internal/transport"
)

// These tests need the compose stack (CLAUDE.md). Each one works in its own
// Postgres schema, dropped afterwards, so they neither see nor leave data.

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func dsn() string {
	return env("LF_PG_DSN", "postgres://loadforge:loadforge@localhost:5432/loadforge?sslmode=disable")
}

// schemaPool returns a pool whose search_path is a fresh, empty schema.
func schemaPool(t *testing.T) *pgxpool.Pool {
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
	schema := fmt.Sprintf("lf_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })

	cfg, err := pgxpool.ParseConfig(dsn())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// testStore is a migrated Store in its own schema.
func testStore(t *testing.T) *Store {
	t.Helper()
	pool := schemaPool(t)
	if err := Migrate(context.Background(), pool, Migrations); err != nil {
		t.Fatal(err)
	}
	return &Store{pool: pool}
}

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: env("LF_REDIS", "localhost:6379")})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Redis (is the stack up?): %v", err)
	}
	return rdb
}

func uniqueKey(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }

// newRun creates a PENDING run with workers w1..wn and returns its token.
func newRun(t *testing.T, s *Store, id string, workers ...string) uint64 {
	t.Helper()
	ctx := context.Background()
	tok, err := s.NextFenceToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) == 0 {
		workers = []string{"w1", "w2"}
	}
	if err := s.CreateRun(ctx, NewRun{ID: id, Config: []byte(`{"rate":100}`), Thresholds: []byte(`{}`), Workers: workers, Fence: tok}); err != nil {
		t.Fatal(err)
	}
	return tok
}

func move(t *testing.T, s *Store, id string, tok uint64, path ...runstate.State) {
	t.Helper()
	for i := 1; i < len(path); i++ {
		tr := Transition{RunID: id, Fence: tok, From: path[i-1], To: path[i]}
		if path[i].Terminal() {
			tr.Verdict = health.Invalid
			if path[i] == runstate.Completed {
				tr.Verdict = health.Degraded
			}
		}
		if err := s.Transition(context.Background(), tr); err != nil {
			t.Fatalf("%s → %s: %v", path[i-1], path[i], err)
		}
	}
}

// ---- migrations ----

func TestIntegrationMigrationsApplyOnce(t *testing.T) {
	ctx := context.Background()
	pool := schemaPool(t)
	for i := 0; i < 2; i++ {
		if err := Migrate(ctx, pool, Migrations); err != nil {
			t.Fatalf("migrate #%d: %v", i+1, err)
		}
	}
	files, _ := LoadMigrations(Migrations)
	var n int
	var sum string
	if err := pool.QueryRow(ctx, `SELECT count(*), max(checksum) FROM schema_migrations`).Scan(&n, &sum); err != nil {
		t.Fatal(err)
	}
	if n != len(files) || sum != files[len(files)-1].Checksum {
		t.Errorf("schema_migrations has %d rows (checksum %s), want %d", n, sum, len(files))
	}
}

func TestIntegrationConcurrentMigrateIsSafe(t *testing.T) {
	pool := schemaPool(t)
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = Migrate(context.Background(), pool, Migrations)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("migrate #%d: %v", i, err)
		}
	}
}

func TestIntegrationEditedMigrationRefused(t *testing.T) {
	ctx := context.Background()
	pool := schemaPool(t)
	v1 := mapFS(map[string]string{"0001_a.sql": "CREATE TABLE a (x int);"})
	if err := Migrate(ctx, pool, v1); err != nil {
		t.Fatal(err)
	}
	edited := mapFS(map[string]string{"0001_a.sql": "CREATE TABLE a (x bigint);"})
	if err := Migrate(ctx, pool, edited); !errors.Is(err, ErrMigrationEdited) {
		t.Fatalf("edited migration: err = %v, want ErrMigrationEdited", err)
	}
	v2 := mapFS(map[string]string{"0001_a.sql": "CREATE TABLE a (x int);", "0002_b.sql": "CREATE TABLE b (y int);"})
	if err := Migrate(ctx, pool, v2); err != nil {
		t.Fatalf("new migration: %v", err)
	}
	if err := Migrate(ctx, pool, v1); !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("older binary: err = %v, want ErrSchemaTooNew", err)
	}
}

func TestIntegrationFailedMigrationRollsBack(t *testing.T) {
	ctx := context.Background()
	pool := schemaPool(t)
	bad := mapFS(map[string]string{"0001_a.sql": "CREATE TABLE a (x int); SELECT no_such_function();"})
	if err := Migrate(ctx, pool, bad); err == nil {
		t.Fatal("broken migration applied")
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("schema_migrations rows = %d (err %v), want 0", n, err)
	}
	var exists bool
	_ = pool.QueryRow(ctx, `SELECT to_regclass('a') IS NOT NULL`).Scan(&exists)
	if exists {
		t.Error("table from the failed migration survived")
	}
}

// ---- fencing ----

func TestIntegrationFenceTokensIncrease(t *testing.T) {
	s := testStore(t)
	prev := uint64(0)
	for range 5 {
		tok, err := s.NextFenceToken(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if tok <= prev {
			t.Fatalf("token %d after %d", tok, prev)
		}
		prev = tok
	}
}

func TestIntegrationStaleFenceRejected(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	old := newRun(t, s, "run-fence")
	newer, _ := s.NextFenceToken(ctx)

	// The new holder writes first, which raises the bar in Postgres.
	if err := s.Transition(ctx, Transition{RunID: "run-fence", Fence: newer, From: runstate.Pending, To: runstate.Preparing}); err != nil {
		t.Fatal(err)
	}
	before, _ := s.Events(ctx, "run-fence")

	writes := map[string]error{
		"transition":       s.Transition(ctx, Transition{RunID: "run-fence", Fence: old, From: runstate.Preparing, To: runstate.Ready}),
		"event":            s.AppendEvent(ctx, "run-fence", old, Event{Kind: EventAck}),
		"prepare ack":      s.RecordPrepareAck(ctx, "run-fence", old, transport.Ack{WorkerID: "w1", Status: transport.StatusReady}),
		"stop ack":         s.RecordStopAck(ctx, "run-fence", old, transport.Ack{WorkerID: "w1", Status: transport.StatusStopped, FinalSeq: ptr(uint64(3))}),
		"worker lost":      s.MarkWorkerLost(ctx, "run-fence", old, "w1", nil),
		"finalize request": s.RequestFinalize(ctx, "run-fence", old),
		"token zero":       s.AppendEvent(ctx, "run-fence", 0, Event{Kind: EventAck}),
	}
	for name, err := range writes {
		if !errors.Is(err, ErrStaleFence) {
			t.Errorf("%s with the old token: err = %v, want ErrStaleFence", name, err)
		}
	}

	r, _ := s.GetRun(ctx, "run-fence")
	after, _ := s.Events(ctx, "run-fence")
	ws, _ := s.RunWorkers(ctx, "run-fence")
	if r.State != runstate.Preparing || r.FenceToken != newer || r.FinalizeRequested {
		t.Errorf("run changed by stale writes: %+v", r)
	}
	if len(after) != len(before) {
		t.Errorf("stale writes appended %d events", len(after)-len(before))
	}
	if ws[0].PrepareStatus != "" || ws[0].FinalSeq != nil || ws[0].Lost {
		t.Errorf("worker row changed by stale writes: %+v", ws[0])
	}
}

func TestIntegrationLeaseFencingSurvivesRedisLoss(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	rdb := testRedis(t)
	leases := NewLeases(rdb)
	run := uniqueKey("run-redisloss")
	key := RunLeaseKey(run)
	t.Cleanup(func() { rdb.Del(context.Background(), key) })

	first := newRun(t, s, run)
	if ok, err := leases.Acquire(ctx, key, first); err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	// Redis restarts with persistence off: every lease is gone.
	rdb.Del(ctx, key)

	second, _ := s.NextFenceToken(ctx)
	if second <= first {
		t.Fatalf("token %d after Redis loss is not above %d", second, first)
	}
	if ok, err := leases.Acquire(ctx, key, second); err != nil || !ok {
		t.Fatalf("second controller acquire: %v %v", ok, err)
	}
	if ok, _ := leases.Renew(ctx, key, first); ok {
		t.Error("first controller renewed a lease it lost")
	}
	if err := s.Transition(ctx, Transition{RunID: run, Fence: second, From: runstate.Pending, To: runstate.Preparing}); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(ctx, Transition{RunID: run, Fence: first, From: runstate.Preparing, To: runstate.Ready}); !errors.Is(err, ErrStaleFence) {
		t.Errorf("first controller's write after Redis loss: err = %v, want ErrStaleFence", err)
	}
}

// ---- leases ----

func TestIntegrationLeaseExclusiveAndHolderOnly(t *testing.T) {
	ctx := context.Background()
	rdb := testRedis(t)
	l := NewLeases(rdb)
	key := RunLeaseKey(uniqueKey("run-lease"))
	t.Cleanup(func() { rdb.Del(context.Background(), key) })

	if ok, err := l.Acquire(ctx, key, 10); err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	if ok, _ := l.Acquire(ctx, key, 11); ok {
		t.Fatal("second holder acquired a held lease")
	}
	if tok, held, _ := l.Holder(ctx, key); !held || tok != 10 {
		t.Fatalf("holder = %d %v, want 10", tok, held)
	}
	ttl := rdb.PTTL(ctx, key).Val()
	if ttl <= 0 || ttl > safety.LeaseTTL {
		t.Errorf("lease TTL %v, want (0, %v]", ttl, safety.LeaseTTL)
	}

	if ok, _ := l.Renew(ctx, key, 11); ok {
		t.Error("non-holder renewed")
	}
	if ok, _ := l.Release(ctx, key, 11); ok {
		t.Error("non-holder released")
	}
	rdb.PExpire(ctx, key, time.Second)
	if ok, err := l.Renew(ctx, key, 10); err != nil || !ok {
		t.Fatalf("holder renew: %v %v", ok, err)
	}
	if ttl := rdb.PTTL(ctx, key).Val(); ttl <= time.Second {
		t.Errorf("renew left TTL at %v", ttl)
	}
	if ok, err := l.Release(ctx, key, 10); err != nil || !ok {
		t.Fatalf("holder release: %v %v", ok, err)
	}
	if _, held, _ := l.Holder(ctx, key); held {
		t.Error("lease still held after release")
	}
	if ok, _ := l.Renew(ctx, key, 10); ok {
		t.Error("renewed a released lease")
	}
	if ok, _ := l.Acquire(ctx, key, 11); !ok {
		t.Error("could not acquire a released lease")
	}
}

// ---- runs and state ----

func TestIntegrationLifecycleAndEventLog(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	tok := newRun(t, s, "run-life")
	if err := s.CreateRun(ctx, NewRun{ID: "run-life", Config: []byte(`{}`), Thresholds: []byte(`{}`), Workers: []string{"w1"}, Fence: tok}); !errors.Is(err, ErrExists) {
		t.Errorf("duplicate create: err = %v, want ErrExists", err)
	}

	move(t, s, "run-life", tok, runstate.Pending, runstate.Preparing)
	for _, w := range []string{"w1", "w2"} {
		if err := s.RecordPrepareAck(ctx, "run-life", tok, transport.Ack{WorkerID: w, RunID: "run-life", CmdSeq: 1, Status: transport.StatusReady}); err != nil {
			t.Fatal(err)
		}
	}
	move(t, s, "run-life", tok, runstate.Preparing, runstate.Ready, runstate.Running, runstate.Stopping)
	for w, seq := range map[string]uint64{"w1": 10, "w2": 11} {
		a := transport.Ack{WorkerID: w, RunID: "run-life", CmdSeq: 3, Status: transport.StatusStopped, FinalSeq: ptr(seq)}
		if err := s.RecordStopAck(ctx, "run-life", tok, a); err != nil {
			t.Fatal(err)
		}
	}
	detail, _ := json.Marshal(map[string]any{"unmeasured": []string{"cpu", "clock_offset"}})
	if err := s.Transition(ctx, Transition{RunID: "run-life", Fence: tok, From: runstate.Stopping, To: runstate.Completed,
		Verdict: health.Degraded, VerdictDetail: detail}); err != nil {
		t.Fatal(err)
	}

	r, err := s.GetRun(ctx, "run-life")
	if err != nil {
		t.Fatal(err)
	}
	if r.State != runstate.Completed || r.Verdict != health.Degraded || len(r.VerdictDetail) == 0 {
		t.Errorf("run = %+v", r)
	}
	ws, _ := s.RunWorkers(ctx, "run-life")
	if len(ws) != 2 || ws[0].PrepareStatus != transport.StatusReady || *ws[1].FinalSeq != 11 ||
		ws[0].ClockOffsetNS != nil || ws[0].CapacityRPS != nil {
		t.Errorf("workers = %+v", ws)
	}

	evs, _ := s.Events(ctx, "run-life")
	var kinds []string
	var states []runstate.State
	for _, e := range evs {
		kinds = append(kinds, e.Kind)
		if e.Kind == EventTransition {
			states = append(states, e.ToState)
		}
	}
	wantStates := []runstate.State{runstate.Preparing, runstate.Ready, runstate.Running, runstate.Stopping, runstate.Completed}
	if fmt.Sprint(states) != fmt.Sprint(wantStates) {
		t.Errorf("transitions %v, want %v", states, wantStates)
	}
	if kinds[0] != EventCreated || len(evs) != 1+5+2+2 {
		t.Errorf("events %v", kinds)
	}
	if err := s.Transition(ctx, Transition{RunID: "run-life", Fence: tok, From: runstate.Completed, To: runstate.Aborted, Verdict: health.Invalid}); err == nil {
		t.Error("left a terminal state")
	}
}

func TestIntegrationTransitionStateConflict(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	tok := newRun(t, s, "run-race")
	move(t, s, "run-race", tok, runstate.Pending, runstate.Preparing)
	err := s.Transition(ctx, Transition{RunID: "run-race", Fence: tok, From: runstate.Pending, To: runstate.Preparing})
	if !errors.Is(err, ErrStateConflict) {
		t.Errorf("second move from PENDING: err = %v, want ErrStateConflict", err)
	}
	if err := s.Transition(ctx, Transition{RunID: "nope", Fence: tok, From: runstate.Pending, To: runstate.Preparing}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown run: err = %v, want ErrNotFound", err)
	}
}

func TestIntegrationSchemaEnforcesVerdicts(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	newRun(t, s, "run-schema")
	for name, sql := range map[string]string{
		"terminal without verdict": `UPDATE runs SET state = 'COMPLETED' WHERE id = 'run-schema'`,
		"verdict while running":    `UPDATE runs SET verdict = 'VALID' WHERE id = 'run-schema'`,
		"aborted but valid":        `UPDATE runs SET state = 'ABORTED', verdict = 'VALID' WHERE id = 'run-schema'`,
		"unknown verdict":          `UPDATE runs SET state = 'COMPLETED', verdict = 'OK' WHERE id = 'run-schema'`,
		"unknown state":            `UPDATE runs SET state = 'PAUSED' WHERE id = 'run-schema'`,
	} {
		if _, err := s.pool.Exec(ctx, sql); err == nil {
			t.Errorf("%s: schema accepted it", name)
		}
	}
}

func TestIntegrationRunEventsAppendOnly(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	newRun(t, s, "run-log")
	if _, err := s.pool.Exec(ctx, `UPDATE run_events SET kind = 'x' WHERE run_id = 'run-log'`); err == nil {
		t.Error("run_events row updated")
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM run_events WHERE run_id = 'run-log'`); err == nil {
		t.Error("run_events row deleted")
	}
}

func TestIntegrationStopAckConflictIsProtocolViolation(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	tok := newRun(t, s, "run-final")
	stop := func(seq uint64) error {
		return s.RecordStopAck(ctx, "run-final", tok, transport.Ack{WorkerID: "w1", CmdSeq: 3, Status: transport.StatusStopped, FinalSeq: &seq})
	}
	if err := stop(7); err != nil {
		t.Fatal(err)
	}
	if err := stop(7); err != nil {
		t.Errorf("repeated identical ack: %v", err)
	}
	if err := stop(9); !errors.Is(err, ErrProtocolViolation) {
		t.Fatalf("conflicting final_seq: err = %v, want ErrProtocolViolation", err)
	}
	ws, _ := s.RunWorkers(ctx, "run-final")
	if *ws[0].FinalSeq != 7 {
		t.Errorf("final_seq overwritten to %d", *ws[0].FinalSeq)
	}
	evs, _ := s.Events(ctx, "run-final")
	if last := evs[len(evs)-1]; last.Kind != EventProtocolViolation || last.WorkerID != "w1" {
		t.Errorf("last event %+v, want a protocol_violation for w1", last)
	}
	err := s.RecordStopAck(ctx, "run-final", tok, transport.Ack{WorkerID: "intruder", Status: transport.StatusStopped, FinalSeq: ptr(uint64(1))})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("ack from a non-member: err = %v, want ErrNotFound", err)
	}
}

func TestIntegrationActiveRunsAndFinalizeRequest(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := newRun(t, s, "run-a")
	b := newRun(t, s, "run-b")
	move(t, s, "run-b", b, runstate.Pending, runstate.Aborted)
	if err := s.RequestFinalize(ctx, "run-a", a); err != nil {
		t.Fatal(err)
	}
	active, err := s.ActiveRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != "run-a" || !active[0].FinalizeRequested {
		t.Errorf("active = %+v", active)
	}
	if err := s.MarkWorkerLost(ctx, "run-a", a, "w2", []byte(`{"silent_ms":5000}`)); err != nil {
		t.Fatal(err)
	}
	ws, _ := s.RunWorkers(ctx, "run-a")
	if ws[0].Lost || !ws[1].Lost {
		t.Errorf("lost flags %v %v", ws[0].Lost, ws[1].Lost)
	}
}

// ---- aggregator rows ----

func hists(b byte) Hists {
	return Hists{Latency: []byte{b}, Lag: []byte{b, 1}, SendDelay: []byte{b, 2}}
}

func TestIntegrationSnapshotInsertDedupes(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	row := SnapshotRow{RunID: "run-snap", WorkerID: "w1", Seq: 4, IntervalStartNS: 1, IntervalEndNS: 2, IntervalNS: 1_000_000_000,
		Counts: Counts{Intended: 100, OK: 98, Err: 1, Timeout: 1}, Hists: hists(1)}
	if ins, err := s.InsertSnapshot(ctx, row); err != nil || !ins {
		t.Fatalf("first insert: %v %v", ins, err)
	}
	dup := row
	dup.OK = 0 // a redelivery is the same message; even if it were not, the first wins
	if ins, err := s.InsertSnapshot(ctx, dup); err != nil || ins {
		t.Fatalf("redelivery: inserted=%v err=%v", ins, err)
	}
	final := row
	final.WorkerID, final.Final, final.Reason = "w2", true, string(transport.ReasonStop)
	if ins, err := s.InsertSnapshot(ctx, final); err != nil || !ins {
		t.Fatalf("other worker: %v %v", ins, err)
	}
	got, err := s.SnapshotsForSeq(ctx, "run-snap", 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].OK != 98 || got[0].IntervalNS != 1_000_000_000 || !got[1].Final || got[1].Reason != "stop" {
		t.Errorf("rows = %+v", got)
	}
	bad := row
	bad.Seq, bad.Reason, bad.Final = 5, "exploded", true
	if _, err := s.InsertSnapshot(ctx, bad); err == nil {
		t.Error("unknown stop reason stored")
	}
}

func TestIntegrationRunSecondsUpsertIsMonotonic(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	newRun(t, s, "run-sec")
	sec := func(merged int, ok int64) SecondRow {
		return SecondRow{RunID: "run-sec", Seq: 0, SnapshotsMerged: merged, Counts: Counts{OK: ok}, Hists: hists(byte(merged)),
			Percentiles: Percentiles{P99: ok}}
	}
	if w, err := s.UpsertSecond(ctx, sec(2, 200)); err != nil || !w {
		t.Fatalf("first: %v %v", w, err)
	}
	if w, err := s.UpsertSecond(ctx, sec(3, 300)); err != nil || !w {
		t.Fatalf("fresher merge: %v %v", w, err)
	}
	if w, err := s.UpsertSecond(ctx, sec(2, 999)); err != nil || w {
		t.Fatalf("stale merge overwrote: written=%v err=%v", w, err)
	}
	if w, err := s.UpsertSecond(ctx, sec(3, 300)); err != nil || !w {
		t.Fatalf("equal merge (same snapshot set): %v %v", w, err)
	}
	rows, _ := s.Seconds(ctx, "run-sec")
	if len(rows) != 1 || rows[0].SnapshotsMerged != 3 || rows[0].OK != 300 || rows[0].P99 != 300 || rows[0].Latency[0] != 3 {
		t.Errorf("rows = %+v", rows)
	}
}

func TestIntegrationResultWrittenOnce(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	newRun(t, s, "run-res")
	if _, err := s.GetResult(ctx, "run-res"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing result: err = %v", err)
	}
	r := ResultRow{RunID: "run-res", ExpectedSnapshots: 10, ReceivedSnapshots: 9, IntervalNSTotal: 9_500_000_000,
		Counts: Counts{Intended: 950, OK: 950}, Hists: hists(1), Percentiles: Percentiles{P50: 1000, Max: 2_000_000}}
	if ins, err := s.PutResult(ctx, r); err != nil || !ins {
		t.Fatalf("put: %v %v", ins, err)
	}
	r2 := r
	r2.ReceivedSnapshots = 10
	if ins, err := s.PutResult(ctx, r2); err != nil || ins {
		t.Fatalf("second put: inserted=%v err=%v", ins, err)
	}
	got, err := s.GetResult(ctx, "run-res")
	if err != nil {
		t.Fatal(err)
	}
	if got.ReceivedSnapshots != 9 || got.Max != 2_000_000 || got.IntervalNSTotal != 9_500_000_000 {
		t.Errorf("result = %+v", got)
	}
	if _, err := s.PutResult(ctx, ResultRow{RunID: "no-such-run", Hists: hists(1)}); err == nil {
		t.Error("result for an unknown run stored")
	}
}

func TestIntegrationRejectsDedupedPerOffset(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	rj := Reject{Topic: transport.SnapshotTopic, Partition: 3, Offset: 42, Key: []byte("run-x:w1"), RunID: "run-x", WorkerID: "w1", Error: "bad histogram range"}
	for range 2 {
		if err := s.InsertReject(ctx, rj); err != nil {
			t.Fatal(err)
		}
	}
	rj.Offset, rj.Key, rj.RunID, rj.WorkerID = 43, []byte{0xff}, "", ""
	if err := s.InsertReject(ctx, rj); err != nil {
		t.Fatal(err)
	}
	if n, err := s.RejectCount(ctx, "run-x"); err != nil || n != 1 {
		t.Errorf("rejects for run-x = %d (err %v), want 1", n, err)
	}
}

func ptr[T any](v T) *T { return &v }
