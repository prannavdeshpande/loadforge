package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
)

// Reads the aggregator needs to ingest, merge and finalize, and the
// verdict engine's inputs. Like the aggregator's writes, none is fenced.

// StreamStatus is what is known about one worker's snapshot stream.
type StreamStatus struct {
	WorkerID       string
	AckFinalSeq    *uint64 // final_seq from the worker's STOP ack
	StoredFinalSeq *uint64 // seq of the stored snapshot marked final
	MaxStoredSeq   *uint64 // highest stored seq; nil when nothing is stored
	Stored         int64   // snapshots stored
}

// FinalSeq is the stream's last seq: the STOP ack's if it reported one,
// else the stored final snapshot's.
func (s StreamStatus) FinalSeq() (uint64, bool) {
	switch {
	case s.AckFinalSeq != nil:
		return *s.AckFinalSeq, true
	case s.StoredFinalSeq != nil:
		return *s.StoredFinalSeq, true
	}
	return 0, false
}

const streamColumns = `w.worker_id, w.final_seq,
	(SELECT min(s.seq) FROM run_snapshots s WHERE s.run_id = w.run_id AND s.worker_id = w.worker_id AND s.final),
	(SELECT max(s.seq) FROM run_snapshots s WHERE s.run_id = w.run_id AND s.worker_id = w.worker_id),
	(SELECT count(*) FROM run_snapshots s WHERE s.run_id = w.run_id AND s.worker_id = w.worker_id)`

func scanStream(row pgx.Row) (StreamStatus, error) {
	var s StreamStatus
	err := row.Scan(&s.WorkerID, &s.AckFinalSeq, &s.StoredFinalSeq, &s.MaxStoredSeq, &s.Stored)
	return s, err
}

// Stream reports a member's stream. It fails with ErrNotFound if the run
// does not exist or the worker is not one of its members.
func (s *Store) Stream(ctx context.Context, runID, workerID string) (StreamStatus, error) {
	st, err := scanStream(s.pool.QueryRow(ctx, `SELECT `+streamColumns+`
		FROM run_workers w WHERE w.run_id = $1 AND w.worker_id = $2`, runID, workerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return StreamStatus{}, fmt.Errorf("%w: worker %s in run %s", ErrNotFound, workerID, runID)
	}
	if err != nil {
		return StreamStatus{}, fmt.Errorf("store: stream %s/%s: %w", runID, workerID, err)
	}
	return st, nil
}

// Streams reports every member's stream, ordered by worker ID.
func (s *Store) Streams(ctx context.Context, runID string) ([]StreamStatus, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+streamColumns+`
		FROM run_workers w WHERE w.run_id = $1 ORDER BY w.worker_id`, runID)
	if err != nil {
		return nil, fmt.Errorf("store: streams %s: %w", runID, err)
	}
	defer rows.Close()
	var out []StreamStatus
	for rows.Next() {
		st, err := scanStream(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// GetSnapshot reads one stored snapshot.
func (s *Store) GetSnapshot(ctx context.Context, runID, workerID string, seq uint64) (SnapshotRow, error) {
	var r SnapshotRow
	err := s.pool.QueryRow(ctx, `SELECT `+snapshotColumns+` FROM run_snapshots
		WHERE run_id = $1 AND worker_id = $2 AND seq = $3`, runID, workerID, seq).Scan(
		&r.RunID, &r.WorkerID, &r.Seq, &r.IntervalStartNS, &r.IntervalEndNS, &r.IntervalNS,
		&r.Intended, &r.OK, &r.Err, &r.Timeout, &r.Dropped, &r.Unfinished, &r.Clamped,
		&r.Latency, &r.Lag, &r.SendDelay, &r.Final, &r.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return SnapshotRow{}, fmt.Errorf("%w: snapshot %s/%s/%d", ErrNotFound, runID, workerID, seq)
	}
	return r, err
}

// ForEachSnapshot calls fn for every stored snapshot of a run, in (seq,
// worker) order, streaming rather than loading them all.
func (s *Store) ForEachSnapshot(ctx context.Context, runID string, fn func(SnapshotRow) error) error {
	rows, err := s.pool.Query(ctx, `SELECT `+snapshotColumns+` FROM run_snapshots
		WHERE run_id = $1 ORDER BY seq, worker_id`, runID)
	if err != nil {
		return fmt.Errorf("store: snapshots for %s: %w", runID, err)
	}
	defer rows.Close()
	for rows.Next() {
		var r SnapshotRow
		if err := rows.Scan(&r.RunID, &r.WorkerID, &r.Seq, &r.IntervalStartNS, &r.IntervalEndNS, &r.IntervalNS,
			&r.Intended, &r.OK, &r.Err, &r.Timeout, &r.Dropped, &r.Unfinished, &r.Clamped,
			&r.Latency, &r.Lag, &r.SendDelay, &r.Final, &r.Reason); err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

// SecondKey names one second of a run.
type SecondKey struct {
	RunID string
	Seq   uint64
}

// DirtySeconds lists seconds of unfinalized runs that have more stored
// snapshots than their run_seconds row was merged from (or no row yet), at
// most limit of them, oldest first.
func (s *Store) DirtySeconds(ctx context.Context, limit int) ([]SecondKey, error) {
	rows, err := s.pool.Query(ctx, `SELECT s.run_id, s.seq
		FROM run_snapshots s
		LEFT JOIN run_seconds r ON r.run_id = s.run_id AND r.seq = s.seq
		WHERE NOT EXISTS (SELECT 1 FROM run_results res WHERE res.run_id = s.run_id)
		GROUP BY s.run_id, s.seq, r.snapshots_merged
		HAVING count(*) > coalesce(r.snapshots_merged, 0)
		ORDER BY s.run_id, s.seq
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: dirty seconds: %w", err)
	}
	defer rows.Close()
	var out []SecondKey
	for rows.Next() {
		var k SecondKey
		if err := rows.Scan(&k.RunID, &k.Seq); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// Progress is a run's merged seconds so far: summed counts, and the latest
// second's percentiles (percentiles are never summed or averaged).
type Progress struct {
	Seconds int64
	// SnapshotsMerged is the sum of the run's run_seconds.snapshots_merged.
	// Each second's count only grows, so this orders progress reads: a read
	// with a higher count is newer.
	SnapshotsMerged int64
	LatestSeq       uint64
	IntervalNSTotal int64
	Counts
	Latest Percentiles
}

// RunProgress sums a run's run_seconds rows. ok is false when there are none.
func (s *Store) RunProgress(ctx context.Context, runID string) (Progress, bool, error) {
	var p Progress
	err := s.pool.QueryRow(ctx, `SELECT count(*), coalesce(sum(snapshots_merged), 0)::bigint,
			coalesce(max(seq), 0), coalesce(sum(interval_ns_total), 0)::bigint,
			coalesce(sum(intended), 0)::bigint, coalesce(sum(ok), 0)::bigint, coalesce(sum(err), 0)::bigint,
			coalesce(sum(timeout), 0)::bigint, coalesce(sum(dropped), 0)::bigint,
			coalesce(sum(unfinished), 0)::bigint, coalesce(sum(clamped), 0)::bigint
		FROM run_seconds WHERE run_id = $1`, runID).Scan(&p.Seconds, &p.SnapshotsMerged, &p.LatestSeq, &p.IntervalNSTotal,
		&p.Intended, &p.OK, &p.Err, &p.Timeout, &p.Dropped, &p.Unfinished, &p.Clamped)
	if err != nil {
		return Progress{}, false, fmt.Errorf("store: progress %s: %w", runID, err)
	}
	if p.Seconds == 0 {
		return p, false, nil
	}
	l := &p.Latest
	err = s.pool.QueryRow(ctx, `SELECT p50_us, p90_us, p99_us, p999_us, max_us, lag_p99_us
		FROM run_seconds WHERE run_id = $1 AND seq = $2`, runID, p.LatestSeq).Scan(&l.P50, &l.P90, &l.P99, &l.P999, &l.Max, &l.LagP99)
	if err != nil {
		return Progress{}, false, fmt.Errorf("store: progress %s: %w", runID, err)
	}
	return p, true, nil
}

// FinalizeCandidate is a run the aggregator may finalize: it is stopping or
// terminal and has no result yet.
type FinalizeCandidate struct {
	RunID             string
	State             runstate.State
	FinalizeRequested bool
}

// FinalizeCandidates lists runs in STOPPING, COMPLETED or ABORTED without a
// result.
func (s *Store) FinalizeCandidates(ctx context.Context) ([]FinalizeCandidate, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, state, finalize_requested FROM runs r
		WHERE state IN ('STOPPING', 'COMPLETED', 'ABORTED')
		  AND NOT EXISTS (SELECT 1 FROM run_results res WHERE res.run_id = r.id)
		ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("store: finalize candidates: %w", err)
	}
	defer rows.Close()
	var out []FinalizeCandidate
	for rows.Next() {
		var (
			c     FinalizeCandidate
			state string
		)
		if err := rows.Scan(&c.RunID, &state, &c.FinalizeRequested); err != nil {
			return nil, err
		}
		c.State = runstate.State(state)
		out = append(out, c)
	}
	return out, rows.Err()
}

// VerdictInputs assembles the health signals for a run from Postgres, with
// the run's thresholds. Nothing reports worker CPU or the dispatch window
// yet, so those signals are unmeasured. Aborted reflects the stored state;
// a controller about to abort sets it itself.
func (s *Store) VerdictInputs(ctx context.Context, runID string) (health.Inputs, health.Thresholds, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return health.Inputs{}, health.Thresholds{}, err
	}
	th, err := health.ParseThresholds(run.Thresholds)
	if err != nil {
		return health.Inputs{}, health.Thresholds{}, fmt.Errorf("store: run %s: %w", runID, err)
	}
	var cfg struct {
		Rate float64 `json:"rate"` // per worker (transport.Prepare)
	}
	if err := json.Unmarshal(run.Config, &cfg); err != nil {
		return health.Inputs{}, health.Thresholds{}, fmt.Errorf("store: run %s config: %w", runID, err)
	}
	workers, err := s.RunWorkers(ctx, runID)
	if err != nil {
		return health.Inputs{}, health.Thresholds{}, err
	}
	in := health.Inputs{
		Aborted:             run.State == runstate.Aborted,
		Workers:             len(workers),
		TargetRatePerWorker: cfg.Rate,
	}
	for _, w := range workers {
		in.ClockOffsetsNS = append(in.ClockOffsetsNS, w.ClockOffsetNS)
		if w.Lost {
			in.LostWorkers = append(in.LostWorkers, w.WorkerID)
		}
	}

	res, err := s.GetResult(ctx, runID)
	switch {
	case errors.Is(err, ErrNotFound):
	case err != nil:
		return health.Inputs{}, health.Thresholds{}, err
	default:
		in.HasResult = true
		in.Intended, in.Dropped = res.Intended, res.Dropped
		in.ExpectedSnapshots, in.ReceivedSnapshots = res.ExpectedSnapshots, res.ReceivedSnapshots
		if res.Intended > 0 {
			lag := res.LagP99
			in.LagP99US = &lag
		}
	}

	err = s.pool.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM run_events WHERE run_id = $1 AND kind = $2) +
			(SELECT count(*) FROM snapshot_rejects WHERE run_id = $1)`,
		runID, EventProtocolViolation).Scan(&in.ProtocolViolations)
	if err != nil {
		return health.Inputs{}, health.Thresholds{}, fmt.Errorf("store: violations for %s: %w", runID, err)
	}
	return in, th, nil
}
