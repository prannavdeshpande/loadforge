package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// These writes are made by the aggregator, which holds no run lease. They
// are safe without fencing because each is idempotent or monotonic:
// snapshots and rejects are deduplicated on their natural keys, run_seconds
// only moves to a merge of at least as many snapshots, and run_results is
// written once.

// Counts are the per-interval counters of a snapshot, summed when merged.
type Counts struct {
	Intended, OK, Err, Timeout, Dropped, Unfinished, Clamped int64
}

// Add adds o to c.
func (c *Counts) Add(o Counts) {
	c.Intended += o.Intended
	c.OK += o.OK
	c.Err += o.Err
	c.Timeout += o.Timeout
	c.Dropped += o.Dropped
	c.Unfinished += o.Unfinished
	c.Clamped += o.Clamped
}

// Hists are HDR V2 compressed histograms (the interchange encoding), kept
// opaque here: internal/aggregator decodes and merges them.
type Hists struct {
	Latency, Lag, SendDelay []byte
}

func (h Hists) complete() bool { return len(h.Latency) > 0 && len(h.Lag) > 0 && len(h.SendDelay) > 0 }

// Percentiles are read from a merged latency histogram, in microseconds.
// They are never averaged.
type Percentiles struct {
	P50, P90, P99, P999, Max int64
	LagP99                   int64 // from the merged dispatch-lag histogram
}

// SnapshotRow is one ingested worker snapshot.
type SnapshotRow struct {
	RunID, WorkerID string
	Seq             uint64
	// Wall-clock labels, unix ns, for display only.
	IntervalStartNS, IntervalEndNS int64
	// IntervalNS is the interval's length on the worker's monotonic clock.
	IntervalNS int64
	Counts
	Hists
	Final  bool
	Reason string // set iff Final
}

// InsertSnapshot stores a snapshot unless (run_id, worker_id, seq) is
// already stored. It reports whether this call stored it, so redelivery
// from Kafka is counted once.
func (s *Store) InsertSnapshot(ctx context.Context, r SnapshotRow) (bool, error) {
	if !r.Hists.complete() {
		return false, errors.New("store: snapshot without its three histograms")
	}
	if r.Final != (r.Reason != "") {
		return false, errors.New("store: a reason must be set on the final snapshot and only there")
	}
	var reason *string
	if r.Reason != "" {
		reason = &r.Reason
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO run_snapshots (run_id, worker_id, seq,
			interval_start_ns, interval_end_ns, interval_ns,
			intended, ok, err, timeout, dropped, unfinished, clamped,
			latency_hist, lag_hist, send_delay_hist, final, reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		ON CONFLICT (run_id, worker_id, seq) DO NOTHING`,
		r.RunID, r.WorkerID, r.Seq, r.IntervalStartNS, r.IntervalEndNS, r.IntervalNS,
		r.Intended, r.OK, r.Err, r.Timeout, r.Dropped, r.Unfinished, r.Clamped,
		r.Latency, r.Lag, r.SendDelay, r.Final, reason)
	if err != nil {
		return false, fmt.Errorf("store: insert snapshot %s/%s/%d: %w", r.RunID, r.WorkerID, r.Seq, err)
	}
	return tag.RowsAffected() == 1, nil
}

const snapshotColumns = `run_id, worker_id, seq, interval_start_ns, interval_end_ns, interval_ns,
	intended, ok, err, timeout, dropped, unfinished, clamped,
	latency_hist, lag_hist, send_delay_hist, final, coalesce(reason, '')`

// SnapshotsForSeq returns every worker's snapshot for one second of a run,
// ordered by worker ID.
func (s *Store) SnapshotsForSeq(ctx context.Context, runID string, seq uint64) ([]SnapshotRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+snapshotColumns+` FROM run_snapshots
		WHERE run_id = $1 AND seq = $2 ORDER BY worker_id`, runID, seq)
	if err != nil {
		return nil, fmt.Errorf("store: snapshots for %s/%d: %w", runID, seq, err)
	}
	defer rows.Close()
	var out []SnapshotRow
	for rows.Next() {
		var r SnapshotRow
		if err := rows.Scan(&r.RunID, &r.WorkerID, &r.Seq, &r.IntervalStartNS, &r.IntervalEndNS, &r.IntervalNS,
			&r.Intended, &r.OK, &r.Err, &r.Timeout, &r.Dropped, &r.Unfinished, &r.Clamped,
			&r.Latency, &r.Lag, &r.SendDelay, &r.Final, &r.Reason); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Reject is a Kafka message the aggregator could not accept.
type Reject struct {
	Topic           string
	Partition       int32
	Offset          int64
	Key             []byte
	RunID, WorkerID string // parsed from Key when well formed, else empty
	Error           string
}

// InsertReject records a rejected message once per (topic, partition,
// offset).
func (s *Store) InsertReject(ctx context.Context, r Reject) error {
	if r.Error == "" {
		return errors.New("store: reject without an error")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO snapshot_rejects (topic, partition, kafka_offset, kafka_key, run_id, worker_id, error)
		VALUES ($1, $2, $3, $4, nullif($5, ''), nullif($6, ''), $7)
		ON CONFLICT (topic, partition, kafka_offset) DO NOTHING`,
		r.Topic, r.Partition, r.Offset, r.Key, r.RunID, r.WorkerID, r.Error)
	if err != nil {
		return fmt.Errorf("store: insert reject %s/%d/%d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	return nil
}

// RejectCount counts the rejected messages attributed to a run.
func (s *Store) RejectCount(ctx context.Context, runID string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM snapshot_rejects WHERE run_id = $1`, runID).Scan(&n)
	return n, err
}

// SecondRow is one second of a run, merged across workers.
type SecondRow struct {
	RunID           string
	Seq             uint64
	SnapshotsMerged int
	IntervalNSTotal int64 // sum of the merged snapshots' monotonic lengths
	Counts
	Hists
	Percentiles
}

// UpsertSecond writes a merged second unless the stored row was merged
// from more snapshots. Snapshots never change and only accumulate, so the
// row with more inputs is the newer one, and a slower aggregator cannot
// overwrite a fresher merge. It reports whether the row was written.
func (s *Store) UpsertSecond(ctx context.Context, r SecondRow) (bool, error) {
	if r.SnapshotsMerged <= 0 || !r.Hists.complete() {
		return false, errors.New("store: a merged second needs its snapshots and histograms")
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO run_seconds (run_id, seq, snapshots_merged, interval_ns_total,
			intended, ok, err, timeout, dropped, unfinished, clamped,
			latency_hist, lag_hist, send_delay_hist, p50_us, p90_us, p99_us, p999_us, max_us, lag_p99_us)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
		ON CONFLICT (run_id, seq) DO UPDATE SET
			snapshots_merged = EXCLUDED.snapshots_merged, interval_ns_total = EXCLUDED.interval_ns_total,
			intended = EXCLUDED.intended, ok = EXCLUDED.ok, err = EXCLUDED.err, timeout = EXCLUDED.timeout,
			dropped = EXCLUDED.dropped, unfinished = EXCLUDED.unfinished, clamped = EXCLUDED.clamped,
			latency_hist = EXCLUDED.latency_hist, lag_hist = EXCLUDED.lag_hist, send_delay_hist = EXCLUDED.send_delay_hist,
			p50_us = EXCLUDED.p50_us, p90_us = EXCLUDED.p90_us, p99_us = EXCLUDED.p99_us,
			p999_us = EXCLUDED.p999_us, max_us = EXCLUDED.max_us, lag_p99_us = EXCLUDED.lag_p99_us,
			updated_at = now()
		WHERE run_seconds.snapshots_merged <= EXCLUDED.snapshots_merged`,
		r.RunID, r.Seq, r.SnapshotsMerged, r.IntervalNSTotal,
		r.Intended, r.OK, r.Err, r.Timeout, r.Dropped, r.Unfinished, r.Clamped,
		r.Latency, r.Lag, r.SendDelay, r.P50, r.P90, r.P99, r.P999, r.Max, r.LagP99)
	if err != nil {
		return false, fmt.Errorf("store: upsert second %s/%d: %w", r.RunID, r.Seq, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Seconds lists a run's merged seconds in order.
func (s *Store) Seconds(ctx context.Context, runID string) ([]SecondRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT run_id, seq, snapshots_merged, interval_ns_total,
			intended, ok, err, timeout, dropped, unfinished, clamped,
			latency_hist, lag_hist, send_delay_hist, p50_us, p90_us, p99_us, p999_us, max_us, lag_p99_us
		FROM run_seconds WHERE run_id = $1 ORDER BY seq`, runID)
	if err != nil {
		return nil, fmt.Errorf("store: seconds for %s: %w", runID, err)
	}
	defer rows.Close()
	var out []SecondRow
	for rows.Next() {
		var r SecondRow
		if err := rows.Scan(&r.RunID, &r.Seq, &r.SnapshotsMerged, &r.IntervalNSTotal,
			&r.Intended, &r.OK, &r.Err, &r.Timeout, &r.Dropped, &r.Unfinished, &r.Clamped,
			&r.Latency, &r.Lag, &r.SendDelay, &r.P50, &r.P90, &r.P99, &r.P999, &r.Max, &r.LagP99); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ResultRow is a run's final merged result.
type ResultRow struct {
	RunID             string
	ExpectedSnapshots int64
	ReceivedSnapshots int64
	RejectedSnapshots int64
	IntervalNSTotal   int64
	Counts
	Hists
	Percentiles
}

// PutResult writes a run's final result. A result is written once; it
// reports false if one already exists, which is left unchanged.
func (s *Store) PutResult(ctx context.Context, r ResultRow) (bool, error) {
	if !r.Hists.complete() {
		return false, errors.New("store: a result needs its histograms")
	}
	if r.ReceivedSnapshots > r.ExpectedSnapshots {
		return false, fmt.Errorf("store: %d snapshots received of %d expected", r.ReceivedSnapshots, r.ExpectedSnapshots)
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO run_results (run_id, expected_snapshots, received_snapshots, rejected_snapshots,
			interval_ns_total, intended, ok, err, timeout, dropped, unfinished, clamped,
			latency_hist, lag_hist, send_delay_hist, p50_us, p90_us, p99_us, p999_us, max_us, lag_p99_us)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT (run_id) DO NOTHING`,
		r.RunID, r.ExpectedSnapshots, r.ReceivedSnapshots, r.RejectedSnapshots,
		r.IntervalNSTotal, r.Intended, r.OK, r.Err, r.Timeout, r.Dropped, r.Unfinished, r.Clamped,
		r.Latency, r.Lag, r.SendDelay, r.P50, r.P90, r.P99, r.P999, r.Max, r.LagP99)
	if err != nil {
		return false, fmt.Errorf("store: put result %s: %w", r.RunID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// GetResult reads a run's final result.
func (s *Store) GetResult(ctx context.Context, runID string) (ResultRow, error) {
	var r ResultRow
	err := s.pool.QueryRow(ctx, `SELECT run_id, expected_snapshots, received_snapshots, rejected_snapshots,
			interval_ns_total, intended, ok, err, timeout, dropped, unfinished, clamped,
			latency_hist, lag_hist, send_delay_hist, p50_us, p90_us, p99_us, p999_us, max_us, lag_p99_us
		FROM run_results WHERE run_id = $1`, runID).Scan(&r.RunID, &r.ExpectedSnapshots, &r.ReceivedSnapshots, &r.RejectedSnapshots,
		&r.IntervalNSTotal, &r.Intended, &r.OK, &r.Err, &r.Timeout, &r.Dropped, &r.Unfinished, &r.Clamped,
		&r.Latency, &r.Lag, &r.SendDelay, &r.P50, &r.P90, &r.P99, &r.P999, &r.Max, &r.LagP99)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResultRow{}, fmt.Errorf("%w: result for run %s", ErrNotFound, runID)
	}
	return r, err
}
