package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/transport"
)

// Event kinds in run_events.
const (
	EventCreated           = "created"
	EventTransition        = "transition"
	EventPrepareAck        = "prepare_ack"
	EventStopAck           = "stop_ack"
	EventAck               = "ack"
	EventNack              = "nack"
	EventWorkerLost        = "worker_lost"
	EventFinalizeRequested = "finalize_requested"
	EventProtocolViolation = "protocol_violation" // feeds the INVALID verdict signal
)

// NewRun is a run to create. Workers fixes the run's membership.
type NewRun struct {
	ID         string
	Config     json.RawMessage
	Thresholds json.RawMessage
	Workers    []string
	Fence      uint64 // the creating controller's lease token
}

// Run is a row of runs.
type Run struct {
	ID                string
	Config            json.RawMessage
	Thresholds        json.RawMessage
	State             runstate.State
	FenceToken        uint64
	Verdict           health.Verdict // empty until terminal
	VerdictDetail     json.RawMessage
	FinalizeRequested bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// RunWorker is a row of run_workers.
type RunWorker struct {
	WorkerID      string
	PrepareStatus transport.AckStatus // empty until the PREPARE ack
	PrepareDetail string
	ClockOffsetNS *int64   // nil = unmeasured
	CapacityRPS   *float64 // nil = unmeasured
	FinalSeq      *uint64  // nil until the STOP ack reports it
	StopDetail    string
	Lost          bool
}

// Event is a run_events row to append.
type Event struct {
	Kind     string
	WorkerID string
	CmdSeq   uint64 // 0 = none
	Detail   json.RawMessage
}

// CreateRun inserts a PENDING run, its members and a "created" event.
func (s *Store) CreateRun(ctx context.Context, r NewRun) error {
	if err := transport.ValidateID("run_id", r.ID); err != nil {
		return err
	}
	if r.Fence == 0 {
		return errors.New("store: CreateRun needs a fencing token")
	}
	if len(r.Workers) == 0 {
		return errors.New("store: a run needs at least one worker")
	}
	seen := make(map[string]bool, len(r.Workers))
	for _, w := range r.Workers {
		if err := transport.ValidateID("worker_id", w); err != nil {
			return err
		}
		if seen[w] {
			return fmt.Errorf("store: worker %q listed twice", w)
		}
		seen[w] = true
	}
	if !json.Valid(r.Config) || !json.Valid(r.Thresholds) {
		return errors.New("store: config and thresholds must be valid JSON")
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO runs (id, config, thresholds, state, fence_token) VALUES ($1, $2, $3, $4, $5)`,
			r.ID, []byte(r.Config), []byte(r.Thresholds), string(runstate.Pending), r.Fence)
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: run %s", ErrExists, r.ID)
		}
		if err != nil {
			return fmt.Errorf("store: create run %s: %w", r.ID, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO run_workers (run_id, worker_id) SELECT $1, unnest($2::text[])`, r.ID, r.Workers); err != nil {
			return fmt.Errorf("store: create run %s workers: %w", r.ID, err)
		}
		return appendEvent(ctx, tx, r.ID, r.Fence, Event{Kind: EventCreated}, "", string(runstate.Pending))
	})
}

const runColumns = `id, config, thresholds, state, fence_token, coalesce(verdict, ''), verdict_detail, finalize_requested, created_at, updated_at`

func scanRun(row pgx.Row) (Run, error) {
	var (
		r       Run
		state   string
		verdict string
	)
	err := row.Scan(&r.ID, &r.Config, &r.Thresholds, &state, &r.FenceToken, &verdict, &r.VerdictDetail,
		&r.FinalizeRequested, &r.CreatedAt, &r.UpdatedAt)
	r.State, r.Verdict = runstate.State(state), health.Verdict(verdict)
	return r, err
}

// GetRun reads a run.
func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, fmt.Errorf("%w: run %s", ErrNotFound, id)
	}
	return r, err
}

// ActiveRuns lists the runs not yet COMPLETED or ABORTED, oldest first. A
// controller starting up uses it to find runs whose controller is gone.
func (s *Store) ActiveRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+runColumns+` FROM runs
		WHERE state NOT IN ('COMPLETED', 'ABORTED') ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("store: active runs: %w", err)
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Transition moves a run from one state to another under the caller's
// fencing token. A terminal state needs the run's verdict, and ABORTED is
// always INVALID; no other state may carry one.
type Transition struct {
	RunID         string
	Fence         uint64
	From, To      runstate.State
	Verdict       health.Verdict  // required for COMPLETED and ABORTED
	VerdictDetail json.RawMessage // which signals degraded, thresholds used
	Detail        json.RawMessage // why, for run_events
}

// Transition applies t, appending the run_events row in the same
// transaction. It fails with ErrStaleFence if the token is old, and with
// ErrStateConflict if the run is no longer in t.From.
func (s *Store) Transition(ctx context.Context, t Transition) error {
	if err := runstate.Check(t.From, t.To); err != nil {
		return err
	}
	switch {
	case t.To.Terminal() && !t.Verdict.Known():
		return fmt.Errorf("store: %s needs a verdict, got %q", t.To, t.Verdict)
	case !t.To.Terminal() && t.Verdict != "":
		return fmt.Errorf("store: %s cannot carry a verdict", t.To)
	case t.To == runstate.Aborted && t.Verdict != health.Invalid:
		return fmt.Errorf("store: an aborted run is INVALID, not %s", t.Verdict)
	}
	return s.guarded(ctx, t.RunID, t.Fence, func(tx pgx.Tx, state string) error {
		if runstate.State(state) != t.From {
			return fmt.Errorf("%w: run %s is %s, not %s", ErrStateConflict, t.RunID, state, t.From)
		}
		var verdict *string
		if t.Verdict != "" {
			v := string(t.Verdict)
			verdict = &v
		}
		_, err := tx.Exec(ctx, `UPDATE runs SET state = $2, verdict = $3, verdict_detail = $4, updated_at = now()
			WHERE id = $1 AND fence_token <= $5`,
			t.RunID, string(t.To), verdict, nullJSON(t.VerdictDetail), t.Fence)
		if err != nil {
			return fmt.Errorf("store: transition run %s: %w", t.RunID, err)
		}
		return appendEvent(ctx, tx, t.RunID, t.Fence, Event{Kind: EventTransition, Detail: t.Detail}, string(t.From), string(t.To))
	})
}

// AppendEvent appends an event under the caller's fencing token.
func (s *Store) AppendEvent(ctx context.Context, runID string, fenceToken uint64, e Event) error {
	return s.guarded(ctx, runID, fenceToken, func(tx pgx.Tx, _ string) error {
		return appendEvent(ctx, tx, runID, fenceToken, e, "", "")
	})
}

// RecordPrepareAck stores a worker's PREPARE reply and logs it.
func (s *Store) RecordPrepareAck(ctx context.Context, runID string, fenceToken uint64, a transport.Ack) error {
	if a.Status != transport.StatusReady && a.Status != transport.StatusNack {
		return fmt.Errorf("store: PREPARE ack status %q", a.Status)
	}
	return s.guarded(ctx, runID, fenceToken, func(tx pgx.Tx, _ string) error {
		if err := updateWorker(ctx, tx, runID, a.WorkerID,
			`prepare_status = $3, prepare_detail = $4, clock_offset_ns = $5, capacity_rps = $6`,
			string(a.Status), a.Detail, a.ClockOffsetNS, a.CapacityRPS); err != nil {
			return err
		}
		return appendEvent(ctx, tx, runID, fenceToken, ackEvent(EventPrepareAck, a), "", "")
	})
}

// RecordStopAck stores a worker's STOP reply. A stopped ack carries the
// final_seq the aggregator waits for. A worker reports one final_seq per
// run: a different one is kept out of run_workers, logged as a
// protocol_violation event and reported as ErrProtocolViolation.
func (s *Store) RecordStopAck(ctx context.Context, runID string, fenceToken uint64, a transport.Ack) error {
	if a.Status == transport.StatusStopped && a.FinalSeq == nil {
		return errors.New("store: stopped ack without final_seq")
	}
	var violation error
	err := s.guarded(ctx, runID, fenceToken, func(tx pgx.Tx, _ string) error {
		if a.FinalSeq != nil {
			var prev *uint64
			if err := tx.QueryRow(ctx, `SELECT final_seq FROM run_workers WHERE run_id = $1 AND worker_id = $2`,
				runID, a.WorkerID).Scan(&prev); err != nil {
				return workerErr(err, runID, a.WorkerID)
			}
			if prev != nil && *prev != *a.FinalSeq {
				violation = fmt.Errorf("%w: run %s worker %s reported final_seq %d after %d",
					ErrProtocolViolation, runID, a.WorkerID, *a.FinalSeq, *prev)
				e := ackEvent(EventProtocolViolation, a)
				e.Kind = EventProtocolViolation
				return appendEvent(ctx, tx, runID, fenceToken, e, "", "")
			}
		}
		if err := updateWorker(ctx, tx, runID, a.WorkerID,
			`final_seq = coalesce($3, final_seq), stop_detail = $4`, a.FinalSeq, a.Detail); err != nil {
			return err
		}
		return appendEvent(ctx, tx, runID, fenceToken, ackEvent(EventStopAck, a), "", "")
	})
	if err != nil {
		return err
	}
	return violation
}

// MarkWorkerLost records that a worker went silent mid-run.
func (s *Store) MarkWorkerLost(ctx context.Context, runID string, fenceToken uint64, workerID string, detail json.RawMessage) error {
	return s.guarded(ctx, runID, fenceToken, func(tx pgx.Tx, _ string) error {
		if err := updateWorker(ctx, tx, runID, workerID, `lost = true`); err != nil {
			return err
		}
		return appendEvent(ctx, tx, runID, fenceToken, Event{Kind: EventWorkerLost, WorkerID: workerID, Detail: detail}, "", "")
	})
}

// RequestFinalize tells the aggregator to finalize the run with whatever
// snapshots it has, counting the rest as missing.
func (s *Store) RequestFinalize(ctx context.Context, runID string, fenceToken uint64) error {
	return s.guarded(ctx, runID, fenceToken, func(tx pgx.Tx, _ string) error {
		if _, err := tx.Exec(ctx, `UPDATE runs SET finalize_requested = true, updated_at = now() WHERE id = $1`, runID); err != nil {
			return err
		}
		return appendEvent(ctx, tx, runID, fenceToken, Event{Kind: EventFinalizeRequested}, "", "")
	})
}

// RunWorkers lists a run's members, ordered by worker ID.
func (s *Store) RunWorkers(ctx context.Context, runID string) ([]RunWorker, error) {
	rows, err := s.pool.Query(ctx, `SELECT worker_id, coalesce(prepare_status, ''), coalesce(prepare_detail, ''),
			clock_offset_ns, capacity_rps, final_seq, coalesce(stop_detail, ''), lost
		FROM run_workers WHERE run_id = $1 ORDER BY worker_id`, runID)
	if err != nil {
		return nil, fmt.Errorf("store: run workers: %w", err)
	}
	defer rows.Close()
	var out []RunWorker
	for rows.Next() {
		var (
			w      RunWorker
			status string
		)
		if err := rows.Scan(&w.WorkerID, &status, &w.PrepareDetail, &w.ClockOffsetNS, &w.CapacityRPS,
			&w.FinalSeq, &w.StopDetail, &w.Lost); err != nil {
			return nil, err
		}
		w.PrepareStatus = transport.AckStatus(status)
		out = append(out, w)
	}
	return out, rows.Err()
}

// StoredEvent is a run_events row as read back.
type StoredEvent struct {
	Event
	ID         int64
	At         time.Time
	FenceToken uint64
	FromState  runstate.State
	ToState    runstate.State
}

// Events lists a run's events in the order they were appended.
func (s *Store) Events(ctx context.Context, runID string) ([]StoredEvent, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, at, fence_token, kind, coalesce(from_state, ''), coalesce(to_state, ''),
			coalesce(worker_id, ''), coalesce(cmd_seq, 0), detail
		FROM run_events WHERE run_id = $1 ORDER BY id`, runID)
	if err != nil {
		return nil, fmt.Errorf("store: events: %w", err)
	}
	defer rows.Close()
	var out []StoredEvent
	for rows.Next() {
		var (
			e        StoredEvent
			from, to string
		)
		if err := rows.Scan(&e.ID, &e.At, &e.FenceToken, &e.Kind, &from, &to, &e.WorkerID, &e.CmdSeq, &e.Detail); err != nil {
			return nil, err
		}
		e.FromState, e.ToState = runstate.State(from), runstate.State(to)
		out = append(out, e)
	}
	return out, rows.Err()
}

func appendEvent(ctx context.Context, tx pgx.Tx, runID string, fenceToken uint64, e Event, from, to string) error {
	if e.Kind == "" {
		return errors.New("store: event without a kind")
	}
	if e.Detail != nil && !json.Valid(e.Detail) {
		return errors.New("store: event detail is not valid JSON")
	}
	var cmdSeq *uint64
	if e.CmdSeq != 0 {
		cmdSeq = &e.CmdSeq
	}
	_, err := tx.Exec(ctx, `INSERT INTO run_events (run_id, fence_token, kind, from_state, to_state, worker_id, cmd_seq, detail)
		VALUES ($1, $2, $3, nullif($4, ''), nullif($5, ''), nullif($6, ''), $7, coalesce($8::jsonb, '{}'))`,
		runID, fenceToken, e.Kind, from, to, e.WorkerID, cmdSeq, nullJSON(e.Detail))
	if err != nil {
		return fmt.Errorf("store: append %s event to run %s: %w", e.Kind, runID, err)
	}
	return nil
}

func ackEvent(kind string, a transport.Ack) Event {
	if a.Status == transport.StatusNack {
		kind = EventNack
	}
	detail, _ := json.Marshal(a) // an Ack always marshals
	return Event{Kind: kind, WorkerID: a.WorkerID, CmdSeq: a.CmdSeq, Detail: detail}
}

// updateWorker sets columns on one member row; set's placeholders start at
// $3. A worker that is not a member is an error, never an insert.
func updateWorker(ctx context.Context, tx pgx.Tx, runID, workerID, set string, args ...any) error {
	tag, err := tx.Exec(ctx, `UPDATE run_workers SET `+set+` WHERE run_id = $1 AND worker_id = $2`,
		append([]any{runID, workerID}, args...)...)
	if err != nil {
		return fmt.Errorf("store: update run %s worker %s: %w", runID, workerID, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: worker %s is not in run %s", ErrNotFound, workerID, runID)
	}
	return nil
}

func workerErr(err error, runID, workerID string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: worker %s is not in run %s", ErrNotFound, workerID, runID)
	}
	return err
}

// nullJSON maps an empty message to SQL NULL.
func nullJSON(m json.RawMessage) any {
	if len(m) == 0 {
		return nil
	}
	return []byte(m)
}
