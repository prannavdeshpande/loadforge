package aggregator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

// Store is what the aggregator needs from Postgres; *store.Store implements
// it. Every write is idempotent or monotonic, so no lease is needed
// (design.md §4.4).
type Store interface {
	Stream(ctx context.Context, runID, workerID string) (store.StreamStatus, error)
	Streams(ctx context.Context, runID string) ([]store.StreamStatus, error)
	InsertSnapshot(ctx context.Context, r store.SnapshotRow) (bool, error)
	GetSnapshot(ctx context.Context, runID, workerID string, seq uint64) (store.SnapshotRow, error)
	InsertReject(ctx context.Context, r store.Reject) error
	RejectCount(ctx context.Context, runID string) (int64, error)
	DirtySeconds(ctx context.Context, limit int) ([]store.SecondKey, error)
	SnapshotsForSeq(ctx context.Context, runID string, seq uint64) ([]store.SnapshotRow, error)
	UpsertSecond(ctx context.Context, r store.SecondRow) (bool, error)
	RunProgress(ctx context.Context, runID string) (store.Progress, bool, error)
	FinalizeCandidates(ctx context.Context) ([]store.FinalizeCandidate, error)
	ForEachSnapshot(ctx context.Context, runID string, fn func(store.SnapshotRow) error) error
	PutResult(ctx context.Context, r store.ResultRow) (bool, error)
}

var _ Store = (*store.Store)(nil)

// Record is a Kafka message, independent of the client library.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Outcome is what ingesting a record did.
type Outcome int

const (
	Stored    Outcome = iota // a new snapshot was stored
	Duplicate                // a redelivery of a stored snapshot; nothing changed
	Rejected                 // recorded in snapshot_rejects
)

func (o Outcome) String() string {
	return [...]string{"stored", "duplicate", "rejected"}[o]
}

// Ingester validates and stores snapshots.
type Ingester struct {
	st Store
}

// NewIngester returns an Ingester writing to st.
func NewIngester(st Store) *Ingester { return &Ingester{st: st} }

// Handle ingests one record. A record that is malformed or contradicts what
// is already known is rejected, which feeds the run's protocol-violation
// signal. A non-nil error means Postgres could not be reached or refused a
// write: the record was not handled, its offset must not be committed, and
// the caller retries it rather than skipping it.
func (in *Ingester) Handle(ctx context.Context, rec Record) (Outcome, error) {
	keyRun, keyWorker := parseKey(rec.Key)
	reject := func(runID, workerID, format string, args ...any) (Outcome, error) {
		err := in.st.InsertReject(ctx, store.Reject{
			Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset, Key: rec.Key,
			RunID: runID, WorkerID: workerID, Error: fmt.Sprintf(format, args...),
		})
		if err != nil {
			return 0, err
		}
		return Rejected, nil
	}

	s, err := transport.DecodeSnapshot(rec.Value)
	if err != nil {
		return reject(keyRun, keyWorker, "decode: %v", err)
	}
	if want := string(transport.SnapshotKey(s.RunID, s.WorkerID)); string(rec.Key) != want {
		return reject(s.RunID, s.WorkerID, "key %q does not match payload %q", rec.Key, want)
	}

	st, err := in.st.Stream(ctx, s.RunID, s.WorkerID)
	if errors.Is(err, store.ErrNotFound) {
		return reject(s.RunID, s.WorkerID, "worker %s is not a member of run %s", s.WorkerID, s.RunID)
	}
	if err != nil {
		return 0, err
	}
	if why := contradicts(st, s); why != "" {
		return reject(s.RunID, s.WorkerID, "seq %d: %s", s.Seq, why)
	}

	row, err := snapshotRow(s)
	if err != nil {
		return reject(s.RunID, s.WorkerID, "re-encode: %v", err)
	}
	stored, err := in.st.InsertSnapshot(ctx, row)
	if err != nil {
		return 0, err
	}
	if stored {
		return Stored, nil
	}
	// Already stored. A redelivery is identical; a different snapshot under
	// the same seq means the worker reused one.
	prev, err := in.st.GetSnapshot(ctx, s.RunID, s.WorkerID, s.Seq)
	if err != nil {
		return 0, err
	}
	if !sameSnapshot(prev, row) {
		return reject(s.RunID, s.WorkerID, "seq %d reused with different content", s.Seq)
	}
	return Duplicate, nil
}

// contradicts checks s against the worker's stream so far. Redeliveries of
// what is stored never contradict it.
func contradicts(st store.StreamStatus, s *transport.Snapshot) string {
	if final, ok := st.FinalSeq(); ok {
		switch {
		case s.Seq > final:
			return fmt.Sprintf("after the stream's final seq %d", final)
		case s.Final && s.Seq != final:
			return fmt.Sprintf("marked final, but the final seq is %d", final)
		case !s.Final && s.Seq == final:
			return "is the final seq but not marked final"
		}
	}
	if s.Final && st.MaxStoredSeq != nil && *st.MaxStoredSeq > s.Seq {
		return fmt.Sprintf("marked final, but seq %d is already stored", *st.MaxStoredSeq)
	}
	return ""
}

// parseKey splits a well-formed run_id:worker_id key, for attributing a
// payload that could not be decoded. It returns empty strings otherwise.
func parseKey(key []byte) (runID, workerID string) {
	r, w, ok := strings.Cut(string(key), ":")
	if !ok || transport.ValidateID("run_id", r) != nil || transport.ValidateID("worker_id", w) != nil {
		return "", ""
	}
	return r, w
}
