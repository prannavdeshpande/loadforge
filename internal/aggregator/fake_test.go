package aggregator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
)

// fakeStore is an in-memory Store with the same semantics as Postgres:
// snapshots and rejects deduplicated on their keys, run_seconds replaced
// only by a merge of at least as many snapshots, results written once.
// The integration tests run the same paths against Postgres.
type fakeStore struct {
	mu        sync.Mutex
	runs      map[string]*fakeRun
	order     []string
	snaps     map[string]map[string]map[uint64]store.SnapshotRow // run → worker → seq
	rejects   map[string]store.Reject                            // topic/partition/offset
	seconds   map[string]map[uint64]store.SecondRow
	results   map[string]store.ResultRow
	failWrite error // if set, InsertSnapshot and InsertReject fail with it
	inserts   int   // successful snapshot inserts
	attempts  int   // InsertSnapshot calls, including duplicates and failures
}

type fakeRun struct {
	state             runstate.State
	finalizeRequested bool
	ackFinal          map[string]*uint64 // members, with STOP-ack final seqs
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		runs: map[string]*fakeRun{}, snaps: map[string]map[string]map[uint64]store.SnapshotRow{},
		rejects: map[string]store.Reject{}, seconds: map[string]map[uint64]store.SecondRow{},
		results: map[string]store.ResultRow{},
	}
}

func (f *fakeStore) addRun(id string, state runstate.State, workers ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &fakeRun{state: state, ackFinal: map[string]*uint64{}}
	for _, w := range workers {
		r.ackFinal[w] = nil
	}
	f.runs[id] = r
	f.order = append(f.order, id)
}

func (f *fakeStore) setState(id string, s runstate.State) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[id].state = s
}

func (f *fakeStore) stopAck(id, worker string, final uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[id].ackFinal[worker] = &final
}

func (f *fakeStore) requestFinalize(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[id].finalizeRequested = true
}

func (f *fakeStore) setFailWrite(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failWrite = err
}

func (f *fakeStore) streamLocked(runID, workerID string) store.StreamStatus {
	st := store.StreamStatus{WorkerID: workerID, AckFinalSeq: f.runs[runID].ackFinal[workerID]}
	for seq, r := range f.snaps[runID][workerID] {
		st.Stored++
		if st.MaxStoredSeq == nil || seq > *st.MaxStoredSeq {
			s := seq
			st.MaxStoredSeq = &s
		}
		if r.Final && (st.StoredFinalSeq == nil || seq < *st.StoredFinalSeq) {
			s := seq
			st.StoredFinalSeq = &s
		}
	}
	return st
}

func (f *fakeStore) Stream(_ context.Context, runID, workerID string) (store.StreamStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[runID]
	if !ok {
		return store.StreamStatus{}, fmt.Errorf("%w: run %s", store.ErrNotFound, runID)
	}
	if _, ok := r.ackFinal[workerID]; !ok {
		return store.StreamStatus{}, fmt.Errorf("%w: worker %s", store.ErrNotFound, workerID)
	}
	return f.streamLocked(runID, workerID), nil
}

func (f *fakeStore) Streams(_ context.Context, runID string) ([]store.StreamStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ws []string
	for w := range f.runs[runID].ackFinal {
		ws = append(ws, w)
	}
	sort.Strings(ws)
	var out []store.StreamStatus
	for _, w := range ws {
		out = append(out, f.streamLocked(runID, w))
	}
	return out, nil
}

func (f *fakeStore) InsertSnapshot(_ context.Context, r store.SnapshotRow) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.failWrite != nil {
		return false, f.failWrite
	}
	if f.snaps[r.RunID] == nil {
		f.snaps[r.RunID] = map[string]map[uint64]store.SnapshotRow{}
	}
	if f.snaps[r.RunID][r.WorkerID] == nil {
		f.snaps[r.RunID][r.WorkerID] = map[uint64]store.SnapshotRow{}
	}
	if _, ok := f.snaps[r.RunID][r.WorkerID][r.Seq]; ok {
		return false, nil
	}
	f.snaps[r.RunID][r.WorkerID][r.Seq] = r
	f.inserts++
	return true, nil
}

func (f *fakeStore) GetSnapshot(_ context.Context, runID, workerID string, seq uint64) (store.SnapshotRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.snaps[runID][workerID][seq]
	if !ok {
		return r, store.ErrNotFound
	}
	return r, nil
}

func (f *fakeStore) InsertReject(_ context.Context, r store.Reject) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWrite != nil {
		return f.failWrite
	}
	if r.Error == "" {
		return errors.New("reject without an error")
	}
	k := fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
	if _, ok := f.rejects[k]; !ok {
		f.rejects[k] = r
	}
	return nil
}

func (f *fakeStore) RejectCount(_ context.Context, runID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, r := range f.rejects {
		if r.RunID == runID {
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) rejectList() []store.Reject {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.Reject
	for _, r := range f.rejects {
		out = append(out, r)
	}
	return out
}

func (f *fakeStore) DirtySeconds(_ context.Context, limit int) ([]store.SecondKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.SecondKey
	for _, runID := range f.order {
		if _, done := f.results[runID]; done {
			continue
		}
		counts := map[uint64]int{}
		for _, bySeq := range f.snaps[runID] {
			for seq := range bySeq {
				counts[seq]++
			}
		}
		var seqs []uint64
		for seq, n := range counts {
			if n > f.seconds[runID][seq].SnapshotsMerged {
				seqs = append(seqs, seq)
			}
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for _, s := range seqs {
			if len(out) == limit {
				return out, nil
			}
			out = append(out, store.SecondKey{RunID: runID, Seq: s})
		}
	}
	return out, nil
}

func (f *fakeStore) SnapshotsForSeq(_ context.Context, runID string, seq uint64) ([]store.SnapshotRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.SnapshotRow
	for _, bySeq := range f.snaps[runID] {
		if r, ok := bySeq[seq]; ok {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out, nil
}

func (f *fakeStore) UpsertSecond(_ context.Context, r store.SecondRow) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seconds[r.RunID] == nil {
		f.seconds[r.RunID] = map[uint64]store.SecondRow{}
	}
	if cur, ok := f.seconds[r.RunID][r.Seq]; ok && cur.SnapshotsMerged > r.SnapshotsMerged {
		return false, nil
	}
	f.seconds[r.RunID][r.Seq] = r
	return true, nil
}

func (f *fakeStore) second(runID string, seq uint64) (store.SecondRow, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.seconds[runID][seq]
	return r, ok
}

func (f *fakeStore) RunProgress(_ context.Context, runID string) (store.Progress, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var p store.Progress
	for seq, r := range f.seconds[runID] {
		p.Seconds++
		p.SnapshotsMerged += int64(r.SnapshotsMerged)
		p.IntervalNSTotal += r.IntervalNSTotal
		p.Counts.Add(r.Counts)
		if p.Seconds == 1 || seq >= p.LatestSeq {
			p.LatestSeq, p.Latest = seq, r.Percentiles
		}
	}
	return p, p.Seconds > 0, nil
}

func (f *fakeStore) FinalizeCandidates(context.Context) ([]store.FinalizeCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.FinalizeCandidate
	for _, id := range f.order {
		r := f.runs[id]
		if _, done := f.results[id]; done {
			continue
		}
		switch r.state {
		case runstate.Stopping, runstate.Completed, runstate.Aborted:
			out = append(out, store.FinalizeCandidate{RunID: id, State: r.state, FinalizeRequested: r.finalizeRequested})
		}
	}
	return out, nil
}

func (f *fakeStore) ForEachSnapshot(_ context.Context, runID string, fn func(store.SnapshotRow) error) error {
	f.mu.Lock()
	var rows []store.SnapshotRow
	for _, bySeq := range f.snaps[runID] {
		for _, r := range bySeq {
			rows = append(rows, r)
		}
	}
	f.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Seq != rows[j].Seq {
			return rows[i].Seq < rows[j].Seq
		}
		return rows[i].WorkerID < rows[j].WorkerID
	})
	for _, r := range rows {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) PutResult(_ context.Context, r store.ResultRow) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.ReceivedSnapshots > r.ExpectedSnapshots {
		return false, fmt.Errorf("%d received of %d expected", r.ReceivedSnapshots, r.ExpectedSnapshots)
	}
	if _, ok := f.results[r.RunID]; ok {
		return false, nil
	}
	f.results[r.RunID] = r
	return true, nil
}

func (f *fakeStore) result(runID string) (store.ResultRow, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.results[runID]
	return r, ok
}

func (f *fakeStore) insertAttempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

func (f *fakeStore) storedCount(runID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, bySeq := range f.snaps[runID] {
		n += len(bySeq)
	}
	return n
}

var _ Store = (*fakeStore)(nil)
