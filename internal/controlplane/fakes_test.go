package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

// fakeStore is an in-memory Store with the same fencing, state and verdict
// rules as internal/store (design.md §3.3): a guarded write locks the run,
// fails with ErrStaleFence for a lower token and raises the run's token
// otherwise, and a failed write changes nothing.
type fakeStore struct {
	mu       sync.Mutex
	fenceSeq uint64
	runs     map[string]*fakeRun
	results  map[string]store.ResultRow
	progress map[string]store.Progress
	reasons  map[string]map[string]transport.StopReason
	// reasonOverride replaces a worker's stored final reason (tests).
	reasonOverride map[string]map[string]transport.StopReason
	writes         []writeRecord
}

type fakeRun struct {
	run     store.Run
	workers []store.RunWorker
	events  []store.StoredEvent
	stopAt  time.Time
}

// writeRecord is one guarded write attempt and whether it was accepted.
type writeRecord struct {
	runID string
	token uint64
	op    string
	ok    bool
	at    time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{runs: map[string]*fakeRun{}, results: map[string]store.ResultRow{}, progress: map[string]store.Progress{},
		reasons: map[string]map[string]transport.StopReason{}, reasonOverride: map[string]map[string]transport.StopReason{}}
}

var _ Store = (*fakeStore)(nil)

func (s *fakeStore) NextFenceToken(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fenceSeq++
	return s.fenceSeq, nil
}

func (s *fakeStore) CreateRun(_ context.Context, r store.NewRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Fence == 0 || len(r.Workers) == 0 {
		return errors.New("fake: bad run")
	}
	if _, ok := s.runs[r.ID]; ok {
		return store.ErrExists
	}
	now := time.Now()
	fr := &fakeRun{run: store.Run{ID: r.ID, Config: r.Config, Thresholds: r.Thresholds, State: runstate.Pending,
		FenceToken: r.Fence, CreatedAt: now, UpdatedAt: now}}
	for _, w := range r.Workers {
		fr.workers = append(fr.workers, store.RunWorker{WorkerID: w})
	}
	s.runs[r.ID] = fr
	s.appendLocked(fr, r.Fence, store.Event{Kind: store.EventCreated}, "", runstate.Pending)
	return nil
}

func (s *fakeStore) GetRun(_ context.Context, id string) (store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fr, ok := s.runs[id]
	if !ok {
		return store.Run{}, fmt.Errorf("%w: run %s", store.ErrNotFound, id)
	}
	return fr.run, nil
}

func (s *fakeStore) ActiveRuns(context.Context) ([]store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.Run
	for _, fr := range s.runs {
		if !fr.run.State.Terminal() {
			out = append(out, fr.run)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// guarded runs fn under the fence rules. fn's error rolls back the token.
func (s *fakeStore) guarded(op, runID string, token uint64, fn func(fr *fakeRun) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := writeRecord{runID: runID, token: token, op: op, at: time.Now()}
	defer func() { s.writes = append(s.writes, rec) }()
	fr, ok := s.runs[runID]
	if !ok {
		return fmt.Errorf("%w: run %s", store.ErrNotFound, runID)
	}
	if token == 0 || token < fr.run.FenceToken {
		return fmt.Errorf("%w: run %s token %d < %d", store.ErrStaleFence, runID, token, fr.run.FenceToken)
	}
	prev := fr.run.FenceToken
	fr.run.FenceToken = token
	if err := fn(fr); err != nil {
		fr.run.FenceToken = prev
		return err
	}
	rec.ok = true
	return nil
}

func (s *fakeStore) appendLocked(fr *fakeRun, token uint64, e store.Event, from, to runstate.State) {
	if e.Detail == nil {
		e.Detail = json.RawMessage(`{}`)
	}
	fr.events = append(fr.events, store.StoredEvent{Event: e, ID: int64(len(fr.events) + 1), At: time.Now(),
		FenceToken: token, FromState: from, ToState: to})
}

func (s *fakeStore) Transition(_ context.Context, t store.Transition) error {
	if err := runstate.Check(t.From, t.To); err != nil {
		return err
	}
	switch {
	case t.To.Terminal() && !t.Verdict.Known():
		return fmt.Errorf("fake: %s needs a verdict", t.To)
	case !t.To.Terminal() && t.Verdict != "":
		return fmt.Errorf("fake: %s cannot carry a verdict", t.To)
	case t.To == runstate.Aborted && t.Verdict != health.Invalid:
		return errors.New("fake: aborted must be INVALID")
	}
	return s.guarded("transition "+string(t.To), t.RunID, t.Fence, func(fr *fakeRun) error {
		if fr.run.State != t.From {
			return fmt.Errorf("%w: run %s is %s, not %s", store.ErrStateConflict, t.RunID, fr.run.State, t.From)
		}
		fr.run.State, fr.run.Verdict, fr.run.VerdictDetail, fr.run.UpdatedAt = t.To, t.Verdict, t.VerdictDetail, time.Now()
		s.appendLocked(fr, t.Fence, store.Event{Kind: store.EventTransition, Detail: t.Detail}, t.From, t.To)
		return nil
	})
}

func (s *fakeStore) AppendEvent(_ context.Context, runID string, token uint64, e store.Event) error {
	return s.guarded("event "+e.Kind, runID, token, func(fr *fakeRun) error {
		s.appendLocked(fr, token, e, "", "")
		return nil
	})
}

func (fr *fakeRun) worker(id string) (*store.RunWorker, error) {
	for i := range fr.workers {
		if fr.workers[i].WorkerID == id {
			return &fr.workers[i], nil
		}
	}
	return nil, fmt.Errorf("%w: worker %s", store.ErrNotFound, id)
}

func ackDetail(a transport.Ack) json.RawMessage {
	return mustJSON(map[string]any{"status": a.Status, "detail": a.Detail, "final_seq": a.FinalSeq})
}

func (s *fakeStore) RecordPrepareAck(_ context.Context, runID string, token uint64, a transport.Ack) error {
	if a.Status != transport.StatusReady && a.Status != transport.StatusNack {
		return fmt.Errorf("fake: PREPARE ack status %q", a.Status)
	}
	return s.guarded("prepare_ack", runID, token, func(fr *fakeRun) error {
		w, err := fr.worker(a.WorkerID)
		if err != nil {
			return err
		}
		w.PrepareStatus, w.PrepareDetail, w.ClockOffsetNS, w.CapacityRPS = a.Status, a.Detail, a.ClockOffsetNS, a.CapacityRPS
		s.appendLocked(fr, token, store.Event{Kind: store.EventPrepareAck, WorkerID: a.WorkerID, CmdSeq: a.CmdSeq, Detail: ackDetail(a)}, "", "")
		return nil
	})
}

func (s *fakeStore) RecordStopAck(_ context.Context, runID string, token uint64, a transport.Ack) error {
	if a.Status == transport.StatusStopped && a.FinalSeq == nil {
		return errors.New("fake: stopped ack without final_seq")
	}
	var violation error
	err := s.guarded("stop_ack", runID, token, func(fr *fakeRun) error {
		w, err := fr.worker(a.WorkerID)
		if err != nil {
			return err
		}
		if a.FinalSeq != nil && w.FinalSeq != nil && *w.FinalSeq != *a.FinalSeq {
			violation = fmt.Errorf("%w: final_seq %d after %d", store.ErrProtocolViolation, *a.FinalSeq, *w.FinalSeq)
			s.appendLocked(fr, token, store.Event{Kind: store.EventProtocolViolation, WorkerID: a.WorkerID, CmdSeq: a.CmdSeq, Detail: ackDetail(a)}, "", "")
			return nil
		}
		if a.FinalSeq != nil {
			v := *a.FinalSeq
			w.FinalSeq = &v
		}
		w.StopDetail = a.Detail
		s.appendLocked(fr, token, store.Event{Kind: store.EventStopAck, WorkerID: a.WorkerID, CmdSeq: a.CmdSeq, Detail: ackDetail(a)}, "", "")
		return nil
	})
	if err != nil {
		return err
	}
	return violation
}

func (s *fakeStore) MarkWorkerLost(_ context.Context, runID string, token uint64, workerID string, detail json.RawMessage) error {
	return s.guarded("worker_lost", runID, token, func(fr *fakeRun) error {
		w, err := fr.worker(workerID)
		if err != nil {
			return err
		}
		w.Lost = true
		s.appendLocked(fr, token, store.Event{Kind: store.EventWorkerLost, WorkerID: workerID, Detail: detail}, "", "")
		return nil
	})
}

func (s *fakeStore) RequestFinalize(_ context.Context, runID string, token uint64) error {
	return s.guarded("finalize", runID, token, func(fr *fakeRun) error {
		fr.run.FinalizeRequested = true
		s.appendLocked(fr, token, store.Event{Kind: store.EventFinalizeRequested}, "", "")
		return nil
	})
}

func (s *fakeStore) RequestStop(_ context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fr, ok := s.runs[runID]
	if !ok {
		return fmt.Errorf("%w: run %s", store.ErrNotFound, runID)
	}
	if fr.run.State.Terminal() {
		return fmt.Errorf("%w: run %s is %s", store.ErrStateConflict, runID, fr.run.State)
	}
	if !fr.run.StopRequested {
		fr.run.StopRequested, fr.stopAt = true, time.Now()
	}
	return nil
}

func (s *fakeStore) MaxCmdSeq(_ context.Context, runID string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n uint64
	if fr, ok := s.runs[runID]; ok {
		for _, e := range fr.events {
			n = max(n, e.CmdSeq)
		}
	}
	return n, nil
}

func (s *fakeStore) FinalReasons(_ context.Context, runID string) (map[string]transport.StopReason, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]transport.StopReason{}
	for w, r := range s.reasons[runID] {
		out[w] = r
	}
	for w, r := range s.reasonOverride[runID] {
		out[w] = r
	}
	return out, nil
}

func (s *fakeStore) RunWorkers(_ context.Context, runID string) ([]store.RunWorker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fr, ok := s.runs[runID]
	if !ok {
		return nil, nil
	}
	return append([]store.RunWorker(nil), fr.workers...), nil
}

func (s *fakeStore) Events(_ context.Context, runID string) ([]store.StoredEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fr, ok := s.runs[runID]
	if !ok {
		return nil, nil
	}
	return append([]store.StoredEvent(nil), fr.events...), nil
}

func (s *fakeStore) GetResult(_ context.Context, runID string) (store.ResultRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.results[runID]
	if !ok {
		return store.ResultRow{}, fmt.Errorf("%w: result for run %s", store.ErrNotFound, runID)
	}
	return r, nil
}

func (s *fakeStore) RunProgress(_ context.Context, runID string) (store.Progress, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.progress[runID]
	return p, ok, nil
}

// VerdictInputs mirrors store.VerdictInputs.
func (s *fakeStore) VerdictInputs(_ context.Context, runID string) (health.Inputs, health.Thresholds, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fr, ok := s.runs[runID]
	if !ok {
		return health.Inputs{}, health.Thresholds{}, store.ErrNotFound
	}
	th, err := health.ParseThresholds(fr.run.Thresholds)
	if err != nil {
		return health.Inputs{}, health.Thresholds{}, err
	}
	var cfg struct {
		Rate float64 `json:"rate"`
	}
	if err := json.Unmarshal(fr.run.Config, &cfg); err != nil {
		return health.Inputs{}, health.Thresholds{}, err
	}
	in := health.Inputs{Aborted: fr.run.State == runstate.Aborted, Workers: len(fr.workers), TargetRatePerWorker: cfg.Rate}
	for _, w := range fr.workers {
		in.ClockOffsetsNS = append(in.ClockOffsetsNS, w.ClockOffsetNS)
		if w.Lost {
			in.LostWorkers = append(in.LostWorkers, w.WorkerID)
		}
	}
	if res, ok := s.results[runID]; ok {
		in.HasResult = true
		in.Intended, in.Dropped = res.Intended, res.Dropped
		in.ExpectedSnapshots, in.ReceivedSnapshots = res.ExpectedSnapshots, res.ReceivedSnapshots
		if res.Intended > 0 {
			lag := res.LagP99
			in.LagP99US = &lag
		}
	}
	for _, e := range fr.events {
		if e.Kind == store.EventProtocolViolation {
			in.ProtocolViolations++
		}
	}
	return in, th, nil
}

// --- test helpers on the fake ---

// seedRun puts a run directly in state with the given token, as if a
// controller (now gone) had created and moved it.
func (s *fakeStore) seedRun(id string, state runstate.State, token uint64, prep transport.Prepare) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fenceSeq = max(s.fenceSeq, token)
	now := time.Now()
	fr := &fakeRun{run: store.Run{ID: id, Config: mustJSON(prep), Thresholds: json.RawMessage(`{}`), State: state,
		FenceToken: token, CreatedAt: now, UpdatedAt: now}}
	for _, w := range prep.Workers {
		fr.workers = append(fr.workers, store.RunWorker{WorkerID: w})
	}
	s.runs[id] = fr
}

// bumpFence plays another controller's write with a fresh, higher token.
func (s *fakeStore) bumpFence(id string) uint64 {
	tok, _ := s.NextFenceToken(context.Background())
	_ = s.AppendEvent(context.Background(), id, tok, store.Event{Kind: "test_other_controller"})
	return tok
}

func (s *fakeStore) writesSince(runID string, t time.Time) []writeRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []writeRecord
	for _, w := range s.writes {
		if w.runID == runID && !w.at.Before(t) {
			out = append(out, w)
		}
	}
	return out
}

// seqCount is the number of snapshots 0..seq (test seqs are small).
func seqCount(seq uint64) int64 {
	if seq >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(seq) + 1
}

// fakeLeaser is an in-memory lease table with TTLs on the monotonic clock.
type fakeLeaser struct {
	mu        sync.Mutex
	ttl       time.Duration
	leases    map[string]fakeLease
	failRenew bool // Renew returns an error, as if Redis were unreachable
}

type fakeLease struct {
	token   uint64
	expires time.Time
}

func newFakeLeaser(ttl time.Duration) *fakeLeaser {
	return &fakeLeaser{ttl: ttl, leases: map[string]fakeLease{}}
}

var _ Leaser = (*fakeLeaser)(nil)

func (l *fakeLeaser) live(key string) (fakeLease, bool) {
	le, ok := l.leases[key]
	if ok && time.Now().After(le.expires) {
		delete(l.leases, key)
		return fakeLease{}, false
	}
	return le, ok
}

func (l *fakeLeaser) Acquire(_ context.Context, key string, token uint64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.live(key); ok {
		return false, nil
	}
	l.leases[key] = fakeLease{token: token, expires: time.Now().Add(l.ttl)}
	return true, nil
}

func (l *fakeLeaser) Renew(_ context.Context, key string, token uint64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failRenew {
		return false, errors.New("fake: redis unreachable")
	}
	le, ok := l.live(key)
	if !ok || le.token != token {
		return false, nil
	}
	l.leases[key] = fakeLease{token: token, expires: time.Now().Add(l.ttl)}
	return true, nil
}

func (l *fakeLeaser) Release(_ context.Context, key string, token uint64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	le, ok := l.live(key)
	if !ok || le.token != token {
		return false, nil
	}
	delete(l.leases, key)
	return true, nil
}

func (l *fakeLeaser) Holder(_ context.Context, key string) (uint64, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	le, ok := l.live(key)
	return le.token, ok, nil
}

// expire deletes a lease, as a Redis restart would.
func (l *fakeLeaser) expire(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.leases, key)
}

func (l *fakeLeaser) setFailRenew(v bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failRenew = v
}

// memPub collects one worker's snapshots. block holds every publish.
type memPub struct {
	mu    sync.Mutex
	snaps []*transport.Snapshot
	block chan struct{}
}

func (p *memPub) Publish(ctx context.Context, _, value []byte) error {
	p.mu.Lock()
	block := p.block
	p.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s, err := transport.DecodeSnapshot(value)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.snaps = append(p.snaps, s)
	p.mu.Unlock()
	return nil
}

func (p *memPub) run(runID string) []*transport.Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*transport.Snapshot
	for _, s := range p.snaps {
		if s.RunID == runID {
			out = append(out, s)
		}
	}
	return out
}

// fakeAggregator finalizes runs from the workers' published snapshots
// with the aggregator's rules (design.md §4.5): once every member's stream
// 0..final_seq is in, or at once when finalize_requested is set.
type fakeAggregator struct {
	st   *fakeStore
	pubs map[string]*memPub // by worker

	mu sync.Mutex
	// mode: "auto" finalizes complete streams, "force" only finalizes when
	// finalize_requested is set, "off" never finalizes.
	mode string
}

func (a *fakeAggregator) setMode(m string) {
	a.mu.Lock()
	a.mode = m
	a.mu.Unlock()
}

func (a *fakeAggregator) loop(stop <-chan struct{}) {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			a.tick()
		}
	}
}

func (a *fakeAggregator) tick() {
	a.mu.Lock()
	mode := a.mode
	a.mu.Unlock()
	if mode == "off" {
		return
	}
	s := a.st
	s.mu.Lock()
	var cands []string
	for id, fr := range s.runs {
		_, done := s.results[id]
		if !done && (fr.run.State == runstate.Stopping || fr.run.State.Terminal()) {
			cands = append(cands, id)
		}
	}
	s.mu.Unlock()
	for _, id := range cands {
		a.maybeFinalize(id, mode)
	}
}

func (a *fakeAggregator) maybeFinalize(runID, mode string) {
	run, _ := a.st.GetRun(context.Background(), runID)
	workers, _ := a.st.RunWorkers(context.Background(), runID)
	streams := map[string][]*transport.Snapshot{}
	complete := true
	var highest int64 = -1
	for _, w := range workers {
		var ss []*transport.Snapshot
		if p := a.pubs[w.WorkerID]; p != nil {
			ss = p.run(runID)
		}
		streams[w.WorkerID] = ss
		for _, x := range ss {
			highest = max(highest, seqCount(x.Seq)-1)
		}
		if w.FinalSeq == nil || int64(len(ss)) != seqCount(*w.FinalSeq) {
			complete = false
		}
	}
	if !run.FinalizeRequested && (mode == "force" || !complete) {
		return
	}
	lat, lag := engine.NewHistogram(), engine.NewHistogram()
	res := store.ResultRow{RunID: runID}
	reasons := map[string]transport.StopReason{}
	for _, w := range workers {
		if w.FinalSeq != nil {
			res.ExpectedSnapshots += seqCount(*w.FinalSeq)
		} else {
			res.ExpectedSnapshots += highest + 1
		}
		for _, x := range streams[w.WorkerID] {
			if w.FinalSeq != nil && x.Seq > *w.FinalSeq {
				continue
			}
			res.ReceivedSnapshots++
			lat.Merge(x.Latency)
			lag.Merge(x.DispatchLag)
			res.IntervalNSTotal += x.IntervalNS
			res.Intended += x.Intended
			res.OK += x.OK
			res.Err += x.Errors
			res.Timeout += x.Timeouts
			res.Dropped += x.Dropped
			res.Unfinished += x.Unfinished
			res.Clamped += x.Clamped
			if x.Final {
				reasons[w.WorkerID] = x.Reason
			}
		}
	}
	res.ReceivedSnapshots = min(res.ReceivedSnapshots, res.ExpectedSnapshots)
	res.P50, res.P90, res.P99 = lat.ValueAtQuantile(50), lat.ValueAtQuantile(90), lat.ValueAtQuantile(99)
	res.P999, res.Max, res.LagP99 = lat.ValueAtQuantile(99.9), lat.Max(), lag.ValueAtQuantile(99)
	a.st.mu.Lock()
	a.st.results[runID] = res
	a.st.reasons[runID] = reasons
	a.st.mu.Unlock()
}
