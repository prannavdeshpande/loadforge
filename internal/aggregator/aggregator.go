package aggregator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/loadforge/loadforge/internal/store"
)

// Config tunes the merger loop.
type Config struct {
	Period   time.Duration // how often to merge and finalize; design.md §4.3 says 1s
	MaxDirty int           // seconds merged per tick at most
}

// DefaultConfig merges every second.
func DefaultConfig() Config { return Config{Period: time.Second, MaxDirty: 10_000} }

// Aggregator merges stored snapshots into run_seconds, finalizes runs into
// run_results and publishes live progress. It keeps no state between ticks
// that Postgres does not hold, so a restart, or several aggregators at
// once, is safe: every write it makes is monotonic or written once.
type Aggregator struct {
	st   Store
	prog ProgressSink // may be nil
	cfg  Config
	now  func() time.Time
}

// New returns an Aggregator. prog may be nil, which disables live progress.
func New(st Store, prog ProgressSink, cfg Config) *Aggregator {
	if cfg.Period <= 0 {
		cfg.Period = DefaultConfig().Period
	}
	if cfg.MaxDirty <= 0 {
		cfg.MaxDirty = DefaultConfig().MaxDirty
	}
	return &Aggregator{st: st, prog: prog, cfg: cfg, now: time.Now}
}

// Run ticks every Period until ctx ends. A failed tick is logged and the
// next one retries: nothing a tick does is lost by failing part way.
func (a *Aggregator) Run(ctx context.Context) error {
	t := time.NewTicker(a.cfg.Period)
	defer t.Stop()
	for {
		if _, err := a.Tick(ctx); err != nil && ctx.Err() == nil {
			log.Printf("aggregator: tick: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// TickReport says what one tick did.
type TickReport struct {
	SecondsMerged int
	Finalized     []string
}

// Tick merges every second that has new snapshots, then finalizes every run
// that is ready.
func (a *Aggregator) Tick(ctx context.Context) (TickReport, error) {
	var rep TickReport
	keys, err := a.st.DirtySeconds(ctx, a.cfg.MaxDirty)
	if err != nil {
		return rep, err
	}
	touched := map[string]bool{}
	var order []string
	for _, k := range keys {
		if err := a.mergeSecond(ctx, k); err != nil {
			return rep, err
		}
		rep.SecondsMerged++
		if !touched[k.RunID] {
			touched[k.RunID] = true
			order = append(order, k.RunID)
		}
	}
	for _, id := range order {
		a.publishProgress(ctx, id, false)
	}

	cands, err := a.st.FinalizeCandidates(ctx)
	if err != nil {
		return rep, err
	}
	var errs []error
	for _, c := range cands {
		done, err := a.maybeFinalize(ctx, c)
		if err != nil {
			errs = append(errs, fmt.Errorf("finalize %s: %w", c.RunID, err))
			continue
		}
		if done {
			rep.Finalized = append(rep.Finalized, c.RunID)
			a.publishProgress(ctx, c.RunID, true)
		}
	}
	return rep, errors.Join(errs...)
}

func (a *Aggregator) mergeSecond(ctx context.Context, k store.SecondKey) error {
	rows, err := a.st.SnapshotsForSeq(ctx, k.RunID, k.Seq)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	sec, err := MergeSecond(rows)
	if err != nil {
		return err
	}
	_, err = a.st.UpsertSecond(ctx, sec)
	return err
}

// maybeFinalize writes the run's result once every member's stream is
// complete, or straight away if the controller requested finalization
// (counting what is missing). It reports whether it wrote the result.
func (a *Aggregator) maybeFinalize(ctx context.Context, c store.FinalizeCandidate) (bool, error) {
	streams, err := a.st.Streams(ctx, c.RunID)
	if err != nil {
		return false, err
	}
	if !c.FinalizeRequested && !streamsComplete(streams) {
		return false, nil
	}
	res, err := a.finalize(ctx, c.RunID, streams)
	if err != nil {
		return false, err
	}
	return a.st.PutResult(ctx, res)
}

// streamsComplete reports whether every member has a known final seq and
// has all of 0..final stored. Ingest rejects anything past the final seq
// once it is known, so a count is enough.
func streamsComplete(streams []store.StreamStatus) bool {
	if len(streams) == 0 {
		return false
	}
	for _, s := range streams {
		final, ok := s.FinalSeq()
		if !ok || s.Stored < seqCount(final) {
			return false
		}
	}
	return true
}

// ExpectedSeqs is how many snapshots each worker owes: final seq + 1 when
// the final seq is known. A worker whose stream never ended (lost, or
// finalization was forced) is expected to have reached the highest seq any
// member reached, since all start on one barrier and rotate on one grid.
// That is a lower bound, so completeness is never overstated.
func ExpectedSeqs(streams []store.StreamStatus) map[string]int64 {
	var runMax int64 = -1
	for _, s := range streams {
		if f, ok := s.FinalSeq(); ok {
			runMax = max(runMax, seqCount(f)-1)
		} else if s.MaxStoredSeq != nil {
			runMax = max(runMax, seqCount(*s.MaxStoredSeq)-1)
		}
	}
	out := make(map[string]int64, len(streams))
	for _, s := range streams {
		if f, ok := s.FinalSeq(); ok {
			out[s.WorkerID] = seqCount(f)
		} else {
			out[s.WorkerID] = runMax + 1
		}
	}
	return out
}

// seqCount is the number of snapshots 0..seq. Seqs are stored as
// non-negative bigints, so they fit; the bound only guards the conversion.
func seqCount(seq uint64) int64 {
	if seq >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(seq) + 1
}

// finalize re-merges every second of the run and merges the run's
// snapshots into its result. Only snapshots inside each worker's expected
// stream count; anything else was already rejected at ingest.
func (a *Aggregator) finalize(ctx context.Context, runID string, streams []store.StreamStatus) (store.ResultRow, error) {
	expected := ExpectedSeqs(streams)
	total := NewMerged()
	var (
		received int64
		group    []store.SnapshotRow
	)
	flush := func() error {
		if len(group) == 0 {
			return nil
		}
		sec, err := MergeSecond(group)
		if err != nil {
			return err
		}
		group = group[:0]
		_, err = a.st.UpsertSecond(ctx, sec)
		return err
	}
	err := a.st.ForEachSnapshot(ctx, runID, func(r store.SnapshotRow) error {
		if len(group) > 0 && group[0].Seq != r.Seq {
			if err := flush(); err != nil {
				return err
			}
		}
		group = append(group, r)
		if want, ok := expected[r.WorkerID]; ok && seqCount(r.Seq) <= want {
			received++
			return total.Add(r)
		}
		log.Printf("aggregator: run %s: snapshot %s/%d is outside the worker's stream; left out of the result", runID, r.WorkerID, r.Seq)
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		return store.ResultRow{}, err
	}

	var want int64
	for _, n := range expected {
		want += n
	}
	rejected, err := a.st.RejectCount(ctx, runID)
	if err != nil {
		return store.ResultRow{}, err
	}
	h, err := total.Hists()
	if err != nil {
		return store.ResultRow{}, err
	}
	return store.ResultRow{
		RunID:             runID,
		ExpectedSnapshots: want,
		ReceivedSnapshots: received,
		RejectedSnapshots: rejected,
		IntervalNSTotal:   total.IntervalNS,
		Counts:            total.Counts,
		Hists:             h,
		Percentiles:       total.Percentiles(),
	}, nil
}

func (a *Aggregator) publishProgress(ctx context.Context, runID string, finalized bool) {
	if a.prog == nil {
		return
	}
	p, ok, err := a.st.RunProgress(ctx, runID)
	if err != nil {
		log.Printf("aggregator: progress for %s: %v", runID, err)
		return
	}
	if !ok && !finalized {
		return
	}
	lp := LiveProgress{Progress: p, Finalized: finalized, UpdatedAt: a.now()}
	// Redis is a cache: losing it delays nothing that matters.
	if err := a.prog.Put(ctx, runID, lp); err != nil {
		log.Printf("aggregator: live progress for %s not cached: %v", runID, err)
	}
}
