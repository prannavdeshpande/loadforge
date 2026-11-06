package aggregator

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/transport"
)

type memProgress struct {
	mu   sync.Mutex
	puts map[string][]LiveProgress
	fail error
}

func (m *memProgress) Put(_ context.Context, runID string, p LiveProgress) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	if m.puts == nil {
		m.puts = map[string][]LiveProgress{}
	}
	m.puts[runID] = append(m.puts[runID], p)
	return nil
}

func (m *memProgress) last(runID string) (LiveProgress, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ps := m.puts[runID]
	if len(ps) == 0 {
		return LiveProgress{}, false
	}
	return ps[len(ps)-1], true
}

type harness struct {
	t    *testing.T
	st   *fakeStore
	in   *Ingester
	agg  *Aggregator
	prog *memProgress
	off  int64
}

func newHarness(t *testing.T) *harness {
	st := newFakeStore()
	prog := &memProgress{}
	return &harness{t: t, st: st, in: NewIngester(st), agg: New(st, prog, DefaultConfig()), prog: prog}
}

func (h *harness) ingest(snaps ...*transport.Snapshot) {
	h.t.Helper()
	for _, s := range snaps {
		if got := handle(h.t, h.in, rec(h.t, s, h.off)); got != Stored {
			h.t.Fatalf("%s/%d: %v", s.WorkerID, s.Seq, got)
		}
		h.off++
	}
}

func (h *harness) tick() TickReport {
	h.t.Helper()
	rep, err := h.agg.Tick(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	return rep
}

func TestMergerMergesEachSecondAcrossWorkers(t *testing.T) {
	h := newHarness(t)
	h.st.addRun("r", runstate.Running, "w1", "w2")
	h.ingest(snap("r", "w1", 0, 100), snap("r", "w2", 0, 200, 300), snap("r", "w1", 1, 400))

	if rep := h.tick(); rep.SecondsMerged != 2 {
		t.Fatalf("merged %d seconds, want 2", rep.SecondsMerged)
	}
	s0, _ := h.st.second("r", 0)
	s1, _ := h.st.second("r", 1)
	if s0.SnapshotsMerged != 2 || s0.OK != 3 || s0.Max < 300 || s1.SnapshotsMerged != 1 || s1.OK != 1 {
		t.Fatalf("seconds: %+v / %+v", s0, s1)
	}
	if rep := h.tick(); rep.SecondsMerged != 0 {
		t.Errorf("re-merged %d unchanged seconds", rep.SecondsMerged)
	}

	// w2's second 1 arrives late: that second is merged again from both.
	h.ingest(snap("r", "w2", 1, 50_000))
	if rep := h.tick(); rep.SecondsMerged != 1 {
		t.Errorf("merged %d seconds after a late snapshot, want 1", rep.SecondsMerged)
	}
	if s1, _ = h.st.second("r", 1); s1.SnapshotsMerged != 2 || s1.Max < 50_000 {
		t.Errorf("second 1 after the late snapshot: %+v", s1)
	}

	p, ok := h.prog.last("r")
	if !ok || p.Seconds != 2 || p.OK != 5 || p.LatestSeq != 1 || p.Latest.Max < 50_000 || p.Finalized {
		t.Errorf("live progress: %+v", p)
	}
}

// A slower merge with fewer inputs must never overwrite a fresher one.
func TestStaleMergeDoesNotOverwrite(t *testing.T) {
	h := newHarness(t)
	h.st.addRun("r", runstate.Running, "w1", "w2")
	h.ingest(snap("r", "w1", 0, 100), snap("r", "w2", 0, 200))
	h.tick()

	stale, err := MergeSecond([]rowT{row(t, "r", "w1", 0, 100)})
	if err != nil {
		t.Fatal(err)
	}
	if wrote, _ := h.st.UpsertSecond(context.Background(), stale); wrote {
		t.Error("a 1-snapshot merge replaced a 2-snapshot one")
	}
}

func TestFinalizesOnceEveryStreamIsComplete(t *testing.T) {
	h := newHarness(t)
	h.st.addRun("r", runstate.Running, "w1", "w2")
	h.ingest(snap("r", "w1", 0, 100, 200), snap("r", "w2", 0, 300),
		final(snap("r", "w1", 1, 400)))
	h.tick()
	h.st.setState("r", runstate.Stopping)
	h.st.stopAck("r", "w1", 1)
	h.st.stopAck("r", "w2", 1)
	if rep := h.tick(); len(rep.Finalized) != 0 {
		t.Fatal("finalized while w2's final snapshot is outstanding")
	}

	h.ingest(final(snap("r", "w2", 1, 2_000_000)))
	if rep := h.tick(); !slices.Equal(rep.Finalized, []string{"r"}) {
		t.Fatalf("finalized %v, want [r]", rep.Finalized)
	}
	res, _ := h.st.result("r")
	if res.ExpectedSnapshots != 4 || res.ReceivedSnapshots != 4 || res.RejectedSnapshots != 0 ||
		res.OK != 5 || res.Intended != 5 || res.IntervalNSTotal != 4e9 {
		t.Errorf("result: %+v", res)
	}
	if res.Max < 2_000_000 || res.P50 < 300 || res.P50 > 400 {
		t.Errorf("result percentiles: %+v", res.Percentiles)
	}
	// Every second was merged too, including the one that arrived last.
	if s1, _ := h.st.second("r", 1); s1.SnapshotsMerged != 2 {
		t.Errorf("second 1: %+v", s1)
	}
	if p, ok := h.prog.last("r"); !ok || !p.Finalized {
		t.Errorf("progress after finalizing: %+v", p)
	}
	if rep := h.tick(); len(rep.Finalized) != 0 {
		t.Error("finalized twice")
	}
}

// Without a STOP ack, the final snapshot itself ends the stream.
func TestFinalSnapshotEndsStreamWithoutAck(t *testing.T) {
	h := newHarness(t)
	h.st.addRun("r", runstate.Stopping, "w1")
	h.ingest(snap("r", "w1", 0, 1), final(snap("r", "w1", 1, 1)))
	if rep := h.tick(); len(rep.Finalized) != 1 {
		t.Fatal("not finalized")
	}
}

// A requested finalization proceeds with gaps and a lost worker, and counts
// what is missing instead of hiding it.
func TestRequestedFinalizationCountsMissingSnapshots(t *testing.T) {
	h := newHarness(t)
	h.st.addRun("r", runstate.Aborted, "w1", "w2", "w3")
	// w1 ended at seq 3 but seq 1 never arrived. w2 was lost after seq 1.
	// w3 never sent anything.
	h.ingest(snap("r", "w1", 0, 1), snap("r", "w1", 2, 1), final(snap("r", "w1", 3, 1)),
		snap("r", "w2", 0, 1), snap("r", "w2", 1, 1))
	h.st.stopAck("r", "w1", 3)
	if rep := h.tick(); len(rep.Finalized) != 0 {
		t.Fatal("finalized incomplete streams without a request")
	}
	h.st.requestFinalize("r")
	if rep := h.tick(); len(rep.Finalized) != 1 {
		t.Fatal("requested finalization did not happen")
	}
	res, _ := h.st.result("r")
	// Each worker owes seq 0..3: 12 expected, 5 received.
	if res.ExpectedSnapshots != 12 || res.ReceivedSnapshots != 5 {
		t.Errorf("expected=%d received=%d, want 12 and 5", res.ExpectedSnapshots, res.ReceivedSnapshots)
	}
}

func TestRunningRunIsNotFinalized(t *testing.T) {
	h := newHarness(t)
	h.st.addRun("r", runstate.Running, "w1")
	h.ingest(final(snap("r", "w1", 0, 1)))
	h.st.requestFinalize("r")
	if rep := h.tick(); len(rep.Finalized) != 0 {
		t.Error("finalized a run that is still RUNNING")
	}
}

func TestRejectsAreCountedInTheResult(t *testing.T) {
	h := newHarness(t)
	h.st.addRun("r", runstate.Stopping, "w1")
	h.ingest(final(snap("r", "w1", 0, 1)))
	handle(t, h.in, Record{Offset: 99, Key: []byte("r:w1"), Value: []byte("garbage")})
	h.tick()
	if res, ok := h.st.result("r"); !ok || res.RejectedSnapshots != 1 {
		t.Errorf("result: %+v", res)
	}
}

// Redis is a cache: a failing progress sink never fails merging or
// finalization.
func TestProgressFailureIsHarmless(t *testing.T) {
	h := newHarness(t)
	h.prog.fail = errors.New("redis down")
	h.st.addRun("r", runstate.Stopping, "w1")
	h.ingest(final(snap("r", "w1", 0, 1)))
	rep := h.tick()
	if rep.SecondsMerged != 1 || len(rep.Finalized) != 1 {
		t.Errorf("tick with Redis down: %+v", rep)
	}
}

func TestLiveProgressIsMarkedProvisional(t *testing.T) {
	f := LiveProgress{}.Fields()
	if f["provisional"] != "true" || f["verdict"] != PendingVerdict {
		t.Errorf("fields: %v", f)
	}
	if ProgressKey("r-1") != "lf:progress:run:r-1" {
		t.Error(ProgressKey("r-1"))
	}
}

func TestExpectedSeqs(t *testing.T) {
	u := func(v uint64) *uint64 { return &v }
	got := ExpectedSeqs([]streamT{
		{WorkerID: "acked", AckFinalSeq: u(4), StoredFinalSeq: u(4), MaxStoredSeq: u(4)},
		{WorkerID: "final-only", StoredFinalSeq: u(2), MaxStoredSeq: u(2)},
		{WorkerID: "lost", MaxStoredSeq: u(1)},
		{WorkerID: "silent"},
	})
	want := map[string]int64{"acked": 5, "final-only": 3, "lost": 5, "silent": 5}
	for w, n := range want {
		if got[w] != n {
			t.Errorf("%s: expected %d, want %d", w, got[w], n)
		}
	}
	if n := ExpectedSeqs([]streamT{{WorkerID: "a"}})["a"]; n != 0 {
		t.Errorf("nothing anywhere: expected %d, want 0", n)
	}
}
