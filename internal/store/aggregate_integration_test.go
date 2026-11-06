//go:build integration

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/transport"
)

func aggSnapshot(run, w string, seq uint64, final bool) SnapshotRow {
	r := SnapshotRow{
		RunID: run, WorkerID: w, Seq: seq, IntervalStartNS: seqNS(seq), IntervalEndNS: seqNS(seq + 1),
		IntervalNS: 1e9, Counts: Counts{Intended: 10, OK: 10},
		Hists: Hists{Latency: []byte("l"), Lag: []byte("g"), SendDelay: []byte("s")},
	}
	if final {
		r.Final, r.Reason = true, "stop"
	}
	return r
}

// seqNS is the start of second seq, in ns; test seqs are small.
func seqNS(seq uint64) int64 {
	if seq > 1<<20 {
		panic("seq out of range")
	}
	return int64(seq) * 1e9
}

func mustInsert(t *testing.T, s *Store, rows ...SnapshotRow) {
	t.Helper()
	for _, r := range rows {
		if ok, err := s.InsertSnapshot(context.Background(), r); err != nil || !ok {
			t.Fatalf("insert %s/%d: %v %v", r.WorkerID, r.Seq, ok, err)
		}
	}
}

func TestIntegrationStreams(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	tok := newRun(t, s, "agg-streams", "w1", "w2")
	mustInsert(t, s, aggSnapshot("agg-streams", "w1", 0, false), aggSnapshot("agg-streams", "w1", 1, true),
		aggSnapshot("agg-streams", "w2", 0, false), aggSnapshot("agg-streams", "w2", 2, false))
	move(t, s, "agg-streams", tok, runstate.Pending, runstate.Preparing, runstate.Ready, runstate.Running, runstate.Stopping)
	one := uint64(1)
	if err := s.RecordStopAck(ctx, "agg-streams", tok, transport.Ack{WorkerID: "w1", RunID: "agg-streams", CmdSeq: 3, Status: transport.StatusStopped, FinalSeq: &one}); err != nil {
		t.Fatal(err)
	}

	st, err := s.Streams(ctx, "agg-streams")
	if err != nil {
		t.Fatal(err)
	}
	if len(st) != 2 {
		t.Fatalf("%d streams", len(st))
	}
	w1, w2 := st[0], st[1]
	if f, ok := w1.FinalSeq(); !ok || f != 1 || w1.Stored != 2 || *w1.StoredFinalSeq != 1 || *w1.MaxStoredSeq != 1 {
		t.Errorf("w1: %+v", w1)
	}
	if _, ok := w2.FinalSeq(); ok || w2.Stored != 2 || *w2.MaxStoredSeq != 2 || w2.StoredFinalSeq != nil {
		t.Errorf("w2: %+v", w2)
	}
	if got, err := s.Stream(ctx, "agg-streams", "w2"); err != nil || got.Stored != 2 {
		t.Errorf("Stream(w2) = %+v, %v", got, err)
	}
	for _, c := range [][2]string{{"agg-streams", "w9"}, {"nope", "w1"}} {
		if _, err := s.Stream(ctx, c[0], c[1]); !errors.Is(err, ErrNotFound) {
			t.Errorf("Stream(%s, %s): %v, want ErrNotFound", c[0], c[1], err)
		}
	}
	if r, err := s.GetSnapshot(ctx, "agg-streams", "w1", 1); err != nil || !r.Final || r.Reason != "stop" {
		t.Errorf("GetSnapshot: %+v, %v", r, err)
	}
	if _, err := s.GetSnapshot(ctx, "agg-streams", "w1", 9); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetSnapshot missing: %v", err)
	}
	var seqs []uint64
	if err := s.ForEachSnapshot(ctx, "agg-streams", func(r SnapshotRow) error { seqs = append(seqs, r.Seq); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 4 || seqs[0] != 0 || seqs[1] != 0 || seqs[2] != 1 || seqs[3] != 2 {
		t.Errorf("ForEachSnapshot order: %v", seqs)
	}
}

func TestIntegrationDirtySecondsAndProgress(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	newRun(t, s, "agg-dirty", "w1", "w2")
	mustInsert(t, s, aggSnapshot("agg-dirty", "w1", 0, false), aggSnapshot("agg-dirty", "w2", 0, false), aggSnapshot("agg-dirty", "w1", 1, false))

	dirty := func() []SecondKey {
		t.Helper()
		k, err := s.DirtySeconds(ctx, 100)
		if err != nil {
			t.Fatal(err)
		}
		return k
	}
	if k := dirty(); len(k) != 2 || k[0] != (SecondKey{"agg-dirty", 0}) || k[1] != (SecondKey{"agg-dirty", 1}) {
		t.Fatalf("dirty = %v", k)
	}
	if k, _ := s.DirtySeconds(ctx, 1); len(k) != 1 {
		t.Errorf("limit ignored: %v", k)
	}
	if _, ok, err := s.RunProgress(ctx, "agg-dirty"); ok || err != nil {
		t.Errorf("progress before any merge: %v %v", ok, err)
	}

	sec := func(seq uint64, n int, p99 int64) SecondRow {
		return SecondRow{RunID: "agg-dirty", Seq: seq, SnapshotsMerged: n, IntervalNSTotal: int64(n) * 1e9,
			Counts: Counts{Intended: int64(10 * n), OK: int64(10 * n)}, Hists: aggSnapshot("", "", 0, false).Hists,
			Percentiles: Percentiles{P99: p99}}
	}
	for _, r := range []SecondRow{sec(0, 2, 100), sec(1, 1, 700)} {
		if _, err := s.UpsertSecond(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if k := dirty(); len(k) != 0 {
		t.Errorf("dirty after merging everything: %v", k)
	}
	mustInsert(t, s, aggSnapshot("agg-dirty", "w2", 1, false))
	if k := dirty(); len(k) != 1 || k[0].Seq != 1 {
		t.Errorf("dirty after a late snapshot: %v", k)
	}

	p, ok, err := s.RunProgress(ctx, "agg-dirty")
	if err != nil || !ok || p.Seconds != 2 || p.LatestSeq != 1 || p.OK != 30 || p.Latest.P99 != 700 || p.IntervalNSTotal != 3e9 {
		t.Errorf("progress = %+v, %v, %v", p, ok, err)
	}

	// A finalized run has nothing dirty, whatever arrives late.
	if _, err := s.PutResult(ctx, ResultRow{RunID: "agg-dirty", ExpectedSnapshots: 4, ReceivedSnapshots: 4, Hists: sec(0, 1, 0).Hists}); err != nil {
		t.Fatal(err)
	}
	if k := dirty(); len(k) != 0 {
		t.Errorf("dirty after finalizing: %v", k)
	}
}

func TestIntegrationFinalizeCandidates(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	path := []runstate.State{runstate.Pending, runstate.Preparing, runstate.Ready, runstate.Running, runstate.Stopping}
	for id, upto := range map[string]int{"cand-running": 4, "cand-stopping": 5, "cand-done": 5} {
		tok := newRun(t, s, id, "w1")
		move(t, s, id, tok, path[:upto]...)
		if id == "cand-stopping" {
			if err := s.RequestFinalize(ctx, id, tok); err != nil {
				t.Fatal(err)
			}
		}
	}
	tok := newRun(t, s, "cand-aborted", "w1")
	move(t, s, "cand-aborted", tok, runstate.Pending, runstate.Aborted)
	if _, err := s.PutResult(ctx, ResultRow{RunID: "cand-done", Hists: aggSnapshot("", "", 0, false).Hists}); err != nil {
		t.Fatal(err)
	}

	c, err := s.FinalizeCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]FinalizeCandidate{}
	for _, x := range c {
		got[x.RunID] = x
	}
	if len(got) != 2 || !got["cand-stopping"].FinalizeRequested || got["cand-aborted"].State != runstate.Aborted {
		t.Errorf("candidates = %+v", c)
	}
}

func TestIntegrationVerdictInputs(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	tok := newRun(t, s, "vin", "w1", "w2") // config {"rate":100}
	move(t, s, "vin", tok, runstate.Pending, runstate.Preparing)
	off := int64(250_000)
	for _, a := range []transport.Ack{
		{WorkerID: "w1", RunID: "vin", CmdSeq: 1, Status: transport.StatusReady, ClockOffsetNS: &off},
		{WorkerID: "w2", RunID: "vin", CmdSeq: 1, Status: transport.StatusReady}, // unmeasured
	} {
		if err := s.RecordPrepareAck(ctx, "vin", tok, a); err != nil {
			t.Fatal(err)
		}
	}
	move(t, s, "vin", tok, runstate.Preparing, runstate.Ready, runstate.Running)

	in, th, err := s.VerdictInputs(ctx, "vin")
	if err != nil {
		t.Fatal(err)
	}
	if th != health.DefaultThresholds() || in.Workers != 2 || in.TargetRatePerWorker != 100 || in.HasResult || in.Aborted ||
		len(in.ClockOffsetsNS) != 2 || *in.ClockOffsetsNS[0] != off || in.ClockOffsetsNS[1] != nil ||
		in.CPUP95Percent != nil || in.DispatchWindowNS != nil || in.ProtocolViolations != 0 {
		t.Fatalf("inputs before the result: %+v", in)
	}

	if err := s.MarkWorkerLost(ctx, "vin", tok, "w2", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertReject(ctx, Reject{Topic: "t", Partition: 0, Offset: 1, RunID: "vin", Error: "decode"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvent(ctx, "vin", tok, Event{Kind: EventProtocolViolation, WorkerID: "w1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutResult(ctx, ResultRow{RunID: "vin", ExpectedSnapshots: 10, ReceivedSnapshots: 9,
		Counts: Counts{Intended: 1000, OK: 998, Dropped: 2}, Hists: aggSnapshot("", "", 0, false).Hists,
		Percentiles: Percentiles{LagP99: 1234}}); err != nil {
		t.Fatal(err)
	}
	in, _, err = s.VerdictInputs(ctx, "vin")
	if err != nil {
		t.Fatal(err)
	}
	if !in.HasResult || *in.LagP99US != 1234 || in.Intended != 1000 || in.Dropped != 2 ||
		in.ExpectedSnapshots != 10 || in.ReceivedSnapshots != 9 ||
		len(in.LostWorkers) != 1 || in.LostWorkers[0] != "w2" || in.ProtocolViolations != 2 {
		t.Fatalf("inputs after the result: %+v", in)
	}
	r, err := health.Evaluate(in, th)
	if err != nil {
		t.Fatal(err)
	}
	if r.Verdict != health.Invalid {
		t.Errorf("verdict = %s, want INVALID (lost worker, violations)", r.Verdict)
	}

	// Per-run overrides are applied; a malformed one is an error, not a
	// silent default.
	if _, err := s.Pool().Exec(ctx, `UPDATE runs SET thresholds = '{"lag_p99_valid_us": 500}' WHERE id = 'vin'`); err != nil {
		t.Fatal(err)
	}
	if _, th, err := s.VerdictInputs(ctx, "vin"); err != nil || th.LagP99ValidUS != 500 {
		t.Errorf("override: %+v, %v", th, err)
	}
	if _, err := s.Pool().Exec(ctx, `UPDATE runs SET thresholds = '{"lag_p99_vaild_us": 500}' WHERE id = 'vin'`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.VerdictInputs(ctx, "vin"); err == nil {
		t.Error("misspelt threshold accepted")
	}
}
