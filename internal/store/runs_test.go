package store

import (
	"context"
	"errors"
	"testing"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/transport"
)

// These checks run before any SQL, so a Store with no pool is enough: if
// one of them reached the database, the nil pool would panic.

func TestTransitionVerdictRulesCheckedBeforeWriting(t *testing.T) {
	s := &Store{}
	ctx := context.Background()
	for name, tr := range map[string]Transition{
		"illegal edge":           {From: runstate.Pending, To: runstate.Running},
		"completed, no verdict":  {From: runstate.Stopping, To: runstate.Completed},
		"completed, bad verdict": {From: runstate.Stopping, To: runstate.Completed, Verdict: "OK"},
		"aborted, no verdict":    {From: runstate.Running, To: runstate.Aborted},
		"aborted, valid":         {From: runstate.Running, To: runstate.Aborted, Verdict: health.Valid},
		"aborted, degraded":      {From: runstate.Running, To: runstate.Aborted, Verdict: health.Degraded},
		"running with verdict":   {From: runstate.Ready, To: runstate.Running, Verdict: health.Valid},
	} {
		tr.RunID, tr.Fence = "r1", 1
		if err := s.Transition(ctx, tr); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	err := s.Transition(ctx, Transition{RunID: "r1", Fence: 1, From: runstate.Completed, To: runstate.Aborted, Verdict: health.Invalid})
	if !errors.Is(err, runstate.ErrIllegalTransition) {
		t.Errorf("leaving a terminal state: err = %v", err)
	}
}

func TestCreateRunValidatesBeforeWriting(t *testing.T) {
	s := &Store{}
	ok := NewRun{ID: "r1", Config: []byte(`{}`), Thresholds: []byte(`{}`), Workers: []string{"w1"}, Fence: 1}
	for name, mut := range map[string]func(*NewRun){
		"bad run id":       func(r *NewRun) { r.ID = "r.1" },
		"no fence":         func(r *NewRun) { r.Fence = 0 },
		"no workers":       func(r *NewRun) { r.Workers = nil },
		"bad worker id":    func(r *NewRun) { r.Workers = []string{"w:1"} },
		"duplicate worker": func(r *NewRun) { r.Workers = []string{"w1", "w1"} },
		"bad config":       func(r *NewRun) { r.Config = []byte(`{`) },
		"no thresholds":    func(r *NewRun) { r.Thresholds = nil },
	} {
		r := ok
		mut(&r)
		if err := s.CreateRun(context.Background(), r); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestAckAndRowShapeCheckedBeforeWriting(t *testing.T) {
	s := &Store{}
	ctx := context.Background()
	if err := s.RecordPrepareAck(ctx, "r1", 1, transport.Ack{WorkerID: "w1", Status: transport.StatusArmed}); err == nil {
		t.Error("PREPARE ack with status armed accepted")
	}
	if err := s.RecordStopAck(ctx, "r1", 1, transport.Ack{WorkerID: "w1", Status: transport.StatusStopped}); err == nil {
		t.Error("stopped ack without final_seq accepted")
	}
	h := Hists{Latency: []byte{1}, Lag: []byte{1}, SendDelay: []byte{1}}
	if _, err := s.InsertSnapshot(ctx, SnapshotRow{RunID: "r1", WorkerID: "w1", Hists: Hists{Latency: []byte{1}}}); err == nil {
		t.Error("snapshot missing histograms accepted")
	}
	if _, err := s.InsertSnapshot(ctx, SnapshotRow{RunID: "r1", WorkerID: "w1", Hists: h, Final: true}); err == nil {
		t.Error("final snapshot without reason accepted")
	}
	if _, err := s.UpsertSecond(ctx, SecondRow{RunID: "r1", Hists: h}); err == nil {
		t.Error("second merged from zero snapshots accepted")
	}
	if _, err := s.PutResult(ctx, ResultRow{RunID: "r1", Hists: h, ExpectedSnapshots: 1, ReceivedSnapshots: 2}); err == nil {
		t.Error("result with more snapshots received than expected accepted")
	}
	if err := s.InsertReject(ctx, Reject{Topic: "t"}); err == nil {
		t.Error("reject without an error accepted")
	}
}

func TestCountsAdd(t *testing.T) {
	c := Counts{1, 2, 3, 4, 5, 6, 7}
	c.Add(Counts{10, 20, 30, 40, 50, 60, 70})
	if c != (Counts{11, 22, 33, 44, 55, 66, 77}) {
		t.Errorf("got %+v", c)
	}
}
