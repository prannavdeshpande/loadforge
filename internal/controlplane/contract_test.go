package controlplane

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

// storeContract checks the fencing and state rules the controller relies
// on. It runs against the fake here and against Postgres in the
// integration tests, so the fake cannot drift from the real store.
func storeContract(t *testing.T, st Store) {
	t.Helper()
	ctx := context.Background()
	id := fmt.Sprintf("contract-%d", time.Now().UnixNano())
	tok1, err := st.NextFenceToken(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, store.NewRun{ID: id, Config: mustJSON(transport.Prepare{Rate: 10}), Thresholds: []byte(`{}`),
		Workers: []string{"w1", "w2"}, Fence: tok1}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, store.NewRun{ID: id, Config: []byte(`{}`), Thresholds: []byte(`{}`), Workers: []string{"w1"}, Fence: tok1}); !errors.Is(err, store.ErrExists) {
		t.Errorf("duplicate run: %v", err)
	}
	tr := func(tok uint64, from, to runstate.State) error {
		t := store.Transition{RunID: id, Fence: tok, From: from, To: to}
		if to.Terminal() {
			t.Verdict = health.Invalid
		}
		return st.Transition(ctx, t)
	}
	if err := tr(tok1, runstate.Pending, runstate.Preparing); err != nil {
		t.Fatal(err)
	}
	if err := tr(tok1, runstate.Pending, runstate.Preparing); !errors.Is(err, store.ErrStateConflict) {
		t.Errorf("wrong from-state: %v", err)
	}
	if err := tr(tok1, runstate.Preparing, runstate.Running); err == nil {
		t.Error("illegal edge PREPARING → RUNNING accepted")
	}

	// Commands are logged before sending; MaxCmdSeq reads the highest.
	for seq := uint64(1); seq <= 2; seq++ {
		if err := st.AppendEvent(ctx, id, tok1, store.Event{Kind: store.EventCommand, CmdSeq: seq}); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := st.MaxCmdSeq(ctx, id); err != nil || n != 2 {
		t.Errorf("MaxCmdSeq = %d, %v", n, err)
	}

	// A stop request is unfenced and idempotent, and visible on the run.
	for range 2 {
		if err := st.RequestStop(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	if r, _ := st.GetRun(ctx, id); !r.StopRequested {
		t.Error("stop request not visible")
	}
	if err := st.RequestStop(ctx, id+"-nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("stop of unknown run: %v", err)
	}

	// A higher token fences out the lower one for every guarded write.
	tok2, _ := st.NextFenceToken(ctx)
	if err := st.AppendEvent(ctx, id, tok2, store.Event{Kind: store.EventRecovered}); err != nil {
		t.Fatal(err)
	}
	seq := uint64(3)
	for name, write := range map[string]func() error{
		"transition": func() error { return tr(tok1, runstate.Preparing, runstate.Ready) },
		"event":      func() error { return st.AppendEvent(ctx, id, tok1, store.Event{Kind: "x"}) },
		"prepare ack": func() error {
			return st.RecordPrepareAck(ctx, id, tok1, transport.Ack{WorkerID: "w1", Status: transport.StatusReady})
		},
		"stop ack": func() error {
			return st.RecordStopAck(ctx, id, tok1, transport.Ack{WorkerID: "w1", Status: transport.StatusStopped, FinalSeq: &seq})
		},
		"lost":     func() error { return st.MarkWorkerLost(ctx, id, tok1, "w1", nil) },
		"finalize": func() error { return st.RequestFinalize(ctx, id, tok1) },
	} {
		if err := write(); !errors.Is(err, store.ErrStaleFence) {
			t.Errorf("%s with the old token: %v", name, err)
		}
	}

	// A failed write does not raise the token: a stale-state transition
	// with a yet higher token leaves tok2 able to write.
	tok3, _ := st.NextFenceToken(ctx)
	if err := tr(tok3, runstate.Running, runstate.Stopping); err == nil {
		t.Fatal("transition from the wrong state accepted")
	}
	if err := st.RecordStopAck(ctx, id, tok2, transport.Ack{WorkerID: "w1", Status: transport.StatusStopped, FinalSeq: &seq}); err != nil {
		t.Errorf("tok2 fenced out by a failed write: %v", err)
	}
	// A contradicting final_seq is refused and logged as a violation.
	other := seq + 1
	if err := st.RecordStopAck(ctx, id, tok2, transport.Ack{WorkerID: "w1", Status: transport.StatusStopped, FinalSeq: &other}); !errors.Is(err, store.ErrProtocolViolation) {
		t.Errorf("contradicting final_seq: %v", err)
	}
	if in, _, err := st.VerdictInputs(ctx, id); err != nil || in.ProtocolViolations != 1 {
		t.Errorf("violations = %d, %v", in.ProtocolViolations, err)
	}

	if err := tr(tok2, runstate.Preparing, runstate.Aborted); err != nil {
		t.Fatal(err)
	}
	if err := st.RequestStop(ctx, id); !errors.Is(err, store.ErrStateConflict) {
		t.Errorf("stop of a terminal run: %v", err)
	}
	if reasons, err := st.FinalReasons(ctx, id); err != nil || len(reasons) != 0 {
		t.Errorf("final reasons with no snapshots: %v, %v", reasons, err)
	}
}

func TestFakeStoreContract(t *testing.T) { storeContract(t, newFakeStore()) }
