//go:build integration

package store

import (
	"context"
	"errors"
	"testing"

	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/transport"
)

// TestIntegrationStopRequest covers migration 0002: an unfenced,
// idempotent stop request that never touches the fence or the state.
func TestIntegrationStopRequest(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	tok := newRun(t, s, "stop-req")
	for range 2 {
		if err := s.RequestStop(ctx, "stop-req"); err != nil {
			t.Fatal(err)
		}
	}
	r, err := s.GetRun(ctx, "stop-req")
	if err != nil || !r.StopRequested || r.State != runstate.Pending || r.FenceToken != tok {
		t.Fatalf("run %+v, %v", r, err)
	}
	var first, again any
	_ = s.pool.QueryRow(ctx, `SELECT stop_requested_at FROM runs WHERE id = 'stop-req'`).Scan(&first)
	_ = s.RequestStop(ctx, "stop-req")
	_ = s.pool.QueryRow(ctx, `SELECT stop_requested_at FROM runs WHERE id = 'stop-req'`).Scan(&again)
	if first != again {
		t.Errorf("a repeated request moved stop_requested_at from %v to %v", first, again)
	}
	if err := s.RequestStop(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown run: %v", err)
	}
	move(t, s, "stop-req", tok, runstate.Pending, runstate.Aborted)
	if err := s.RequestStop(ctx, "stop-req"); !errors.Is(err, ErrStateConflict) {
		t.Errorf("terminal run: %v", err)
	}
}

func TestIntegrationMaxCmdSeqAndFinalReasons(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	tok := newRun(t, s, "seq-run", "w1", "w2")
	if n, err := s.MaxCmdSeq(ctx, "seq-run"); err != nil || n != 0 {
		t.Fatalf("no commands: %d, %v", n, err)
	}
	for _, seq := range []uint64{1, 2, 3} {
		if err := s.AppendEvent(ctx, "seq-run", tok, Event{Kind: EventCommand, CmdSeq: seq}); err != nil {
			t.Fatal(err)
		}
	}
	// An ack event for a lower seq does not lower it.
	if err := s.AppendEvent(ctx, "seq-run", tok, Event{Kind: EventAck, WorkerID: "w1", CmdSeq: 2}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.MaxCmdSeq(ctx, "seq-run"); err != nil || n != 3 {
		t.Errorf("MaxCmdSeq = %d, %v", n, err)
	}

	final := aggSnapshot("seq-run", "w2", 1, true)
	final.Reason = string(transport.ReasonDeadman)
	mustInsert(t, s, aggSnapshot("seq-run", "w1", 0, false), aggSnapshot("seq-run", "w1", 1, true),
		aggSnapshot("seq-run", "w2", 0, false), final)
	got, err := s.FinalReasons(ctx, "seq-run")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["w1"] != transport.ReasonStop || got["w2"] != transport.ReasonDeadman {
		t.Errorf("final reasons %v", got)
	}
}
