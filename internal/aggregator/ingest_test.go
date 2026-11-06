package aggregator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/transport"
)

func handle(t *testing.T, in *Ingester, r Record) Outcome {
	t.Helper()
	out, err := in.Handle(context.Background(), r)
	if err != nil {
		t.Fatalf("handle offset %d: %v", r.Offset, err)
	}
	return out
}

func TestRedeliveryIsStoredOnce(t *testing.T) {
	st := newFakeStore()
	st.addRun("r", runstate.Running, "w1")
	in := NewIngester(st)
	r := rec(t, snap("r", "w1", 0, 100, 200), 10)

	if got := handle(t, in, r); got != Stored {
		t.Fatalf("first delivery: %v", got)
	}
	// Redelivered at the same offset (a crash before commit) and at a new
	// one (the producer retried).
	if got := handle(t, in, r); got != Duplicate {
		t.Errorf("redelivery: %v", got)
	}
	r.Offset = 11
	if got := handle(t, in, r); got != Duplicate {
		t.Errorf("producer retry: %v", got)
	}
	if st.inserts != 1 || len(st.rejectList()) != 0 {
		t.Errorf("inserts=%d rejects=%v", st.inserts, st.rejectList())
	}
}

func TestIngestRejects(t *testing.T) {
	good := func() *transport.Snapshot { return snap("r", "w1", 2, 100) }
	for _, c := range []struct {
		name    string
		prepare func(*fakeStore, *Ingester)
		record  func(t *testing.T) Record
		run     string // the run the reject is attributed to
		want    string // in the reject's error
	}{
		{
			name:   "undecodable payload, attributed by key",
			record: func(*testing.T) Record { return Record{Offset: 1, Key: []byte("r:w1"), Value: []byte("garbage")} },
			run:    "r", want: "decode",
		},
		{
			name:   "undecodable payload, malformed key",
			record: func(*testing.T) Record { return Record{Offset: 1, Key: []byte("no colon"), Value: []byte("garbage")} },
			run:    "", want: "decode",
		},
		{
			name: "key does not match payload",
			record: func(t *testing.T) Record {
				r := rec(t, good(), 1)
				r.Key = []byte("r:w2")
				return r
			},
			run: "r", want: "does not match",
		},
		{
			name:   "unknown run",
			record: func(t *testing.T) Record { return rec(t, snap("nope", "w1", 0, 1), 1) },
			run:    "nope", want: "not a member",
		},
		{
			name:   "worker not in the run",
			record: func(t *testing.T) Record { return rec(t, snap("r", "w9", 0, 1), 1) },
			run:    "r", want: "not a member",
		},
		{
			name:    "after the acked final seq",
			prepare: func(st *fakeStore, _ *Ingester) { st.stopAck("r", "w1", 1) },
			record:  func(t *testing.T) Record { return rec(t, good(), 1) },
			run:     "r", want: "after the stream's final seq 1",
		},
		{
			name:    "final at the wrong seq",
			prepare: func(st *fakeStore, _ *Ingester) { st.stopAck("r", "w1", 3) },
			record:  func(t *testing.T) Record { return rec(t, final(good()), 1) },
			run:     "r", want: "final seq is 3",
		},
		{
			name:    "final seq not marked final",
			prepare: func(st *fakeStore, _ *Ingester) { st.stopAck("r", "w1", 2) },
			record:  func(t *testing.T) Record { return rec(t, good(), 1) },
			run:     "r", want: "not marked final",
		},
		{
			name: "after a stored final snapshot",
			prepare: func(st *fakeStore, in *Ingester) {
				_, _ = in.Handle(context.Background(), rec(t, final(snap("r", "w1", 1, 1)), 0))
			},
			record: func(t *testing.T) Record { return rec(t, good(), 1) },
			run:    "r", want: "after the stream's final seq 1",
		},
		{
			name: "final below a stored seq",
			prepare: func(st *fakeStore, in *Ingester) {
				_, _ = in.Handle(context.Background(), rec(t, snap("r", "w1", 5, 1), 0))
			},
			record: func(t *testing.T) Record { return rec(t, final(good()), 1) },
			run:    "r", want: "seq 5 is already stored",
		},
		{
			name: "seq reused with different content",
			prepare: func(st *fakeStore, in *Ingester) {
				_, _ = in.Handle(context.Background(), rec(t, good(), 0))
			},
			record: func(t *testing.T) Record { return rec(t, snap("r", "w1", 2, 999), 1) },
			run:    "r", want: "reused",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := newFakeStore()
			st.addRun("r", runstate.Running, "w1", "w2")
			in := NewIngester(st)
			if c.prepare != nil {
				c.prepare(st, in)
			}
			before := st.storedCount("r")
			r := c.record(t)
			if got := handle(t, in, r); got != Rejected {
				t.Fatalf("outcome %v, want rejected", got)
			}
			if st.storedCount("r") != before {
				t.Error("a rejected snapshot was stored")
			}
			var found bool
			for _, rj := range st.rejectList() {
				if rj.Offset == r.Offset {
					found = true
					if rj.RunID != c.run || !strings.Contains(rj.Error, c.want) {
						t.Errorf("reject run=%q error=%q; want run %q, error containing %q", rj.RunID, rj.Error, c.run, c.want)
					}
				}
			}
			if !found {
				t.Errorf("no reject recorded at offset %d: %v", r.Offset, st.rejectList())
			}
			// A rejected record redelivered at its offset adds no reject row.
			n := len(st.rejectList())
			handle(t, in, r)
			if got := len(st.rejectList()); got != n {
				t.Errorf("%d rejects after redelivery, want %d", got, n)
			}
		})
	}
}

// A redelivered final snapshot is a duplicate, not a contradiction of the
// stream it ended.
func TestRedeliveredFinalIsNotAViolation(t *testing.T) {
	st := newFakeStore()
	st.addRun("r", runstate.Stopping, "w1")
	in := NewIngester(st)
	handle(t, in, rec(t, snap("r", "w1", 0, 1), 0))
	f := rec(t, final(snap("r", "w1", 1, 1)), 1)
	handle(t, in, f)
	st.stopAck("r", "w1", 1)
	if got := handle(t, in, f); got != Duplicate {
		t.Errorf("redelivered final: %v", got)
	}
	if got := handle(t, in, rec(t, snap("r", "w1", 0, 1), 0)); got != Duplicate {
		t.Errorf("redelivered seq 0: %v", got)
	}
}

// A store failure must surface as an error, so the record's offset is not
// committed and nothing is skipped, and retrying once the store is back
// stores it.
func TestStoreFailureIsRetriedNotSkipped(t *testing.T) {
	st := newFakeStore()
	st.addRun("r", runstate.Running, "w1")
	in := NewIngester(st)
	down := errors.New("connection refused")
	st.setFailWrite(down)

	for _, r := range []Record{rec(t, snap("r", "w1", 0, 1), 0), {Offset: 1, Key: []byte("r:w1"), Value: []byte("garbage")}} {
		if _, err := in.Handle(context.Background(), r); !errors.Is(err, down) {
			t.Fatalf("offset %d: err = %v, want the store's error", r.Offset, err)
		}
	}
	if st.storedCount("r") != 0 || len(st.rejectList()) != 0 {
		t.Fatal("wrote while the store was failing")
	}
	st.setFailWrite(nil)
	if got := handle(t, in, rec(t, snap("r", "w1", 0, 1), 0)); got != Stored {
		t.Errorf("after recovery: %v", got)
	}
}
