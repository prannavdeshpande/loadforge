package aggregator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/stubserver"
	"github.com/loadforge/loadforge/internal/timingtest"
	"github.com/loadforge/loadforge/internal/transport"
	"github.com/loadforge/loadforge/internal/worker"
)

// ingestPub hands each published snapshot straight to the ingester, as if
// Kafka had delivered it.
type ingestPub struct {
	t   *testing.T
	in  *Ingester
	mu  sync.Mutex
	off int64
}

func (p *ingestPub) Publish(ctx context.Context, key, value []byte) error {
	p.mu.Lock()
	off := p.off
	p.off++
	p.mu.Unlock()
	out, err := p.in.Handle(ctx, Record{Topic: transport.SnapshotTopic, Offset: off, Key: key, Value: value})
	if err == nil && out != Stored {
		p.t.Errorf("snapshot at offset %d: %v", off, out)
	}
	return err
}

// TestMergedResultCarriesTheStall is the standard coordinated-omission case
// (CLAUDE.md, workflow rule 2) through the aggregator: two workers at
// 100 req/s each against one target that freezes for 2s from 1.5s into a
// 4s run. Each worker's ~200 requests intended during the stall complete at
// ~3.5s with latencies from ~2s down to ~0, so of 800 requests ≈ 400 sat
// behind the stall, ≈ 200 are ≥ 1s, and the merged p99 (top 8) is ≈ 1.96s. A closed-loop generator,
// or one timing from the actual send, would report a few slow requests at
// most.
func TestMergedResultCarriesTheStall(t *testing.T) {
	if testing.Short() {
		t.Skip("runs for ~5s")
	}
	timingtest.Exclusive(t) // asserts on dispatch lag: never alongside another such test
	stub := stubserver.New(time.Millisecond)
	srv := httptest.NewServer(stub)
	defer srv.Close()

	st := newFakeStore()
	workers := []string{"w1", "w2"}
	st.addRun("run-co", runstate.Running, workers...)
	pub := &ingestPub{t: t, in: NewIngester(st)}
	agg := New(st, nil, DefaultConfig())

	ctx := context.Background()
	t0 := time.Now().Add(200 * time.Millisecond)
	stall := time.AfterFunc(time.Until(t0.Add(1500*time.Millisecond)), func() { stub.Stall(2 * time.Second) })
	defer stall.Stop()

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eng, err := engine.New(engine.Config{
				Rate: 100, Duration: 4 * time.Second, MaxInFlight: 50, QueueDepth: 1000,
				RequestTimeout: 10 * time.Second, DrainTimeout: 10 * time.Second,
			}, &engine.HTTPDoer{Client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 100}}, Method: http.MethodGet, URL: srv.URL})
			if err != nil {
				t.Error(err)
				return
			}
			em := worker.NewEmitter(ctx, "run-co", w, pub, worker.EmitterConfig{Period: time.Second, Buffer: 60, PublishTimeout: 5 * time.Second})
			em.Start(eng, t0)
			res, err := eng.Run(ctx, t0)
			if err != nil {
				t.Error(err)
			}
			seq, err := em.Finish(ctx, res.Final, transport.ReasonCompleted)
			if err != nil || em.Failed() != 0 {
				t.Errorf("%s: finish: %v, %d undelivered", w, err, em.Failed())
			}
			st.stopAck("run-co", w, seq)
		}()
	}
	wg.Wait()
	st.setState("run-co", runstate.Stopping)

	rep, err := agg.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Finalized) != 1 {
		t.Fatalf("run not finalized: %+v", rep)
	}
	res, _ := st.result("run-co")
	us := func(v int64) time.Duration { return time.Duration(v) * time.Microsecond }

	if res.ReceivedSnapshots != res.ExpectedSnapshots || res.RejectedSnapshots != 0 {
		t.Errorf("snapshots: %d of %d, %d rejected", res.ReceivedSnapshots, res.ExpectedSnapshots, res.RejectedSnapshots)
	}
	if res.Intended < 796 || res.Intended > 804 || res.OK != res.Intended || res.Unfinished != 0 {
		t.Errorf("intended=%d ok=%d unfinished=%d, want ≈ 800 all ok", res.Intended, res.OK, res.Unfinished)
	}
	if p99 := us(res.P99); p99 < 1800*time.Millisecond {
		t.Errorf("merged p99 = %v, want ≥ 1.8s: the stall is hidden", p99)
	}
	if max := us(res.Max); max < 1950*time.Millisecond || max > 2200*time.Millisecond {
		t.Errorf("merged max = %v, want ≈ 2s", max)
	}
	// Half of all requests were intended during the stall, so p90 (the top
	// 80) is ≈ 1.6s and must not look healthy either.
	if p90 := us(res.P90); p90 < 500*time.Millisecond {
		t.Errorf("merged p90 = %v, want well above the 1ms baseline", p90)
	}
	// The stall is the server's: the generator kept its schedule.
	if lag := us(res.LagP99); lag > 25*time.Millisecond {
		t.Errorf("dispatch lag p99 = %v: the server stall leaked into the dispatcher", lag)
	}

	// The slow completions land in the second that holds ~3.5s, merged
	// across both workers, not smeared away or only in the final interval.
	var worst time.Duration
	var worstSeq uint64
	for seq := uint64(0); ; seq++ {
		s, ok := st.second("run-co", seq)
		if !ok {
			break
		}
		if s.SnapshotsMerged != len(workers) {
			t.Errorf("second %d merged %d snapshots, want %d", seq, s.SnapshotsMerged, len(workers))
		}
		if m := us(s.Max); m > worst {
			worst, worstSeq = m, seq
		}
	}
	if worst < 1950*time.Millisecond || worstSeq != 3 {
		t.Errorf("slowest second is %d with max %v, want second 3 with ≈ 2s", worstSeq, worst)
	}
}
