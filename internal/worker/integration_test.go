//go:build integration

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/stubserver"
	"github.com/loadforge/loadforge/internal/transport"
)

// These tests need the compose stack and the snapshot topic (CLAUDE.md).

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func stackNATS(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(env("LF_NATS_URL", nats.DefaultURL))
	if err != nil {
		t.Fatalf("NATS (is the stack up?): %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func stackKafka(t *testing.T) *KafkaPublisher {
	t.Helper()
	pub, err := NewKafkaPublisher([]string{env("LF_KAFKA", "localhost:9092")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pub.Ping(ctx); err != nil {
		t.Fatalf("Kafka (is the stack up?): %v", err)
	}
	return pub
}

// uniqueRun returns a run ID no earlier test used, so that records left in
// the shared topic by other runs are filtered out.
func uniqueRun(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

type record struct {
	partition int32
	offset    int64
	snap      *transport.Snapshot
}

// consumeRun reads the topic from since and returns runID's records per
// worker, in consumption order, once every worker in want has its final
// snapshot.
func consumeRun(t *testing.T, runID string, since time.Time, want []string) map[string][]record {
	t.Helper()
	c, err := kgo.NewClient(
		kgo.SeedBrokers(env("LF_KAFKA", "localhost:9092")),
		kgo.ConsumeTopics(transport.SnapshotTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(since.Add(-time.Second).UnixMilli())),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got := map[string][]record{}
	done := func() bool {
		for _, w := range want {
			rs := got[w]
			if len(rs) == 0 || !rs[len(rs)-1].snap.Final {
				return false
			}
		}
		return true
	}
	for !done() {
		fs := c.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for final snapshots of %v; have %d workers", want, len(got))
		}
		fs.EachRecord(func(r *kgo.Record) {
			s, err := transport.DecodeSnapshot(r.Value)
			if err != nil {
				t.Errorf("undecodable record at %d/%d: %v", r.Partition, r.Offset, err)
				return
			}
			if s.RunID != runID {
				return
			}
			if string(r.Key) != string(transport.SnapshotKey(s.RunID, s.WorkerID)) {
				t.Errorf("record key %q for %s/%s", r.Key, s.RunID, s.WorkerID)
			}
			got[s.WorkerID] = append(got[s.WorkerID], record{r.Partition, r.Offset, s})
		})
	}
	return got
}

// checkRecords asserts one partition per worker, seq 0..final in offset
// order, and exactly the last final.
func checkRecords(t *testing.T, worker string, rs []record, reason transport.StopReason) {
	t.Helper()
	var want uint64
	for i, r := range rs {
		if r.partition != rs[0].partition {
			t.Errorf("%s: records on partitions %d and %d", worker, rs[0].partition, r.partition)
		}
		if i > 0 && r.offset <= rs[i-1].offset {
			t.Errorf("%s: offsets out of order", worker)
		}
		if r.snap.Seq != want {
			t.Fatalf("%s: record %d has seq %d: stream not contiguous", worker, i, r.snap.Seq)
		}
		want++
		if r.snap.Final != (i == len(rs)-1) {
			t.Errorf("%s: record %d final=%v", worker, i, r.snap.Final)
		}
	}
	if got := rs[len(rs)-1].snap.Reason; got != reason {
		t.Errorf("%s: final reason %q, want %q", worker, got, reason)
	}
}

func TestIntegrationKafkaOrderingPerWorker(t *testing.T) {
	pub := stackKafka(t)
	runID := uniqueRun("kafka")
	since := time.Now()
	workers := []string{"w1", "w2", "w3"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, w := range workers {
		em := NewEmitter(ctx, runID, w, pub, EmitterConfig{Period: time.Hour, Buffer: 32, PublishTimeout: 10 * time.Second})
		start := time.Now()
		for range 15 {
			em.emit(ctx, oneOK(start, start.Add(time.Second)), "")
			start = start.Add(time.Second)
		}
		if _, err := em.Finish(ctx, oneOK(start, start.Add(time.Second)), transport.ReasonCompleted); err != nil {
			t.Fatal(err)
		}
	}
	got := consumeRun(t, runID, since, workers)
	for _, w := range workers {
		checkRecords(t, w, got[w], transport.ReasonCompleted)
		if len(got[w]) != 16 {
			t.Errorf("%s: %d records, want 16", w, len(got[w]))
		}
	}
}

// controller is the test's stand-in for cmd/controlplane: it sends a
// command once and collects the acks of every worker until all have
// answered or the deadline passes.
type controller struct {
	t     *testing.T
	nc    *nats.Conn
	runID string
	fence uint64
	seq   uint64
}

func (c *controller) send(typ transport.CmdType, workers int, set func(*transport.Command)) map[string]transport.Ack {
	c.t.Helper()
	c.seq++
	cmd := transport.Command{Type: typ, RunID: c.runID, CmdSeq: c.seq, FenceToken: c.fence, Deadline: time.Now().Add(15 * time.Second)}
	if set != nil {
		set(&cmd)
	}
	b, _ := json.Marshal(cmd)
	inbox := nats.NewInbox()
	sub, err := c.nc.SubscribeSync(inbox)
	if err != nil {
		c.t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := c.nc.PublishRequest(transport.CmdSubject(c.runID), inbox, b); err != nil {
		c.t.Fatal(err)
	}
	acks := map[string]transport.Ack{}
	for len(acks) < workers {
		m, err := sub.NextMsg(time.Until(cmd.Deadline))
		if err != nil {
			c.t.Fatalf("%s: %d of %d acks before the deadline", typ, len(acks), workers)
		}
		var a transport.Ack
		if err := json.Unmarshal(m.Data, &a); err != nil {
			c.t.Fatal(err)
		}
		acks[a.WorkerID] = a
	}
	return acks
}

func (c *controller) heartbeat() (stop func()) {
	done := make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		b, _ := json.Marshal(transport.Heartbeat{RunID: c.runID, FenceToken: c.fence})
		for {
			_ = c.nc.Publish(transport.RunHeartbeatSubject(c.runID), b)
			select {
			case <-tk.C:
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

// TestIntegrationRunWithStallEndToEnd drives two workers through
// PREPARE → START → STOP over the real NATS and Kafka while the target
// stalls for 2s, then checks what an aggregator would read from Kafka: two
// complete streams whose final seqs match the STOP acks and whose merged
// latency shows the stall (coordinated omission, CLAUDE.md rule 2).
func TestIntegrationRunWithStallEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("runs for ~6s")
	}
	stub := stubserver.New(time.Millisecond)
	srv := httptest.NewServer(stub)
	defer srv.Close()

	pub := stackKafka(t)
	workers := []string{"it-w1", "it-w2"}
	for _, id := range workers {
		w, err := New(Config{ID: id, RateCap: 1000}, stackNATS(t), pub)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Close(context.Background()) })
	}

	ctl := &controller{t: t, nc: stackNATS(t), runID: uniqueRun("e2e"), fence: 1}
	since := time.Now()
	for id, a := range ctl.send(transport.CmdPrepare, 2, func(c *transport.Command) {
		c.Prepare = &transport.Prepare{
			Workers: workers, Method: "GET", TargetURL: srv.URL,
			Rate: 50, DurationMS: 60_000, MaxInFlight: 50, QueueDepth: 1000,
			RequestTimeoutMS: 10_000, DrainTimeoutMS: 10_000,
		}
	}) {
		if a.Status != transport.StatusReady {
			t.Fatalf("%s PREPARE: %+v", id, a)
		}
	}
	defer ctl.heartbeat()()

	startAt := time.Now().Add(time.Second)
	for id, a := range ctl.send(transport.CmdStart, 2, func(c *transport.Command) { c.Start = &transport.Start{StartAt: startAt} }) {
		if a.Status != transport.StatusArmed {
			t.Fatalf("%s START: %+v", id, a)
		}
	}
	t0 := time.Now().Add(time.Until(startAt))
	stall := time.AfterFunc(time.Until(t0.Add(1500*time.Millisecond)), func() { stub.Stall(2 * time.Second) })
	defer stall.Stop()
	time.Sleep(time.Until(t0.Add(4 * time.Second)))

	stopAcks := ctl.send(transport.CmdStop, 2, nil)
	got := consumeRun(t, ctl.runID, since, workers)

	lat := engine.NewHistogram()
	var intended, ok int64
	for _, id := range workers {
		a := stopAcks[id]
		if a.Status != transport.StatusStopped || a.FinalSeq == nil || a.Detail != "" {
			t.Fatalf("%s STOP: %+v", id, a)
		}
		rs := got[id]
		checkRecords(t, id, rs, transport.ReasonStop)
		if last := rs[len(rs)-1].snap.Seq; last != *a.FinalSeq {
			t.Errorf("%s: STOP ack final_seq %d, last record seq %d", id, *a.FinalSeq, last)
		}
		for _, r := range rs {
			lat.Merge(r.snap.Latency)
			intended += r.snap.Intended
			ok += r.snap.OK
		}
	}

	// 2 workers × 50 req/s × 4s.
	if intended < 396 || intended > 404 || ok != intended {
		t.Errorf("intended=%d ok=%d, want ≈ 400 all ok", intended, ok)
	}
	us := func(v int64) time.Duration { return time.Duration(v) * time.Microsecond }
	if p99 := us(lat.ValueAtQuantile(99)); p99 < 1800*time.Millisecond {
		t.Errorf("merged p99 from Kafka = %v, want ≥ 1.8s: the stall is hidden", p99)
	}
	if max := us(lat.Max()); max < 1950*time.Millisecond || max > 2200*time.Millisecond {
		t.Errorf("merged max = %v, want ≈ 2s", max)
	}
	parts := []int32{got[workers[0]][0].partition, got[workers[1]][0].partition}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	t.Logf("run %s: partitions %v, records %d+%d, intended=%d p99=%v max=%v",
		ctl.runID, parts, len(got[workers[0]]), len(got[workers[1]]), intended, us(lat.ValueAtQuantile(99)), us(lat.Max()))
}
