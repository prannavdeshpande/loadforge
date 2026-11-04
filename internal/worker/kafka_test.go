package worker

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/transport"
)

// TestKafkaPublisherKeepsEachWorkerOnOnePartitionInOrder runs two workers'
// streams through the real producer into an in-memory Kafka with the
// production partition count, and reads them back.
func TestKafkaPublisherKeepsEachWorkerOnOnePartitionInOrder(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(12, transport.SnapshotTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	pub, err := NewKafkaPublisher(cluster.ListenAddrs())
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := pub.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	const perWorker = 20
	workers := []string{"w1", "w2"}
	for _, w := range workers {
		em := NewEmitter(ctx, "run-1", w, pub, EmitterConfig{Period: time.Hour, Buffer: perWorker, PublishTimeout: 5 * time.Second})
		start := time.Now()
		for range perWorker - 1 {
			end := start.Add(time.Second)
			em.emit(ctx, oneOK(start, end), "")
			start = end
		}
		if _, err := em.Finish(ctx, oneOK(start, start.Add(time.Second)), transport.ReasonStop); err != nil {
			t.Fatalf("%s: %v", w, err)
		}
		if em.Failed() != 0 {
			t.Fatalf("%s: %d snapshots not delivered", w, em.Failed())
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(transport.SnapshotTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	type seen struct {
		partition int32
		seqs      []uint64
	}
	got := map[string]*seen{}
	total := 0
	for total < perWorker*len(workers) {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("consumed %d of %d records: %v", total, perWorker*len(workers), err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			total++
			s, err := transport.DecodeSnapshot(r.Value)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			key := string(r.Key)
			if want := string(transport.SnapshotKey(s.RunID, s.WorkerID)); key != want {
				t.Errorf("record key %q, payload says %q", key, want)
			}
			g := got[key]
			if g == nil {
				g = &seen{partition: r.Partition}
				got[key] = g
			}
			if r.Partition != g.partition {
				t.Errorf("%s: records on partitions %d and %d", key, g.partition, r.Partition)
			}
			g.seqs = append(g.seqs, s.Seq)
		})
	}
	for _, w := range workers {
		g := got["run-1:"+w]
		if g == nil {
			t.Fatalf("no records for %s", w)
		}
		var want uint64
		for _, seq := range g.seqs {
			if seq != want {
				t.Fatalf("%s: seqs %v, want 0..%d in order", w, g.seqs, perWorker-1)
			}
			want++
		}
	}
	// Same partitioner as Kafka's Java clients (murmur2), so the aggregator
	// and any other producer agree on where a key lives.
	if got["run-1:w1"].partition == got["run-1:w2"].partition {
		t.Logf("both keys hashed to partition %d (allowed, just unlucky)", got["run-1:w1"].partition)
	}
}

var _ Rotator = (*engine.Engine)(nil)
