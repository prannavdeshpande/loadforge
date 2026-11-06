package aggregator

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/loadforge/loadforge/internal/transport"
)

// ConsumerGroup is the aggregators' Kafka consumer group. Several
// aggregators share the topic's partitions; each worker's snapshots stay on
// one partition, so one aggregator sees them in order.
const ConsumerGroup = "loadforge-aggregator"

// NewKafkaClient returns a consumer for the snapshot topic that commits
// only offsets marked with MarkCommitRecords, which Consume does after the
// record's write has committed in Postgres.
func NewKafkaClient(brokers []string, extra ...kgo.Opt) (*kgo.Client, error) {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(ConsumerGroup),
		kgo.ConsumeTopics(transport.SnapshotTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
		// No rebalance while a poll's records are being written, so a
		// partition is never handed over half-processed.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				log.Printf("aggregator: commit on revoke: %v (records will be redelivered)", err)
			}
		}),
	}, extra...)
	return kgo.NewClient(opts...)
}

// Consume polls cl and ingests every record in order until ctx ends. A
// record whose write fails is retried with backoff and never skipped, so
// its partition waits rather than losing a snapshot. On return the marked
// offsets have been committed as far as possible.
func Consume(ctx context.Context, cl *kgo.Client, in *Ingester) error {
	defer func() {
		// ctx is usually cancelled by now; the final commit still needs time.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := cl.CommitMarkedOffsets(cctx); err != nil {
			log.Printf("aggregator: final commit: %v (records will be redelivered)", err)
		}
	}()
	for {
		fetches := cl.PollRecords(ctx, 1000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			cl.AllowRebalance()
			return ctx.Err()
		}
		fetches.EachError(func(topic string, p int32, err error) {
			log.Printf("aggregator: fetch %s/%d: %v", topic, p, err)
		})
		var stopped bool
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, r := range p.Records {
				if stopped {
					return
				}
				if !handleWithRetry(ctx, in, r) {
					stopped = true
					return
				}
				cl.MarkCommitRecords(r)
			}
		})
		cl.AllowRebalance()
		if stopped {
			return ctx.Err()
		}
	}
}

// handleWithRetry ingests r, retrying transient failures until it succeeds
// or ctx ends. It reports whether r was handled.
func handleWithRetry(ctx context.Context, in *Ingester, r *kgo.Record) bool {
	rec := Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value}
	backoff := 100 * time.Millisecond
	for {
		out, err := in.Handle(ctx, rec)
		if err == nil {
			if out == Rejected {
				log.Printf("aggregator: rejected %s/%d@%d (key %q)", r.Topic, r.Partition, r.Offset, r.Key)
			}
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		log.Printf("aggregator: %s/%d@%d: %v; retrying in %v", r.Topic, r.Partition, r.Offset, err, backoff)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 5*time.Second)
	}
}
