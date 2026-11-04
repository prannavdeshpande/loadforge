package worker

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/loadforge/loadforge/internal/transport"
)

// KafkaPublisher produces snapshots to transport.SnapshotTopic.
//
// The client is idempotent with acks from all in-sync replicas (franz-go's
// defaults), and keyed records are partitioned with Kafka's murmur2 hash, so
// every snapshot of one worker lands in one partition, in order and without
// duplicates from retries.
type KafkaPublisher struct {
	client *kgo.Client
}

// NewKafkaPublisher connects to brokers. extra options are for tests.
func NewKafkaPublisher(brokers []string, extra ...kgo.Opt) (*KafkaPublisher, error) {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(transport.SnapshotTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
		kgo.RecordDeliveryTimeout(10 * time.Second),
	}, extra...)
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{client: c}, nil
}

// Ping checks that a broker is reachable.
func (p *KafkaPublisher) Ping(ctx context.Context) error { return p.client.Ping(ctx) }

// Publish produces one record and waits for the broker's ack.
func (p *KafkaPublisher) Publish(ctx context.Context, key, value []byte) error {
	return p.client.ProduceSync(ctx, &kgo.Record{Key: key, Value: value}).FirstErr()
}

func (p *KafkaPublisher) Close() { p.client.Close() }
