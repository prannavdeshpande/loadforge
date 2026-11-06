// Package kafkatest positions integration-test consumers on the shared
// snapshot topic deterministically.
//
// Tests share one topic with every earlier run's records. Starting a
// consumer from a timestamp (kgo's AfterMilli) is racy: franz-go resolves it
// with a timestamp lookup and, when that finds nothing yet, a separate
// end-offset lookup, so a record appended between the two is skipped. Under
// load that lost a whole test's snapshots. Taking the end offsets before the
// test produces anything, and starting exactly there, cannot skip a record.
package kafkatest

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func client(t testing.TB, brokers []string) (*kgo.Client, context.Context) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(func() { cancel(); cl.Close() })
	return cl, ctx
}

// EndOffsets returns the current end offset of every partition of topic.
// Call it before the test produces anything.
func EndOffsets(t testing.TB, brokers []string, topic string) map[int32]int64 {
	t.Helper()
	cl, ctx := client(t, brokers)

	meta := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(topic)
	meta.Topics = append(meta.Topics, mt)
	mresp, err := meta.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("Kafka metadata (is the stack up?): %v", err)
	}
	if len(mresp.Topics) != 1 || kerr.ErrorForCode(mresp.Topics[0].ErrorCode) != nil || len(mresp.Topics[0].Partitions) == 0 {
		t.Fatalf("topic %s (was it created?): %+v", topic, mresp.Topics)
	}

	list := kmsg.NewPtrListOffsetsRequest()
	list.ReplicaID = -1
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = topic
	for _, p := range mresp.Topics[0].Partitions {
		lp := kmsg.NewListOffsetsRequestTopicPartition()
		lp.Partition = p.Partition
		lp.CurrentLeaderEpoch = -1
		lp.Timestamp = -1 // latest
		lt.Partitions = append(lt.Partitions, lp)
	}
	list.Topics = append(list.Topics, lt)
	lresp, err := list.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[int32]int64, len(lt.Partitions))
	for _, rt := range lresp.Topics {
		for _, rp := range rt.Partitions {
			if err := kerr.ErrorForCode(rp.ErrorCode); err != nil {
				t.Fatalf("end offset of %s/%d: %v", topic, rp.Partition, err)
			}
			out[rp.Partition] = rp.Offset
		}
	}
	if len(out) != len(mresp.Topics[0].Partitions) {
		t.Fatalf("listed %d end offsets for %d partitions", len(out), len(mresp.Topics[0].Partitions))
	}
	return out
}

// ConsumeFrom is a kgo option for a consumer without a group that reads
// topic from exactly offsets.
func ConsumeFrom(topic string, offsets map[int32]int64) kgo.Opt {
	ps := make(map[int32]kgo.Offset, len(offsets))
	for p, o := range offsets {
		ps[p] = kgo.NewOffset().At(o)
	}
	return kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: ps})
}

// StartGroupAtEnd commits topic's current end offsets for group, which must
// have no members yet, so a consumer in it reads exactly the records
// produced from now on. Call it before the test produces anything.
func StartGroupAtEnd(t testing.TB, brokers []string, group, topic string) {
	t.Helper()
	offsets := EndOffsets(t, brokers, topic)
	cl, ctx := client(t, brokers)

	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = group
	commit.Generation = -1 // a group with no members yet
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = topic
	for p, o := range offsets {
		cp := kmsg.NewOffsetCommitRequestTopicPartition()
		cp.Partition = p
		cp.Offset = o
		cp.LeaderEpoch = -1
		ct.Partitions = append(ct.Partitions, cp)
	}
	commit.Topics = append(commit.Topics, ct)
	cresp, err := commit.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, rt := range cresp.Topics {
		for _, rp := range rt.Partitions {
			if err := kerr.ErrorForCode(rp.ErrorCode); err != nil {
				t.Fatalf("commit start offset of %s/%d: %v", topic, rp.Partition, err)
			}
		}
	}
}
