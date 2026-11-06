package aggregator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/transport"
)

func produce(t *testing.T, ctx context.Context, addrs []string, snaps ...*transport.Snapshot) {
	t.Helper()
	p, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.DefaultProduceTopic(transport.SnapshotTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, s := range snaps {
		b, err := transport.EncodeSnapshot(s)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.ProduceSync(ctx, &kgo.Record{Key: transport.SnapshotKey(s.RunID, s.WorkerID), Value: b}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// consumeUntil runs Consume with a fresh group member until cond holds or
// the timeout passes, keeps consuming for linger, then stops it the way
// SIGTERM would.
func consumeUntil(t *testing.T, addrs []string, st *fakeStore, timeout, linger time.Duration, cond func() bool) {
	t.Helper()
	cl, err := NewKafkaClient(addrs, kgo.FetchMaxWait(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Consume(ctx, cl, NewIngester(st)) }()
	for deadline := time.Now().Add(timeout); !cond() && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(linger)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume: %v", err)
	}
}

// Offsets are committed only once the write has committed: records that
// failed to store are redelivered to the next consumer in the group, and
// records that were stored are not.
func TestConsumeCommitsOnlyAfterTheWrite(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(12, transport.SnapshotTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	addrs := cluster.ListenAddrs()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st := newFakeStore()
	st.addRun("r", runstate.Running, "w1", "w2")
	produce(t, ctx, addrs, snap("r", "w1", 0, 1), snap("r", "w2", 0, 1), snap("r", "w1", 1, 1))

	// Postgres is down: the consumer keeps retrying and commits nothing.
	st.setFailWrite(errors.New("connection refused"))
	consumeUntil(t, addrs, st, time.Second, 0, func() bool { return false })
	if n := st.storedCount("r"); n != 0 {
		t.Fatalf("%d stored while failing", n)
	}

	// Back up: the next member gets every record again and stores each once.
	st.setFailWrite(nil)
	consumeUntil(t, addrs, st, 10*time.Second, 0, func() bool { return st.storedCount("r") == 3 })
	if n := st.storedCount("r"); n != 3 {
		t.Fatalf("%d stored after recovery, want 3", n)
	}

	// Those offsets were committed: a new member is handed only the one new
	// record, not the three before it again.
	produce(t, ctx, addrs, final(snap("r", "w2", 1, 1)))
	before := st.insertAttempts()
	// Linger so that a redelivery, had there been one, would be handled.
	consumeUntil(t, addrs, st, 10*time.Second, 500*time.Millisecond, func() bool { return st.storedCount("r") == 4 })
	if got := st.insertAttempts() - before; st.storedCount("r") != 4 || got != 1 {
		t.Errorf("stored=%d, %d records handled; want 4 stored and 1 handled", st.storedCount("r"), got)
	}
}
