package aggregator

import (
	"math"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

func hist(us ...int64) *hdrhistogram.Histogram {
	h := engine.NewHistogram()
	for _, v := range us {
		if err := h.RecordValue(v); err != nil {
			panic(err)
		}
	}
	return h
}

var testT0 = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

// snap is a 1s snapshot of seq for worker w holding one OK request per
// latency given, each released with 5µs of lag.
func snap(run, w string, seq uint64, latUS ...int64) *transport.Snapshot {
	lags := make([]int64, len(latUS))
	for i := range lags {
		lags[i] = 5
	}
	start := testT0.Add(time.Duration(toI64(seq)) * time.Second)
	return &transport.Snapshot{
		RunID: run, WorkerID: w, Seq: seq,
		IntervalStart: start, IntervalEnd: start.Add(time.Second), IntervalNS: int64(time.Second),
		Latency: hist(latUS...), SendDelay: hist(lags...), DispatchLag: hist(lags...),
		Intended: int64(len(latUS)), OK: int64(len(latUS)),
	}
}

func final(s *transport.Snapshot) *transport.Snapshot {
	s.Final, s.Reason = true, transport.ReasonStop
	return s
}

// rec encodes s as the Kafka record at offset off.
func rec(t *testing.T, s *transport.Snapshot, off int64) Record {
	t.Helper()
	b, err := transport.EncodeSnapshot(s)
	if err != nil {
		t.Fatal(err)
	}
	return Record{Topic: transport.SnapshotTopic, Partition: 3, Offset: off, Key: transport.SnapshotKey(s.RunID, s.WorkerID), Value: b}
}

// toI64 converts a test seq or value; they are always small.
func toI64(u uint64) int64 {
	if u > math.MaxInt64 {
		panic("value out of range")
	}
	return int64(u)
}

type (
	rowT    = store.SnapshotRow
	streamT = store.StreamStatus
)
