package aggregator

import (
	"strings"
	"testing"
	"time"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/store"
)

func row(t *testing.T, run, w string, seq uint64, latUS ...int64) store.SnapshotRow {
	t.Helper()
	r, err := snapshotRow(snap(run, w, seq, latUS...))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// Merging must give exactly the histogram of recording every value in one
// place: that is what makes per-worker histograms safe to combine.
func TestMergeEqualsRecordingEverythingTogether(t *testing.T) {
	all := engine.NewHistogram()
	var rows []store.SnapshotRow
	x := uint64(1) // deterministic spread of values from 1µs to 3s
	for _, w := range []string{"w1", "w2", "w3"} {
		vals := make([]int64, 500)
		for i := range vals {
			x = x*6364136223846793005 + 1442695040888963407
			vals[i] = 1 + toI64((x>>33)%3_000_000)
			_ = all.RecordValue(vals[i])
		}
		rows = append(rows, row(t, "r", w, 7, vals...))
	}
	sec, err := MergeSecond(rows)
	if err != nil {
		t.Fatal(err)
	}
	m := NewMerged()
	for _, r := range rows {
		if err := m.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	if !m.Latency().Equals(all) {
		t.Fatal("merged latency differs from recording all values together")
	}
	want := store.Percentiles{
		P50: all.ValueAtQuantile(50), P90: all.ValueAtQuantile(90), P99: all.ValueAtQuantile(99),
		P999: all.ValueAtQuantile(99.9), Max: all.Max(), LagP99: 5,
	}
	if sec.Percentiles != want {
		t.Errorf("percentiles = %+v, want %+v", sec.Percentiles, want)
	}
	if sec.SnapshotsMerged != 3 || sec.Intended != 1500 || sec.OK != 1500 || sec.IntervalNSTotal != 3*int64(time.Second) {
		t.Errorf("merged second = %+v", sec)
	}
}

// One worker is healthy, the other hit a 2s stall for 5% of its requests.
// The merged p99 is the stall; averaging the two p99s would report about
// half of it, which is the bug rule 2 forbids.
func TestMergedP99IsNotTheAverageOfP99s(t *testing.T) {
	fast := make([]int64, 1000)
	slow := make([]int64, 1000)
	for i := range fast {
		fast[i], slow[i] = 1000, 1000
	}
	for i := 0; i < 50; i++ {
		slow[i] = 2_000_000
	}
	a, b := row(t, "r", "w1", 0, fast...), row(t, "r", "w2", 0, slow...)
	sec, err := MergeSecond([]store.SnapshotRow{a, b})
	if err != nil {
		t.Fatal(err)
	}
	ma, mb := NewMerged(), NewMerged()
	_ = ma.Add(a)
	_ = mb.Add(b)
	avg := (ma.Percentiles().P99 + mb.Percentiles().P99) / 2
	if sec.P99 < 1_990_000 {
		t.Errorf("merged p99 = %dµs, want ≈ 2s", sec.P99)
	}
	if avg > 1_100_000 {
		t.Fatalf("test is not discriminating: average p99 = %dµs", avg)
	}
}

func TestMergeSecondRejectsBadInput(t *testing.T) {
	good := row(t, "r", "w1", 0, 100)
	wrongCount := row(t, "r", "w2", 0, 100)
	wrongCount.OK = 2
	corrupt := row(t, "r", "w2", 0, 100)
	corrupt.Latency = []byte("not a histogram")

	for name, rows := range map[string][]store.SnapshotRow{
		"none":              nil,
		"different seq":     {good, row(t, "r", "w2", 1, 100)},
		"different run":     {good, row(t, "q", "w2", 0, 100)},
		"same worker":       {good, good},
		"count mismatch":    {good, wrongCount},
		"corrupt histogram": {good, corrupt},
	} {
		if _, err := MergeSecond(rows); err == nil {
			t.Errorf("%s: merged without error", name)
		} else if !strings.HasPrefix(err.Error(), "aggregator:") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestSameSnapshotComparesContent(t *testing.T) {
	a := row(t, "r", "w1", 0, 100, 200)
	b := row(t, "r", "w1", 0, 100, 200)
	if !sameSnapshot(a, b) {
		t.Error("identical snapshots compare different")
	}
	c := row(t, "r", "w1", 0, 100, 300)
	if sameSnapshot(a, c) {
		t.Error("snapshots with different latencies compare equal")
	}
	d := b
	d.IntervalNS++
	if sameSnapshot(a, d) {
		t.Error("snapshots with different interval_ns compare equal")
	}
}
