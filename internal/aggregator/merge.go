// Package aggregator ingests worker snapshots from Kafka into Postgres,
// merges them per second and per run, and publishes provisional live
// progress to Redis (design.md §4.3, §4.4).
//
// Percentiles are only ever read from merged histograms, never averaged
// (CLAUDE.md rule 2).
package aggregator

import (
	"fmt"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

// Merged accumulates snapshots: summed counts and monotonic lengths, and
// merged histograms. The zero value is not usable; call NewMerged.
type Merged struct {
	N          int // snapshots added
	IntervalNS int64
	store.Counts
	latency, lag, sendDelay *hdrhistogram.Histogram
}

// NewMerged returns an empty accumulator.
func NewMerged() *Merged {
	return &Merged{latency: engine.NewHistogram(), lag: engine.NewHistogram(), sendDelay: engine.NewHistogram()}
}

// Add merges one stored snapshot. It checks the invariants the worker's
// encoder enforced, so a row that was corrupted at rest fails loudly instead
// of skewing the merge.
func (m *Merged) Add(r store.SnapshotRow) error {
	lat, lag, sd, err := decodeHists(r.Hists)
	if err != nil {
		return fmt.Errorf("aggregator: snapshot %s/%s/%d: %w", r.RunID, r.WorkerID, r.Seq, err)
	}
	if done := r.OK + r.Err + r.Timeout; lat.TotalCount() != done || sd.TotalCount() != done || lag.TotalCount() != r.Intended {
		return fmt.Errorf("aggregator: snapshot %s/%s/%d: histogram counts (%d, %d, %d) disagree with ok+err+timeout=%d, intended=%d",
			r.RunID, r.WorkerID, r.Seq, lat.TotalCount(), sd.TotalCount(), lag.TotalCount(), done, r.Intended)
	}
	if r.IntervalNS < 0 {
		return fmt.Errorf("aggregator: snapshot %s/%s/%d: negative interval_ns", r.RunID, r.WorkerID, r.Seq)
	}
	for _, p := range []struct {
		name     string
		dst, src *hdrhistogram.Histogram
	}{{"latency", m.latency, lat}, {"lag", m.lag, lag}, {"send_delay", m.sendDelay, sd}} {
		// Same range on both sides, so nothing can fall outside it.
		if dropped := p.dst.Merge(p.src); dropped != 0 {
			return fmt.Errorf("aggregator: merging %s lost %d values", p.name, dropped)
		}
	}
	m.N++
	m.IntervalNS += r.IntervalNS
	m.Counts.Add(r.Counts)
	return nil
}

func decodeHists(h store.Hists) (lat, lag, sd *hdrhistogram.Histogram, err error) {
	if lat, err = transport.DecodeHistogram(h.Latency); err != nil {
		return nil, nil, nil, fmt.Errorf("latency: %w", err)
	}
	if lag, err = transport.DecodeHistogram(h.Lag); err != nil {
		return nil, nil, nil, fmt.Errorf("lag: %w", err)
	}
	if sd, err = transport.DecodeHistogram(h.SendDelay); err != nil {
		return nil, nil, nil, fmt.Errorf("send_delay: %w", err)
	}
	return lat, lag, sd, nil
}

// Percentiles reads the merged latency histogram (and the lag p99 from the
// merged lag histogram), in microseconds. An empty histogram reads as 0.
func (m *Merged) Percentiles() store.Percentiles {
	return store.Percentiles{
		P50:    m.latency.ValueAtQuantile(50),
		P90:    m.latency.ValueAtQuantile(90),
		P99:    m.latency.ValueAtQuantile(99),
		P999:   m.latency.ValueAtQuantile(99.9),
		Max:    m.latency.Max(),
		LagP99: m.lag.ValueAtQuantile(99),
	}
}

// Latency exposes the merged latency histogram, for tests and reports.
func (m *Merged) Latency() *hdrhistogram.Histogram { return m.latency }

// Hists encodes the merged histograms for storage.
func (m *Merged) Hists() (store.Hists, error) {
	var h store.Hists
	var err error
	if h.Latency, err = transport.EncodeHistogram(m.latency); err != nil {
		return h, err
	}
	if h.Lag, err = transport.EncodeHistogram(m.lag); err != nil {
		return h, err
	}
	h.SendDelay, err = transport.EncodeHistogram(m.sendDelay)
	return h, err
}

// MergeSecond merges every worker's snapshot for one second of a run. The
// rows must share the run and seq and come from distinct workers. Workers
// start on the same barrier and rotate on the same grid, so seq k is the
// same second on every worker.
func MergeSecond(rows []store.SnapshotRow) (store.SecondRow, error) {
	if len(rows) == 0 {
		return store.SecondRow{}, fmt.Errorf("aggregator: no snapshots to merge")
	}
	m := NewMerged()
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		if r.RunID != rows[0].RunID || r.Seq != rows[0].Seq {
			return store.SecondRow{}, fmt.Errorf("aggregator: merging %s/%d with %s/%d", r.RunID, r.Seq, rows[0].RunID, rows[0].Seq)
		}
		if seen[r.WorkerID] {
			return store.SecondRow{}, fmt.Errorf("aggregator: worker %s twice in %s/%d", r.WorkerID, r.RunID, r.Seq)
		}
		seen[r.WorkerID] = true
		if err := m.Add(r); err != nil {
			return store.SecondRow{}, err
		}
	}
	h, err := m.Hists()
	if err != nil {
		return store.SecondRow{}, err
	}
	return store.SecondRow{
		RunID: rows[0].RunID, Seq: rows[0].Seq,
		SnapshotsMerged: m.N, IntervalNSTotal: m.IntervalNS,
		Counts: m.Counts, Hists: h, Percentiles: m.Percentiles(),
	}, nil
}

// snapshotRow converts a decoded snapshot for storage.
func snapshotRow(s *transport.Snapshot) (store.SnapshotRow, error) {
	r := store.SnapshotRow{
		RunID: s.RunID, WorkerID: s.WorkerID, Seq: s.Seq,
		IntervalStartNS: s.IntervalStart.UnixNano(), IntervalEndNS: s.IntervalEnd.UnixNano(),
		IntervalNS: s.IntervalNS,
		Counts: store.Counts{Intended: s.Intended, OK: s.OK, Err: s.Errors, Timeout: s.Timeouts,
			Dropped: s.Dropped, Unfinished: s.Unfinished, Clamped: s.Clamped},
		Final: s.Final, Reason: string(s.Reason),
	}
	var err error
	if r.Latency, err = transport.EncodeHistogram(s.Latency); err != nil {
		return r, err
	}
	if r.Lag, err = transport.EncodeHistogram(s.DispatchLag); err != nil {
		return r, err
	}
	r.SendDelay, err = transport.EncodeHistogram(s.SendDelay)
	return r, err
}

// sameSnapshot reports whether two stored rows hold the same snapshot,
// comparing histograms by content rather than by encoded bytes.
func sameSnapshot(a, b store.SnapshotRow) bool {
	if a.RunID != b.RunID || a.WorkerID != b.WorkerID || a.Seq != b.Seq ||
		a.IntervalStartNS != b.IntervalStartNS || a.IntervalEndNS != b.IntervalEndNS || a.IntervalNS != b.IntervalNS ||
		a.Counts != b.Counts || a.Final != b.Final || a.Reason != b.Reason {
		return false
	}
	al, ag, as, err1 := decodeHists(a.Hists)
	bl, bg, bs, err2 := decodeHists(b.Hists)
	return err1 == nil && err2 == nil && al.Equals(bl) && ag.Equals(bg) && as.Equals(bs)
}
