package engine

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Interval is everything recorded between two rotations, or over a whole run
// when returned by Engine.Run. All histograms are in microseconds.
//
// Once every executor has finished, the counts satisfy
// OK + Errors + Timeouts + Dropped + Unfinished == Intended.
type Interval struct {
	Start, End time.Time

	// Latency is completion time − intended send time, for every completed
	// request whatever its outcome (ok, error or timeout).
	Latency *hdrhistogram.Histogram
	// SendDelay is execution start − intended send time: dispatch lag plus
	// time spent queued behind the in-flight cap.
	SendDelay *hdrhistogram.Histogram
	// DispatchLag is release by the dispatcher − intended send time. It
	// measures only the scheduler's own health.
	DispatchLag *hdrhistogram.Histogram

	Intended   int64 // released by the dispatcher
	OK         int64
	Errors     int64
	Timeouts   int64
	Dropped    int64 // generator_dropped: the local queue was full
	Unfinished int64 // queued or in flight when the run was cut off
	Clamped    int64 // values above the histogram range, recorded as 60s
}

func newInterval() *Interval {
	return &Interval{Latency: NewHistogram(), SendDelay: NewHistogram(), DispatchLag: NewHistogram()}
}

// merge adds o into iv. Histograms that are nil in o are skipped.
func (iv *Interval) merge(o *Interval) {
	for _, p := range [][2]*hdrhistogram.Histogram{
		{iv.Latency, o.Latency}, {iv.SendDelay, o.SendDelay}, {iv.DispatchLag, o.DispatchLag},
	} {
		if p[1] != nil {
			p[0].Merge(p[1]) // same range on both sides, so nothing is dropped
		}
	}
	iv.Intended += o.Intended
	iv.OK += o.OK
	iv.Errors += o.Errors
	iv.Timeouts += o.Timeouts
	iv.Dropped += o.Dropped
	iv.Unfinished += o.Unfinished
	iv.Clamped += o.Clamped
}

func (iv *Interval) reset() {
	for _, h := range []*hdrhistogram.Histogram{iv.Latency, iv.SendDelay, iv.DispatchLag} {
		if h != nil {
			h.Reset()
		}
	}
	*iv = Interval{Latency: iv.Latency, SendDelay: iv.SendDelay, DispatchLag: iv.DispatchLag}
}

// shard is double-buffered: writers record into cur under mu, and rotation
// swaps cur with spare, so the lock is held only for the swap. mu is
// uncontended except for executors sharing a shard and the once-per-interval
// swap.
type shard struct {
	mu         sync.Mutex
	cur, spare *Interval
}

func newShard(mk func() *Interval) *shard {
	return &shard{cur: mk(), spare: mk()}
}

// recorder holds one shard for the dispatcher and a set of shards shared by
// the executors.
type recorder struct {
	dispatch *shard
	exec     []*shard

	rotateMu sync.Mutex
	open     bool // between start and finish
	last     time.Time
	total    *Interval
}

func newRecorder(execShards int) *recorder {
	r := &recorder{
		dispatch: newShard(func() *Interval { return &Interval{DispatchLag: NewHistogram()} }),
		total:    newInterval(),
	}
	for range execShards {
		r.exec = append(r.exec, newShard(func() *Interval {
			return &Interval{Latency: NewHistogram(), SendDelay: NewHistogram()}
		}))
	}
	return r
}

// start opens the first interval at t0. Nothing is recorded before start.
func (r *recorder) start(t0 time.Time) {
	r.rotateMu.Lock()
	defer r.rotateMu.Unlock()
	r.last, r.open = t0, true
}

// rotate closes the current interval at now and returns it. Every value
// recorded before the swap of a shard lands in exactly one interval. ok is
// false outside start..finish.
func (r *recorder) rotate(now time.Time) (iv Interval, ok bool) {
	r.rotateMu.Lock()
	defer r.rotateMu.Unlock()
	if !r.open {
		return Interval{}, false
	}
	return r.rotateLocked(now), true
}

// finish closes the last interval at now and returns it. Later rotations
// report nothing.
func (r *recorder) finish(now time.Time) Interval {
	r.rotateMu.Lock()
	defer r.rotateMu.Unlock()
	iv := r.rotateLocked(now)
	r.open = false
	return iv
}

func (r *recorder) rotateLocked(now time.Time) Interval {
	out := newInterval()
	out.Start, out.End = r.last, now
	r.last = now
	for _, s := range append([]*shard{r.dispatch}, r.exec...) {
		s.mu.Lock()
		s.cur, s.spare = s.spare, s.cur
		s.mu.Unlock()
		// Only the rotator touches spare, and rotations are serialised.
		out.merge(s.spare)
		s.spare.reset()
	}
	r.total.merge(out)
	return *out
}

// totals returns a copy of everything rotated so far.
func (r *recorder) totals() Interval {
	r.rotateMu.Lock()
	defer r.rotateMu.Unlock()
	out := newInterval()
	out.merge(r.total)
	return *out
}
