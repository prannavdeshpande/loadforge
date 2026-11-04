package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/transport"
)

// Publisher delivers one encoded snapshot. Calls are sequential and in seq
// order, so a publisher that writes to one partition keeps the order.
type Publisher interface {
	Publish(ctx context.Context, key, value []byte) error
}

// Rotator is the part of the engine the emitter drives.
type Rotator interface {
	Rotate() (engine.Interval, bool)
}

type EmitterConfig struct {
	Period         time.Duration // rotation period: one snapshot per period
	Buffer         int           // snapshots waiting for the publisher
	PublishTimeout time.Duration // per snapshot
}

// Emitter turns a run's intervals into snapshots with a contiguous seq and
// hands them to a Publisher (design.md §4.3).
//
// Rotation never waits on publishing. If the buffer is full or a publish
// fails, that snapshot is lost but its seq stays used, so the aggregator
// sees a gap and counts it against completeness. A seq is never reused.
type Emitter struct {
	ctx             context.Context // ends the emitter's work on a hard abort
	runID, workerID string
	cfg             EmitterConfig
	pub             Publisher

	mu      sync.Mutex // serialises seq assignment with closing out
	nextSeq uint64
	closed  bool

	out        chan outMsg
	senderDone chan struct{}
	failed     atomic.Int64

	gridStarted bool // set by Start, read by Finish; same owner
	stopGrid    chan struct{}
	gridDone    chan struct{}

	// Interval labels are wall(t0) + monotonic time since t0, so a wall
	// clock step during the run cannot make them go backwards or overlap.
	anchor     time.Time // t0, with its monotonic reading
	anchorWall time.Time // t0's wall reading only
}

type outMsg struct {
	seq   uint64
	value []byte
	// delivered is closed by the sender once this snapshot is published.
	// Only the final snapshot has one.
	delivered chan struct{}
}

// NewEmitter starts the emitter's sender. Cancelling ctx abandons any
// snapshot not yet delivered; it is for a hard abort, not a normal stop.
func NewEmitter(ctx context.Context, runID, workerID string, pub Publisher, cfg EmitterConfig) *Emitter {
	e := &Emitter{
		ctx: ctx, runID: runID, workerID: workerID, cfg: cfg, pub: pub,
		out:        make(chan outMsg, cfg.Buffer),
		senderDone: make(chan struct{}),
		stopGrid:   make(chan struct{}),
		gridDone:   make(chan struct{}),
	}
	go e.send(ctx)
	return e
}

// Start rotates r on the grid t0 + k·Period, k = 1, 2, …, until r reports
// the run finished or Finish is called. A late rotation does not skip the
// next grid point, so there is one snapshot per period and seq k ends at
// about t0 + (k+1)·Period. Start is called at most once, before Finish.
func (e *Emitter) Start(r Rotator, t0 time.Time) {
	e.gridStarted = true
	e.anchor, e.anchorWall = t0, t0.Round(0)
	go func() {
		defer close(e.gridDone)
		for k := 1; ; k++ {
			timer := time.NewTimer(time.Until(t0.Add(time.Duration(k) * e.cfg.Period)))
			select {
			case <-timer.C:
			case <-e.stopGrid:
				timer.Stop()
				return
			}
			iv, ok := r.Rotate()
			if !ok {
				return
			}
			e.emit(e.ctx, iv, "")
		}
	}()
}

// Failed is the number of snapshots that were not delivered.
func (e *Emitter) Failed() int64 { return e.failed.Load() }

// Finish stops rotation, emits final as the last snapshot with reason, and
// waits until it is delivered or ctx is done. It returns the final seq,
// which the STOP ack reports, and an error if the final snapshot was not
// delivered. Finish is called once, after the run has ended, so that final
// really is the last interval.
func (e *Emitter) Finish(ctx context.Context, final engine.Interval, reason transport.StopReason) (uint64, error) {
	close(e.stopGrid)
	if e.gridStarted {
		<-e.gridDone // no rotation can race the final snapshot's seq
	}
	seq, delivered := e.emit(ctx, final, reason)
	if delivered == nil {
		return seq, fmt.Errorf("worker: final snapshot %d was not queued", seq)
	}
	select {
	case <-delivered:
		return seq, nil
	case <-e.senderDone:
		// The sender closes delivered before it exits, so check again.
		select {
		case <-delivered:
			return seq, nil
		default:
			return seq, fmt.Errorf("worker: final snapshot %d was not delivered", seq)
		}
	case <-ctx.Done():
		return seq, fmt.Errorf("worker: final snapshot %d: %w", seq, ctx.Err())
	}
}

// emit encodes iv as the next snapshot and queues it. A reason marks the
// final snapshot, for which it returns a channel closed on delivery, or nil
// if it could not be queued.
func (e *Emitter) emit(ctx context.Context, iv engine.Interval, reason transport.StopReason) (uint64, chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		panic("worker: snapshot emitted after the final one")
	}
	seq := e.nextSeq
	e.nextSeq++
	final := reason != ""
	if final {
		e.closed = true
		defer close(e.out)
	}

	if !e.anchor.IsZero() {
		iv.Start, iv.End = e.label(iv.Start), e.label(iv.End)
	}
	value, err := transport.EncodeSnapshot(toSnapshot(e.runID, e.workerID, seq, iv, reason))
	if err != nil {
		// Not expected: the engine's intervals satisfy the invariants.
		e.failed.Add(1)
		log.Printf("worker: run %s: snapshot %d dropped: %v", e.runID, seq, err)
		return seq, nil
	}
	m := outMsg{seq: seq, value: value}

	if !final {
		select {
		case e.out <- m:
		default:
			e.failed.Add(1)
			log.Printf("worker: run %s: snapshot %d dropped: publisher is %d snapshots behind", e.runID, seq, cap(e.out))
		}
		return seq, nil
	}
	// The aggregator waits for the final snapshot, so it waits for room
	// rather than being dropped at once.
	m.delivered = make(chan struct{})
	select {
	case e.out <- m:
		return seq, m.delivered
	case <-ctx.Done():
		e.failed.Add(1)
		log.Printf("worker: run %s: final snapshot %d dropped: %v", e.runID, seq, ctx.Err())
		return seq, nil
	}
}

func (e *Emitter) label(t time.Time) time.Time {
	return e.anchorWall.Add(t.Sub(e.anchor))
}

func (e *Emitter) send(ctx context.Context) {
	defer close(e.senderDone)
	key := transport.SnapshotKey(e.runID, e.workerID)
	for m := range e.out {
		pubCtx, cancel := context.WithTimeout(ctx, e.cfg.PublishTimeout)
		err := e.pub.Publish(pubCtx, key, m.value)
		cancel()
		if err != nil {
			e.failed.Add(1)
			log.Printf("worker: run %s: snapshot %d not delivered: %v", e.runID, m.seq, err)
			continue
		}
		if m.delivered != nil {
			close(m.delivered)
		}
	}
}

// toSnapshot converts an interval. A run that never started has an empty
// interval with no histograms.
func toSnapshot(runID, workerID string, seq uint64, iv engine.Interval, reason transport.StopReason) *transport.Snapshot {
	orEmpty := func(h *hdrhistogram.Histogram) *hdrhistogram.Histogram {
		if h == nil {
			return engine.NewHistogram()
		}
		return h
	}
	return &transport.Snapshot{
		RunID: runID, WorkerID: workerID, Seq: seq,
		IntervalStart: iv.Start, IntervalEnd: iv.End,
		Latency: orEmpty(iv.Latency), SendDelay: orEmpty(iv.SendDelay), DispatchLag: orEmpty(iv.DispatchLag),
		Intended: iv.Intended, OK: iv.OK, Errors: iv.Errors, Timeouts: iv.Timeouts,
		Dropped: iv.Dropped, Unfinished: iv.Unfinished, Clamped: iv.Clamped,
		Final: reason != "", Reason: reason,
	}
}
