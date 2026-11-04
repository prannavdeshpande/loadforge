package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Doer executes one request. It must return promptly once ctx is done.
// A nil error is counted as OK; the engine decides whether a failure was a
// timeout from the state of ctx.
type Doer interface {
	Do(ctx context.Context) error
}

// Config describes one open-loop run.
type Config struct {
	Rate     float64       // requests per second, resolution 0.001
	Duration time.Duration // requests with t_i in [t0, t0+Duration) are sent

	MaxInFlight int // executors, i.e. the concurrency cap
	QueueDepth  int // requests that may wait behind the cap before generator_dropped

	RequestTimeout time.Duration // per request, measured from execution start
	DrainTimeout   time.Duration // after the last release; must be ≥ RequestTimeout
}

// Validate reports whether New would accept c.
func (c Config) Validate() error {
	switch {
	case c.Duration <= 0:
		return errors.New("engine: Duration must be positive")
	case c.MaxInFlight <= 0:
		return errors.New("engine: MaxInFlight must be positive")
	case c.QueueDepth < 0:
		return errors.New("engine: QueueDepth must not be negative")
	case c.RequestTimeout <= 0:
		return errors.New("engine: RequestTimeout must be positive")
	case c.DrainTimeout < c.RequestTimeout:
		return fmt.Errorf("engine: DrainTimeout (%v) must be at least RequestTimeout (%v)", c.DrainTimeout, c.RequestTimeout)
	}
	_, err := scaleRate(c.Rate)
	return err
}

// Engine is an open-loop load generator. The dispatcher releases request i at
// its intended time t_i whether or not earlier requests have finished, and
// latency is always measured from t_i (design.md §4).
type Engine struct {
	cfg     Config
	doer    Doer
	rec     *recorder
	started atomic.Bool

	stopOnce sync.Once
	stopCh   chan struct{}
	stopAt   time.Time // written before stopCh is closed

	// sleepUntil blocks until t or until ctx is done. Tests replace it to
	// simulate a stalled dispatcher.
	sleepUntil func(ctx context.Context, t time.Time) error
}

type job struct {
	intended time.Time
}

// Result is what Run returns.
type Result struct {
	// Total covers the whole run, including every interval already returned
	// by Rotate.
	Total Interval
	// Final is the last interval: from the last Rotate (or t0) to the end of
	// the run. It is never returned by Rotate, so a caller publishing
	// intervals must publish Final too or lose the end of the run.
	Final Interval
}

// New returns an engine for cfg. An Engine runs once.
func New(cfg Config, d Doer) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if d == nil {
		return nil, errors.New("engine: nil Doer")
	}
	return &Engine{
		cfg:        cfg,
		doer:       d,
		rec:        newRecorder(min(cfg.MaxInFlight, runtime.GOMAXPROCS(0))),
		stopCh:     make(chan struct{}),
		sleepUntil: sleepUntil,
	}, nil
}

// Rotate closes the current recording interval and returns it. It is safe to
// call concurrently with Run, which is how per-second snapshots are taken.
// The first interval starts at t0; callers should rotate at t0 + k·period.
//
// ok is false before Run has started and once Run has taken the final
// interval: there is nothing to report, and the caller must not publish an
// interval after Result.Final.
func (e *Engine) Rotate() (iv Interval, ok bool) {
	return e.rec.rotate(time.Now())
}

// Stop ends the run gracefully: the dispatcher releases every request
// intended before the moment of the call, including any it is behind on,
// then stops, and in-flight and queued requests drain up to DrainTimeout.
// Run then returns a nil error. Stop may be called at any time and more than
// once; before Run it makes Run release nothing.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		e.stopAt = time.Now()
		close(e.stopCh)
	})
}

// Run generates load starting at t0 and returns the totals for the whole
// run and its final interval. t0 must be derived from time.Now so that all
// timing uses the monotonic clock.
//
// Cancelling ctx aborts the run: dispatching stops, in-flight requests are
// cancelled and counted as Unfinished, and ctx's error is returned with the
// result.
func (e *Engine) Run(ctx context.Context, t0 time.Time) (Result, error) {
	if !e.started.CompareAndSwap(false, true) {
		return Result{}, errors.New("engine: Run called more than once")
	}
	sched := Schedule{t0: t0}
	sched.mrate, _ = scaleRate(e.cfg.Rate) // validated in New
	e.rec.start(t0)

	// execCtx outlives dispatching so in-flight requests can drain. It ends
	// when ctx is cancelled or the drain deadline passes.
	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()

	queue := make(chan job, e.cfg.QueueDepth)
	var wg sync.WaitGroup
	for k := range e.cfg.MaxInFlight {
		wg.Add(1)
		go func(s *shard) {
			defer wg.Done()
			e.execute(execCtx, queue, s)
		}(e.rec.exec[k%len(e.rec.exec)])
	}

	err := e.dispatch(ctx, sched, t0.Add(e.cfg.Duration), queue)
	close(queue)

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()
	if err == nil {
		timer := time.NewTimer(e.cfg.DrainTimeout)
		select {
		case <-drained:
		case <-timer.C:
			cancelExec()
			<-drained
		}
		timer.Stop()
	} else {
		cancelExec()
		<-drained
	}

	end := time.Now()
	if end.Before(t0) {
		// Stopped or aborted before the start barrier: an empty run.
		end = t0
	}
	final := e.rec.finish(end)
	total := e.rec.totals()
	total.Start, total.End = t0, end
	return Result{Total: total, Final: final}, err
}

// dispatch releases each request at its intended time. It never waits on a
// response: hand-off to the executors is a non-blocking send. If it falls
// behind, overdue requests are released immediately one after another and
// each records its lag; none is skipped.
//
// Stop moves end forward to the moment Stop was called. Requests intended
// before then are still released, so a dispatcher that is behind when Stop
// arrives still reports its lag instead of hiding it.
func (e *Engine) dispatch(ctx context.Context, sched Schedule, end time.Time, queue chan<- job) error {
	// sleepCtx also ends on Stop, so that a long sleep wakes up.
	sleepCtx, cancelSleep := context.WithCancel(ctx)
	defer cancelSleep()
	go func() {
		select {
		case <-e.stopCh:
			cancelSleep()
		case <-sleepCtx.Done():
		}
	}()

	d := e.rec.dispatch
	for i := uint64(0); ; i++ {
		intended := sched.At(i)
		if !intended.Before(end) {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.sleepUntil(sleepCtx, intended); err != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Stopped. stopCh is closed, so stopAt is visible.
			if e.stopAt.Before(end) {
				end = e.stopAt
			}
			sleepCtx = ctx
			if !intended.Before(end) {
				return nil
			}
			// Intended before the stop, so it is due now: release it.
		}
		lag := time.Since(intended)

		dropped := false
		select {
		case queue <- job{intended: intended}:
		default:
			dropped = true
		}

		d.mu.Lock()
		c := d.cur
		c.Intended++
		if dropped {
			c.Dropped++
		}
		if record(c.DispatchLag, lag) {
			c.Clamped++
		}
		d.mu.Unlock()
	}
}

// execute runs queued requests until the queue is closed and empty.
func (e *Engine) execute(ctx context.Context, queue <-chan job, s *shard) {
	for j := range queue {
		if ctx.Err() != nil {
			s.mu.Lock()
			s.cur.Unfinished++
			s.mu.Unlock()
			continue
		}

		start := time.Now()
		reqCtx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout)
		err := e.doer.Do(reqCtx)
		done := time.Now()
		timedOut := errors.Is(reqCtx.Err(), context.DeadlineExceeded)
		cancel()

		s.mu.Lock()
		c := s.cur
		if err != nil && ctx.Err() != nil {
			// Cut off by abort or drain deadline: it never completed.
			c.Unfinished++
		} else {
			if record(c.Latency, done.Sub(j.intended)) {
				c.Clamped++
			}
			if record(c.SendDelay, start.Sub(j.intended)) {
				c.Clamped++
			}
			switch {
			case err == nil:
				c.OK++
			case timedOut:
				c.Timeouts++
			default:
				c.Errors++
			}
		}
		s.mu.Unlock()
	}
}

func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
