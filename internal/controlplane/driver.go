package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

// errLeaseLost means this controller no longer holds the run: its lease was
// refused or a guarded write came back stale. It must stop at once, without
// any further command or write (CLAUDE.md rule 3).
var errLeaseLost = errors.New("controlplane: run lease lost")

// abortError ends a run ABORTED with verdict INVALID.
type abortError struct {
	reason string
	lost   []string // members to mark lost
	// stopSent means a STOP was already sent and its deadline passed (or
	// every stream already ended), so the abort does not send another.
	stopSent bool
}

func (e *abortError) Error() string { return "abort: " + e.reason }

func aborting(format string, args ...any) *abortError {
	return &abortError{reason: fmt.Sprintf(format, args...)}
}

// fenceLost reports whether err means the run is no longer ours.
func fenceLost(err error) bool {
	return errors.Is(err, errLeaseLost) || errors.Is(err, store.ErrStaleFence) || errors.Is(err, store.ErrStateConflict)
}

// driver drives one run from PENDING to a terminal state while holding its
// lease. Only its own goroutine changes the run; the watcher only cancels
// its context with a cause.
type driver struct {
	c       *Controller
	cfg     Config
	runID   string
	fence   uint64
	members []string
	prep    transport.Prepare
	keeper  *leaseKeeper
	hb      *heartbeater

	state       runstate.State // as last written by this driver
	cmdSeq      uint64         // highest cmd_seq used
	prepareSent bool

	// cancel cuts the run's normal course short with a cause (a lost
	// worker or lease, a heartbeat gap, shutdown). Writes use a context
	// that is never cancelled: an abort must still be recorded.
	cancel context.CancelCauseFunc

	stopReq  chan struct{}
	stopOnce sync.Once
	liveness atomic.Bool // check members' heartbeats
	done     chan struct{}
}

// newDriver returns the driver and its run context, which d.cancel
// cancels. parent is used for its values only.
func newDriver(parent context.Context, c *Controller, run store.Run, members []string, prep transport.Prepare, fence uint64, keeper *leaseKeeper) (*driver, context.Context) {
	rctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	d := &driver{
		c: c, cfg: c.cfg, runID: run.ID, fence: fence, members: members, prep: prep, keeper: keeper,
		state: run.State, cancel: cancel,
		stopReq: make(chan struct{}), done: make(chan struct{}),
	}
	d.hb = newHeartbeater(c, run.ID, fence, keeper)
	return d, rctx
}

// requestStop asks the driver to stop the run; it is safe to call often.
func (d *driver) requestStop() { d.stopOnce.Do(func() { close(d.stopReq) }) }

func (d *driver) stopRequested() bool {
	select {
	case <-d.stopReq:
		return true
	default:
		return false
	}
}

// run drives a new run and records how it ended. ctx is never cancelled;
// rctx is the run context from newDriver.
func (d *driver) run(ctx, rctx context.Context) {
	defer close(d.done)
	defer d.c.forget(d.runID, d)
	watchDone := make(chan struct{})
	go d.watch(ctx, watchDone)
	err := d.drive(ctx, rctx)
	close(watchDone)
	d.end(ctx, err)
}

// recover aborts an orphaned run whose lease this driver just took.
func (d *driver) recover(ctx context.Context) {
	defer close(d.done)
	defer d.c.forget(d.runID, d)
	d.end(ctx, d.recoverOrphan(ctx))
}

func (d *driver) recoverOrphan(ctx context.Context) error {
	// The first guarded write raises runs.fence_token to our token, so
	// the old controller, if it is only paused, can write nothing more.
	if err := d.write(ctx, func(ctx context.Context) error {
		return d.c.st.AppendEvent(ctx, d.runID, d.fence, store.Event{Kind: store.EventRecovered,
			Detail: mustJSON(map[string]any{"controller": d.cfg.InstanceID, "seen_state": d.state})})
	}); err != nil {
		return err
	}
	var run store.Run
	if err := d.read(ctx, func(ctx context.Context) (err error) { run, err = d.c.st.GetRun(ctx, d.runID); return }); err != nil {
		return err
	}
	if run.State.Terminal() {
		return nil
	}
	if err := d.read(ctx, func(ctx context.Context) (err error) { d.cmdSeq, err = d.c.st.MaxCmdSeq(ctx, d.runID); return }); err != nil {
		return err
	}
	d.state = run.State
	d.prepareSent = run.State != runstate.Pending
	return aborting("orphaned in %s: no controller held the run's lease", run.State)
}

// end records the outcome of drive or recover.
func (d *driver) end(ctx context.Context, err error) {
	var ab *abortError
	switch {
	case err == nil:
		d.hb.stop()
		d.keeper.close(ctx, true)
		return
	case fenceLost(err):
		d.giveUp(ctx, err)
		return
	case errors.As(err, &ab):
	default:
		ab = aborting("controller error: %v", err)
	}
	if aerr := d.abort(ctx, ab); aerr != nil {
		log.Printf("controlplane: run %s: abort (%s) failed: %v", d.runID, ab.reason, aerr)
		d.giveUp(ctx, aerr)
		return
	}
	d.hb.stop()
	d.keeper.close(ctx, true)
}

// giveUp stops heartbeats and drops the run at once. If the run is still
// not terminal, an orphan sweep recovers it once the lease is free.
func (d *driver) giveUp(ctx context.Context, err error) {
	log.Printf("controlplane: run %s: giving up the run: %v", d.runID, err)
	d.hb.stop()
	d.keeper.abandon(ctx, err.Error())
}

// drive takes a new run from PENDING through COMPLETED. Any error is an
// abort, a lost lease, or a failure the abort path handles.
func (d *driver) drive(ctx, rctx context.Context) error {
	if err := context.Cause(rctx); err != nil {
		return err
	}
	if d.stopRequested() {
		return aborting("stop requested before PREPARE")
	}
	if err := d.transition(ctx, runstate.Preparing, nil); err != nil {
		return err
	}
	d.liveness.Store(true)
	d.hb.start() // workers arm their dead-man's switch on PREPARE

	if err := d.prepare(ctx, rctx); err != nil {
		return err
	}
	if err := d.transition(ctx, runstate.Ready, nil); err != nil {
		return err
	}
	startMono, err := d.start(ctx, rctx)
	if err != nil {
		return err
	}
	if err := d.transition(ctx, runstate.Running, mustJSON(map[string]any{"start_at": startMono.UTC()})); err != nil {
		return err
	}

	why, err := d.waitRunning(ctx, rctx, startMono.Add(time.Duration(d.prep.DurationMS)*time.Millisecond))
	if err != nil {
		return err
	}
	if err := d.transition(ctx, runstate.Stopping, mustJSON(map[string]string{"reason": why})); err != nil {
		return err
	}
	missing, err := d.sendStop(ctx, rctx, nil, true)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		ab := aborting("%s", describeMissing(transport.CmdStop, missing, d.cfg.stopTimeout(d.drain())))
		ab.stopSent = true
		return ab
	}
	// Every worker has ended its run and closed its dead-man's switch.
	d.liveness.Store(false)
	d.hb.stop()

	if err := d.awaitResult(ctx, rctx); err != nil {
		return err
	}
	if err := d.checkStreamsEndedNormally(ctx); err != nil {
		return err
	}
	return d.complete(ctx, why)
}

func (d *driver) drain() time.Duration {
	return time.Duration(d.prep.DrainTimeoutMS) * time.Millisecond
}

// beforeStart is ctx cancelled by a stop request, which before START
// aborts the run: there is no edge to STOPPING from PREPARING or READY.
func (d *driver) beforeStart(rctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(rctx)
	go func() {
		select {
		case <-d.stopReq:
			cancel(aborting("stop requested in %s, before START", d.state))
		case <-ctx.Done():
		}
	}()
	return ctx, func() { cancel(nil) }
}

func (d *driver) prepare(ctx, rctx context.Context) error {
	fctx, done := d.beforeStart(rctx)
	defer done()
	seq, err := d.newCommand(ctx, transport.CmdPrepare)
	if err != nil {
		return err
	}
	d.prepareSent = true
	deadline := time.Now().Add(d.cfg.PrepareTimeout)
	cmd := transport.Command{Type: transport.CmdPrepare, RunID: d.runID, CmdSeq: seq, FenceToken: d.fence,
		Deadline: deadline, Prepare: &d.prep}
	var stop error
	res, err := fanout(fctx, d.c.nc, d.cfg, cmd, d.members, deadline, func(a transport.Ack) bool {
		switch a.Status {
		case transport.StatusReady, transport.StatusNack:
			if werr := d.write(ctx, func(ctx context.Context) error { return d.c.st.RecordPrepareAck(ctx, d.runID, d.fence, a) }); werr != nil {
				stop = werr
			} else if a.Status == transport.StatusNack {
				stop = aborting("worker %s refused PREPARE: %s", a.WorkerID, a.Detail)
			}
		default:
			stop = d.violation(ctx, a, "PREPARE acked with status %q", a.Status)
		}
		return stop != nil
	})
	switch {
	case stop != nil:
		return stop
	case err != nil:
		return err
	case len(res.missing) > 0:
		return aborting("%s", describeMissing(transport.CmdPrepare, res.missing, d.cfg.PrepareTimeout))
	}
	return nil
}

// start sends START and returns t0 on the monotonic clock.
func (d *driver) start(ctx, rctx context.Context) (time.Time, error) {
	fctx, done := d.beforeStart(rctx)
	defer done()
	seq, err := d.newCommand(ctx, transport.CmdStart)
	if err != nil {
		return time.Time{}, err
	}
	// One reading gives both clocks: start_at is the wall-clock barrier the
	// workers use, startMono the same instant on this host's monotonic clock.
	startMono := time.Now().Add(d.cfg.StartLead)
	ackBy := startMono.Add(-d.cfg.StartAckMargin)
	cmd := transport.Command{Type: transport.CmdStart, RunID: d.runID, CmdSeq: seq, FenceToken: d.fence,
		Deadline: ackBy, Start: &transport.Start{StartAt: startMono}}
	var stop error
	res, err := fanout(fctx, d.c.nc, d.cfg, cmd, d.members, ackBy, func(a transport.Ack) bool {
		switch a.Status {
		case transport.StatusArmed, transport.StatusNack:
			if werr := d.logAck(ctx, a); werr != nil {
				stop = werr
			} else if a.Status == transport.StatusNack {
				stop = aborting("worker %s refused START: %s", a.WorkerID, a.Detail)
			}
		default:
			stop = d.violation(ctx, a, "START acked with status %q", a.Status)
		}
		return stop != nil
	})
	switch {
	case stop != nil:
		return time.Time{}, stop
	case err != nil:
		return time.Time{}, err
	case len(res.missing) > 0:
		return time.Time{}, aborting("%s", describeMissing(transport.CmdStart, res.missing, d.cfg.StartLead-d.cfg.StartAckMargin))
	}
	return startMono, nil
}

// waitRunning waits for the run's end on the monotonic clock or a stop
// request, and says which.
func (d *driver) waitRunning(ctx, rctx context.Context, end time.Time) (string, error) {
	t := time.NewTimer(time.Until(end))
	defer t.Stop()
	select {
	case <-t.C:
		return "duration elapsed", nil
	case <-d.stopReq:
		if err := d.write(ctx, func(ctx context.Context) error {
			return d.c.st.AppendEvent(ctx, d.runID, d.fence, store.Event{Kind: store.EventStopRequested})
		}); err != nil {
			return "", err
		}
		return "stop requested", nil
	case <-rctx.Done():
		return "", context.Cause(rctx)
	}
}

// sendStop sends STOP to every member except skip and records the acks.
// strict treats a NACK as an abort; an abort's own STOP accepts NACKs from
// workers that never prepared. It returns the members that did not ack.
func (d *driver) sendStop(ctx, fctx context.Context, skip []string, strict bool) ([]string, error) {
	targets := without(d.members, skip)
	if len(targets) == 0 {
		return nil, nil
	}
	seq, err := d.newCommand(ctx, transport.CmdStop)
	if err != nil {
		return nil, err
	}
	wait := d.cfg.stopTimeout(d.drain())
	deadline := time.Now().Add(wait)
	cmd := transport.Command{Type: transport.CmdStop, RunID: d.runID, CmdSeq: seq, FenceToken: d.fence, Deadline: deadline}
	var stop error
	res, err := fanout(fctx, d.c.nc, d.cfg, cmd, targets, deadline, func(a transport.Ack) bool {
		switch a.Status {
		case transport.StatusStopped:
			werr := d.write(ctx, func(ctx context.Context) error { return d.c.st.RecordStopAck(ctx, d.runID, d.fence, a) })
			// A contradicting final_seq is already logged as a protocol
			// violation, which makes the verdict INVALID; keep collecting.
			if werr != nil && !errors.Is(werr, store.ErrProtocolViolation) {
				stop = werr
			}
		case transport.StatusNack:
			if werr := d.logAck(ctx, a); werr != nil {
				stop = werr
			} else if strict {
				stop = aborting("worker %s refused STOP: %s", a.WorkerID, a.Detail)
			}
		default:
			stop = d.violation(ctx, a, "STOP acked with status %q", a.Status)
		}
		return stop != nil
	})
	if stop != nil {
		return nil, stop
	}
	if err != nil {
		return nil, err
	}
	return res.missing, nil
}

// awaitResult waits for the aggregator's result, setting
// finalize_requested once FinalizeWait has passed. It never completes a run
// without a result: an aggregator outage delays the verdict, it does not
// change it (design.md §8.4).
func (d *driver) awaitResult(ctx, rctx context.Context) error {
	forceAt := time.Now().Add(d.cfg.FinalizeWait)
	forced := false
	t := time.NewTicker(d.cfg.FinalizePoll)
	defer t.Stop()
	for {
		var err error
		_ = d.read(ctx, func(ctx context.Context) error { _, err = d.c.st.GetResult(ctx, d.runID); return nil })
		switch {
		case err == nil:
			return nil
		case !errors.Is(err, store.ErrNotFound):
			log.Printf("controlplane: run %s: reading result: %v", d.runID, err)
		case !forced && time.Now().After(forceAt):
			log.Printf("controlplane: run %s: no result after %v, requesting finalization", d.runID, d.cfg.FinalizeWait)
			if err := d.write(ctx, func(ctx context.Context) error { return d.c.st.RequestFinalize(ctx, d.runID, d.fence) }); err != nil {
				return err
			}
			forced = true
		}
		select {
		case <-t.C:
		case <-d.keeper.Lost():
			return errLeaseLost
		case <-rctx.Done():
			return context.Cause(rctx)
		}
	}
}

// checkStreamsEndedNormally aborts a run in which a worker's stream ended
// because of its dead-man's switch or a shutdown: that worker stopped
// generating load early, so the run did not measure what was asked
// (design.md §5).
func (d *driver) checkStreamsEndedNormally(ctx context.Context) error {
	var reasons map[string]transport.StopReason
	if err := d.read(ctx, func(ctx context.Context) (err error) { reasons, err = d.c.st.FinalReasons(ctx, d.runID); return }); err != nil {
		return err
	}
	for _, w := range d.members {
		if r := reasons[w]; r == transport.ReasonDeadman || r == transport.ReasonShutdown {
			ab := aborting("worker %s stopped early (final snapshot reason %s)", w, r)
			ab.stopSent = true
			return ab
		}
	}
	return nil
}

// complete computes the verdict and records COMPLETED in one fenced write.
func (d *driver) complete(ctx context.Context, why string) error {
	var (
		in  health.Inputs
		th  health.Thresholds
		res health.Result
	)
	if err := d.read(ctx, func(ctx context.Context) (err error) { in, th, err = d.c.st.VerdictInputs(ctx, d.runID); return }); err != nil {
		return err
	}
	res, err := health.Evaluate(in, th)
	if err != nil {
		return err
	}
	return d.write(ctx, func(ctx context.Context) error {
		err := d.c.st.Transition(ctx, store.Transition{RunID: d.runID, Fence: d.fence, From: d.state, To: runstate.Completed,
			Verdict: res.Verdict, VerdictDetail: res.Detail(), Detail: mustJSON(map[string]string{"reason": why})})
		if err == nil {
			d.state = runstate.Completed
		}
		return err
	})
}

// abort stops the run's workers (unless that already happened), marks lost
// workers, asks the aggregator to finalize what it has, and records
// ABORTED with verdict INVALID.
func (d *driver) abort(ctx context.Context, ab *abortError) error {
	log.Printf("controlplane: run %s: aborting from %s: %s", d.runID, d.state, ab.reason)
	if d.prepareSent && !ab.stopSent {
		fctx, cancel := context.WithTimeout(ctx, d.cfg.stopTimeout(d.drain())+d.cfg.StoreTimeout)
		missing, err := d.sendStop(ctx, fctx, ab.lost, false)
		cancel()
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			log.Printf("controlplane: run %s: %s", d.runID, describeMissing(transport.CmdStop, missing, d.cfg.stopTimeout(d.drain())))
		}
	}
	d.liveness.Store(false)
	d.hb.stop()
	for _, w := range ab.lost {
		if err := d.write(ctx, func(ctx context.Context) error {
			return d.c.st.MarkWorkerLost(ctx, d.runID, d.fence, w, mustJSON(map[string]string{"reason": ab.reason}))
		}); err != nil {
			return err
		}
	}
	if d.prepareSent {
		if err := d.write(ctx, func(ctx context.Context) error { return d.c.st.RequestFinalize(ctx, d.runID, d.fence) }); err != nil {
			return err
		}
	}
	detail := d.abortedVerdict(ctx, ab.reason)
	return d.write(ctx, func(ctx context.Context) error {
		err := d.c.st.Transition(ctx, store.Transition{RunID: d.runID, Fence: d.fence, From: d.state, To: runstate.Aborted,
			Verdict: health.Invalid, VerdictDetail: detail, Detail: mustJSON(map[string]string{"reason": ab.reason})})
		if err == nil {
			d.state = runstate.Aborted
		}
		return err
	})
}

// abortedVerdict is the verdict_detail of an aborted run: the full health
// result with Aborted set, plus why.
func (d *driver) abortedVerdict(ctx context.Context, reason string) json.RawMessage {
	fallback := mustJSON(map[string]string{"verdict": string(health.Invalid), "abort_reason": reason})
	var (
		in health.Inputs
		th health.Thresholds
	)
	if err := d.read(ctx, func(ctx context.Context) (err error) { in, th, err = d.c.st.VerdictInputs(ctx, d.runID); return }); err != nil {
		return fallback
	}
	in.Aborted = true
	res, err := health.Evaluate(in, th)
	if err != nil || res.Verdict != health.Invalid {
		return fallback
	}
	var m map[string]any
	if json.Unmarshal(res.Detail(), &m) != nil {
		return fallback
	}
	m["abort_reason"] = reason
	return mustJSON(m)
}

// transition moves the run forward from its current state.
func (d *driver) transition(ctx context.Context, to runstate.State, detail json.RawMessage) error {
	return d.write(ctx, func(ctx context.Context) error {
		err := d.c.st.Transition(ctx, store.Transition{RunID: d.runID, Fence: d.fence, From: d.state, To: to, Detail: detail})
		if err == nil {
			d.state = to
		}
		return err
	})
}

// newCommand takes the next cmd_seq and logs it, fenced, before the command
// is sent, so a later controller never reuses it (design.md §3.3).
func (d *driver) newCommand(ctx context.Context, typ transport.CmdType) (uint64, error) {
	seq := d.cmdSeq + 1
	if err := d.write(ctx, func(ctx context.Context) error {
		return d.c.st.AppendEvent(ctx, d.runID, d.fence, store.Event{Kind: store.EventCommand, CmdSeq: seq,
			Detail: mustJSON(map[string]string{"type": string(typ)})})
	}); err != nil {
		return 0, err
	}
	d.cmdSeq = seq
	return seq, nil
}

func (d *driver) logAck(ctx context.Context, a transport.Ack) error {
	kind := store.EventAck
	if a.Status == transport.StatusNack {
		kind = store.EventNack
	}
	return d.write(ctx, func(ctx context.Context) error {
		return d.c.st.AppendEvent(ctx, d.runID, d.fence, store.Event{Kind: kind, WorkerID: a.WorkerID, CmdSeq: a.CmdSeq,
			Detail: mustJSON(map[string]string{"status": string(a.Status), "detail": a.Detail})})
	})
}

// violation logs a protocol violation (which makes the verdict INVALID)
// and returns the abort it causes.
func (d *driver) violation(ctx context.Context, a transport.Ack, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if err := d.write(ctx, func(ctx context.Context) error {
		return d.c.st.AppendEvent(ctx, d.runID, d.fence, store.Event{Kind: store.EventProtocolViolation, WorkerID: a.WorkerID,
			CmdSeq: a.CmdSeq, Detail: mustJSON(map[string]string{"detail": msg})})
	}); err != nil {
		return err
	}
	return aborting("worker %s: %s", a.WorkerID, msg)
}

// write runs a guarded store write. Writes are not tied to the run context: an abort
// after shutdown or a lost worker must still be recorded.
func (d *driver) write(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, d.cfg.StoreTimeout)
	defer cancel()
	err := fn(ctx)
	if errors.Is(err, store.ErrStaleFence) {
		return fmt.Errorf("%w: %w", errLeaseLost, err)
	}
	return err
}

func (d *driver) read(ctx context.Context, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, d.cfg.StoreTimeout)
	defer cancel()
	return fn(ctx)
}

// watch cancels the driver when its lease is lost, a member goes silent,
// heartbeats have lapsed long enough that workers may have tripped, or the
// controller shuts down; and it polls Postgres for stop requests made
// through other instances.
func (d *driver) watch(ctx context.Context, done <-chan struct{}) {
	t := time.NewTicker(d.cfg.LivenessCheck)
	defer t.Stop()
	lastPoll := time.Now()
	for {
		select {
		case <-done:
			return
		case <-d.keeper.Lost():
			d.cancel(errLeaseLost)
			return
		case <-t.C:
		}
		if d.liveness.Load() {
			if lost := d.silentMembers(); len(lost) > 0 {
				d.cancel(&abortError{reason: fmt.Sprintf("worker(s) %v lost: no heartbeat for %v", lost, d.cfg.WorkerSilence), lost: lost})
				return
			}
		}
		if since, on := d.hb.sinceLast(); on && since >= d.cfg.heartbeatGap() {
			d.cancel(aborting("no controller heartbeat published for %v: workers may have tripped their dead-man's switch", since.Round(time.Millisecond)))
			return
		}
		if time.Since(lastPoll) >= d.cfg.StopPoll && !d.stopRequested() {
			lastPoll = time.Now()
			var run store.Run
			if err := d.read(ctx, func(ctx context.Context) (err error) { run, err = d.c.st.GetRun(ctx, d.runID); return }); err == nil && run.StopRequested {
				d.requestStop()
			}
		}
	}
}

// silentMembers are members not heard from within WorkerSilence, or that
// report another run.
func (d *driver) silentMembers() []string {
	var lost []string
	for _, w := range d.members {
		at, runID, ok := d.c.reg.Last(w)
		if !ok || time.Since(at) >= d.cfg.WorkerSilence || (runID != "" && runID != d.runID) {
			lost = append(lost, w)
		}
	}
	return lost
}

// heartbeater publishes lf.run.<id>.hb while the lease gate is open.
type heartbeater struct {
	c      *Controller
	runID  string
	fence  uint64
	keeper *leaseKeeper

	mu      sync.Mutex
	running bool
	last    time.Time // last successful publish, monotonic
	stopCh  chan struct{}
	done    chan struct{}
}

func newHeartbeater(c *Controller, runID string, fence uint64, keeper *leaseKeeper) *heartbeater {
	return &heartbeater{c: c, runID: runID, fence: fence, keeper: keeper}
}

func (h *heartbeater) start() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.running || h.stopCh != nil {
		return
	}
	h.running, h.last = true, time.Now()
	h.stopCh, h.done = make(chan struct{}), make(chan struct{})
	go h.loop()
}

func (h *heartbeater) loop() {
	defer close(h.done)
	b := mustJSON(transport.Heartbeat{RunID: h.runID, FenceToken: h.fence})
	t := time.NewTicker(h.c.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		if h.keeper.gateOpen() {
			if err := h.c.nc.Publish(transport.RunHeartbeatSubject(h.runID), b); err == nil {
				h.mu.Lock()
				h.last = time.Now()
				h.mu.Unlock()
			}
		}
		select {
		case <-h.stopCh:
			return
		case <-t.C:
		}
	}
}

// stop ends heartbeats; it returns once no more can be published.
func (h *heartbeater) stop() {
	h.mu.Lock()
	if !h.running {
		h.mu.Unlock()
		return
	}
	h.running = false
	close(h.stopCh)
	done := h.done
	h.mu.Unlock()
	<-done
}

// sinceLast is the time since the last published heartbeat, and whether
// heartbeats are on.
func (h *heartbeater) sinceLast() (time.Duration, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Since(h.last), h.running
}

func without(all, skip []string) []string {
	if len(skip) == 0 {
		return all
	}
	drop := make(map[string]bool, len(skip))
	for _, s := range skip {
		drop[s] = true
	}
	var out []string
	for _, a := range all {
		if !drop[a] {
			out = append(out, a)
		}
	}
	return out
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
