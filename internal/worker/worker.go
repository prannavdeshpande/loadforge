// Package worker runs open-loop load for one run at a time on the
// controller's NATS commands and publishes per-second HDR snapshots.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/loadforge/loadforge/internal/engine"
	"github.com/loadforge/loadforge/internal/safety"
	"github.com/loadforge/loadforge/internal/transport"
)

type Config struct {
	ID      string
	RateCap safety.RateCap

	// Zero values take the defaults below. Tests shorten them.
	DeadManTimeout    time.Duration // safety.DeadManTimeout
	HeartbeatInterval time.Duration // safety.HeartbeatInterval
	SnapshotPeriod    time.Duration // 1s
	SnapshotBuffer    int           // 60 snapshots
	PublishTimeout    time.Duration // 10s per snapshot
	FinalTimeout      time.Duration // 30s for the final snapshot to be delivered
}

func (c *Config) setDefaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	set(&c.DeadManTimeout, safety.DeadManTimeout)
	set(&c.HeartbeatInterval, safety.HeartbeatInterval)
	set(&c.SnapshotPeriod, time.Second)
	set(&c.PublishTimeout, 10*time.Second)
	set(&c.FinalTimeout, 30*time.Second)
	if c.SnapshotBuffer == 0 {
		c.SnapshotBuffer = 60
	}
}

// keptRuns is how many ended runs a worker remembers, so that repeated
// commands for them still get their cached ack.
const keptRuns = 32

type phase int

const (
	phaseRefused  phase = iota // PREPARE was NACKed
	phasePrepared              // engine built, dead-man armed
	phaseRunning               // START accepted: Run is running or waiting for t0
	phaseStopping              // stop requested; waiting for drain and final snapshot
	phaseDone                  // final snapshot handled
)

type run struct {
	id      string
	fence   uint64               // highest fence token seen
	lastSeq uint64               // highest cmd_seq handled
	acks    map[uint64]cachedAck // by cmd_seq, for repeats

	phase   phase
	reason  transport.StopReason // why it stopped; first cause wins
	eng     *engine.Engine
	httpTr  *http.Transport
	warm    *warmPool
	deadman *safety.DeadMan
	hbSub   *nats.Subscription

	waiters  []waiter // STOPs to answer once done
	finalSeq uint64
	finalErr error // the final snapshot was not delivered
	done     chan struct{}
}

type cachedAck struct {
	typ transport.CmdType
	ack []byte
}

type waiter struct {
	reply  string
	cmdSeq uint64
}

// Worker handles the commands of design.md §6 for one run at a time.
type Worker struct {
	cfg Config
	nc  *nats.Conn
	pub Publisher

	// ctx ends only on a hard abort, when Close runs out of time.
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	runs    map[string]*run
	order   []string // run IDs, oldest first
	active  *run     // prepared, running or stopping
	closing bool

	cmdSub    *nats.Subscription
	hbStop    chan struct{}
	hbDone    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// New subscribes to every run's command subject and starts the worker's
// heartbeat. It does not take ownership of nc or pub.
func New(cfg Config, nc *nats.Conn, pub Publisher) (*Worker, error) {
	if err := transport.ValidateID("worker_id", cfg.ID); err != nil {
		return nil, err
	}
	cfg.setDefaults()
	if cfg.DeadManTimeout >= safety.LeaseTTL {
		return nil, fmt.Errorf("worker: dead-man timeout %v must be below the lease TTL %v", cfg.DeadManTimeout, safety.LeaseTTL)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		cfg: cfg, nc: nc, pub: pub, ctx: ctx, cancel: cancel,
		runs:   map[string]*run{},
		hbStop: make(chan struct{}),
		hbDone: make(chan struct{}),
	}
	sub, err := nc.Subscribe(transport.CmdSubjectAll, w.handle)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("worker: subscribe to commands: %w", err)
	}
	w.cmdSub = sub
	go w.heartbeat()
	return w, nil
}

// Close stops the active run gracefully, with reason shutdown, and waits for
// its final snapshot. If ctx ends first, the run is aborted: in-flight
// requests are cancelled and counted as unfinished. Later calls return the
// first call's result.
func (w *Worker) Close(ctx context.Context) error {
	w.closeOnce.Do(func() { w.closeErr = w.close(ctx) })
	return w.closeErr
}

func (w *Worker) close(ctx context.Context) error {
	w.mu.Lock()
	w.closing = true
	r := w.active
	if r != nil {
		w.stopRun(ctx, r, transport.ReasonShutdown)
	}
	w.mu.Unlock()

	var err error
	if r != nil {
		select {
		case <-r.done:
		case <-ctx.Done():
			err = fmt.Errorf("worker: run %s aborted at shutdown: %w", r.id, ctx.Err())
			w.cancel()
			<-r.done
		}
	}
	w.cancel()
	err = errors.Join(err, w.cmdSub.Unsubscribe())
	close(w.hbStop)
	<-w.hbDone
	return err
}

func (w *Worker) heartbeat() {
	defer close(w.hbDone)
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-w.hbStop:
			return
		}
		hb := transport.WorkerHeartbeat{WorkerID: w.cfg.ID}
		w.mu.Lock()
		if w.active != nil {
			hb.RunID = w.active.id
		}
		w.mu.Unlock()
		b, _ := json.Marshal(hb)
		if err := w.nc.Publish(transport.WorkerHeartbeatSubject(w.cfg.ID), b); err != nil {
			log.Printf("worker %s: heartbeat: %v", w.cfg.ID, err)
		}
	}
}

// handle runs on the command subscription's goroutine. It never blocks on a
// run: anything slow happens elsewhere and answers later.
func (w *Worker) handle(msg *nats.Msg) {
	var c transport.Command
	if err := json.Unmarshal(msg.Data, &c); err != nil {
		log.Printf("worker %s: undecodable command on %s: %v", w.cfg.ID, msg.Subject, err)
		return
	}
	if msg.Subject != transport.CmdSubject(c.RunID) {
		log.Printf("worker %s: command for run %q on subject %s ignored", w.cfg.ID, c.RunID, msg.Subject)
		return
	}

	// Warm-up dials the target, so it runs before taking w.mu. Commands
	// arrive one at a time on this subscription, so nothing else can claim
	// the worker meanwhile. A pool that prepare does not adopt is closed.
	warm, warmErr := w.warmUpFor(&c)
	defer func() {
		if warm != nil {
			warm.Close()
		}
	}()

	w.mu.Lock()
	defer w.mu.Unlock()
	r := w.runs[c.RunID]
	member := r != nil || (c.Type == transport.CmdPrepare && c.Prepare != nil && c.Prepare.Lists(w.cfg.ID))
	if !member {
		return // not our run: stay silent
	}
	if err := c.Validate(); err != nil {
		w.reply(msg, nil, &c, w.nack(&c, err.Error()))
		return
	}

	if r == nil {
		r = w.addRun(c.RunID)
	} else {
		if c.FenceToken < r.fence {
			log.Printf("worker %s: run %s: %s with stale fence %d < %d ignored", w.cfg.ID, r.id, c.Type, c.FenceToken, r.fence)
			return
		}
		if prev, ok := r.acks[c.CmdSeq]; ok {
			if prev.typ != c.Type {
				log.Printf("worker %s: run %s: protocol violation: cmd_seq %d was %s, now %s; ignored",
					w.cfg.ID, r.id, c.CmdSeq, prev.typ, c.Type)
				return
			}
			if err := msg.Respond(prev.ack); err != nil {
				log.Printf("worker %s: run %s: re-ack %d: %v", w.cfg.ID, r.id, c.CmdSeq, err)
			}
			return
		}
		if c.CmdSeq < r.lastSeq {
			log.Printf("worker %s: run %s: %s cmd_seq %d < %d ignored", w.cfg.ID, r.id, c.Type, c.CmdSeq, r.lastSeq)
			return
		}
	}
	r.fence = max(r.fence, c.FenceToken)
	r.lastSeq = c.CmdSeq
	if r.deadman != nil {
		r.deadman.Observe(r.fence)
	}

	switch c.Type {
	case transport.CmdPrepare:
		if w.prepare(msg, r, &c, warm, warmErr) {
			warm = nil // the run owns it now
		}
	case transport.CmdStart:
		w.start(msg, r, &c)
	case transport.CmdRate:
		w.reply(msg, r, &c, w.nack(&c, "unsupported"))
	case transport.CmdStop:
		w.stop(msg, r, &c)
	}
}

// engineConfig checks a PREPARE against this worker's limits.
func (w *Worker) engineConfig(p *transport.Prepare) (engine.Config, error) {
	if err := w.cfg.RateCap.Check(p.Rate); err != nil {
		return engine.Config{}, err
	}
	cfg := engine.Config{
		Rate:           p.Rate,
		Duration:       time.Duration(p.DurationMS) * time.Millisecond,
		MaxInFlight:    p.MaxInFlight,
		QueueDepth:     p.QueueDepth,
		RequestTimeout: time.Duration(p.RequestTimeoutMS) * time.Millisecond,
		DrainTimeout:   time.Duration(p.DrainTimeoutMS) * time.Millisecond,
	}
	return cfg, cfg.Validate()
}

// warmUpFor opens the warm connections for a PREPARE this worker would
// accept: listed, valid, not busy and within limits. Anything else is left
// to prepare to refuse, without having contacted the target. It returns
// (nil, nil) when there is nothing to warm. w.mu must not be held.
func (w *Worker) warmUpFor(c *transport.Command) (*warmPool, error) {
	if c.Type != transport.CmdPrepare || c.Prepare == nil || !c.Prepare.Lists(w.cfg.ID) || c.Validate() != nil {
		return nil, nil
	}
	w.mu.Lock()
	idle := w.runs[c.RunID] == nil && w.active == nil && !w.closing
	w.mu.Unlock()
	if !idle {
		return nil, nil
	}
	if _, err := w.engineConfig(c.Prepare); err != nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(w.ctx, warmTimeout(c.Deadline))
	defer cancel()
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	return warmUp(ctx, c.Prepare.TargetURL, min(c.Prepare.MaxInFlight, maxWarmConns), dialer)
}

// prepare handles PREPARE. It reports whether the run adopted warm.
func (w *Worker) prepare(msg *nats.Msg, r *run, c *transport.Command, warm *warmPool, warmErr error) bool {
	refuse := func(detail string) bool {
		w.reply(msg, r, c, w.nack(c, detail))
		return false
	}
	switch {
	case r.eng != nil:
		return refuse("already prepared")
	case w.closing:
		return refuse("worker is shutting down")
	case w.active != nil:
		return refuse(fmt.Sprintf("busy with run %s", w.active.id))
	}
	p := c.Prepare
	cfg, err := w.engineConfig(p)
	if err != nil {
		return refuse(err.Error())
	}
	if warmErr != nil {
		return refuse(warmErr.Error())
	}
	if warm == nil {
		return refuse("warm-up did not run")
	}
	// No proxy: the warm connections go straight to the target, and so must
	// the requests that use them.
	tr := &http.Transport{
		DialContext:         warm.DialContext,
		MaxIdleConns:        p.MaxInFlight,
		MaxIdleConnsPerHost: p.MaxInFlight,
		IdleConnTimeout:     90 * time.Second,
	}
	eng, err := engine.New(cfg, &engine.HTTPDoer{Client: &http.Client{Transport: tr}, Method: p.Method, URL: p.TargetURL})
	if err != nil {
		return refuse(err.Error())
	}

	dm := safety.NewDeadMan(w.cfg.DeadManTimeout, r.fence, func() { w.deadmanTripped(r) })
	sub, err := w.nc.Subscribe(transport.RunHeartbeatSubject(r.id), func(m *nats.Msg) {
		var hb transport.Heartbeat
		if json.Unmarshal(m.Data, &hb) == nil && hb.RunID == r.id {
			dm.Beat(hb.FenceToken)
		}
	})
	if err != nil {
		dm.Close()
		return refuse(fmt.Sprintf("subscribe to heartbeats: %v", err))
	}
	r.eng, r.httpTr, r.warm, r.deadman, r.hbSub = eng, tr, warm, dm, sub
	r.phase = phasePrepared
	w.active = r
	log.Printf("worker %s: run %s prepared, %d warm connections to %s", w.cfg.ID, r.id, warm.size(), warm.addr)
	w.reply(msg, r, c, w.ack(c, transport.StatusReady))
	return true
}

func (w *Worker) start(msg *nats.Msg, r *run, c *transport.Command) {
	if r.phase != phasePrepared {
		w.reply(msg, r, c, w.nack(c, "not in the prepared state"))
		return
	}
	// The barrier is the only wall-clock input: turn it into a monotonic
	// t0 once, on receipt.
	d := time.Until(c.Start.StartAt)
	if d <= 0 {
		w.reply(msg, r, c, w.nack(c, fmt.Sprintf("start_at passed %v ago", -d)))
		return
	}
	t0 := time.Now().Add(d)
	r.phase = phaseRunning
	go w.execute(r, t0)
	w.reply(msg, r, c, w.ack(c, transport.StatusArmed))
}

func (w *Worker) stop(msg *nats.Msg, r *run, c *transport.Command) {
	switch r.phase {
	case phaseRefused:
		w.reply(msg, r, c, w.nack(c, "not prepared"))
		return
	case phaseDone:
		w.reply(msg, r, c, w.stoppedAck(r, c.CmdSeq))
		return
	}
	w.stopRun(w.ctx, r, transport.ReasonStop)
	r.waiters = append(r.waiters, waiter{reply: msg.Reply, cmdSeq: c.CmdSeq})
}

// stopRun asks r to end with reason. The first cause is the one reported.
// ctx bounds publishing the final snapshot of a run that never started.
// w.mu must be held.
func (w *Worker) stopRun(ctx context.Context, r *run, reason transport.StopReason) {
	switch r.phase {
	case phasePrepared:
		r.reason = reason
		r.phase = phaseStopping
		go w.finishUnstarted(ctx, r)
	case phaseRunning:
		r.reason = reason
		r.phase = phaseStopping
		r.eng.Stop()
	}
}

func (w *Worker) deadmanTripped(r *run) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if r.phase == phasePrepared || r.phase == phaseRunning {
		log.Printf("worker %s: run %s: no controller heartbeat for %v, stopping", w.cfg.ID, r.id, w.cfg.DeadManTimeout)
		w.stopRun(w.ctx, r, transport.ReasonDeadman)
	}
}

func (w *Worker) newEmitter(r *run) *Emitter {
	return NewEmitter(w.ctx, r.id, w.cfg.ID, w.pub, EmitterConfig{
		Period: w.cfg.SnapshotPeriod, Buffer: w.cfg.SnapshotBuffer, PublishTimeout: w.cfg.PublishTimeout,
	})
}

// execute runs the engine and publishes its snapshots.
func (w *Worker) execute(r *run, t0 time.Time) {
	em := w.newEmitter(r)
	em.Start(r.eng, t0)
	res, err := r.eng.Run(w.ctx, t0)
	if err != nil {
		log.Printf("worker %s: run %s aborted: %v", w.cfg.ID, r.id, err)
	}

	w.mu.Lock()
	if r.reason == "" {
		r.reason = transport.ReasonCompleted
	}
	reason := r.reason
	w.mu.Unlock()

	ctx, cancel := context.WithTimeout(w.ctx, w.cfg.FinalTimeout)
	defer cancel()
	seq, ferr := em.Finish(ctx, res.Final, reason)
	t := res.Total
	log.Printf("worker %s: run %s ended (%s): intended=%d ok=%d err=%d timeout=%d dropped=%d unfinished=%d final_seq=%d undelivered=%d",
		w.cfg.ID, r.id, reason, t.Intended, t.OK, t.Errors, t.Timeouts, t.Dropped, t.Unfinished, seq, em.Failed())
	w.finish(r, seq, ferr)
}

// finishUnstarted ends a run stopped before START. It sent nothing, but it
// still publishes one empty final snapshot so that every worker that acked
// PREPARE has a complete stream.
func (w *Worker) finishUnstarted(ctx context.Context, r *run) {
	em := w.newEmitter(r)
	ctx, cancel := context.WithTimeout(ctx, w.cfg.FinalTimeout)
	defer cancel()
	now := time.Now()
	w.mu.Lock()
	reason := r.reason
	w.mu.Unlock()
	seq, err := em.Finish(ctx, engine.Interval{Start: now, End: now}, reason)
	w.finish(r, seq, err)
}

func (w *Worker) finish(r *run, finalSeq uint64, finalErr error) {
	if finalErr != nil {
		log.Printf("worker %s: run %s: %v", w.cfg.ID, r.id, finalErr)
	}
	r.httpTr.CloseIdleConnections()
	r.warm.Close()

	w.mu.Lock()
	defer w.mu.Unlock()
	r.deadman.Close()
	if err := r.hbSub.Unsubscribe(); err != nil {
		log.Printf("worker %s: run %s: unsubscribe heartbeats: %v", w.cfg.ID, r.id, err)
	}
	r.phase, r.finalSeq, r.finalErr = phaseDone, finalSeq, finalErr
	for _, wt := range r.waiters {
		w.send(wt.reply, r, transport.CmdStop, w.stoppedAck(r, wt.cmdSeq))
	}
	r.waiters = nil
	if w.active == r {
		w.active = nil
	}
	close(r.done)
}

// addRun records a run this worker is a member of. w.mu must be held.
func (w *Worker) addRun(id string) *run {
	r := &run{id: id, acks: map[uint64]cachedAck{}, phase: phaseRefused, done: make(chan struct{})}
	w.runs[id] = r
	w.order = append(w.order, id)
	for len(w.order) > keptRuns {
		old := w.runs[w.order[0]]
		if old == w.active {
			break
		}
		delete(w.runs, w.order[0])
		w.order = w.order[1:]
	}
	return r
}

func (w *Worker) ack(c *transport.Command, s transport.AckStatus) transport.Ack {
	return transport.Ack{WorkerID: w.cfg.ID, RunID: c.RunID, CmdSeq: c.CmdSeq, Status: s}
}

func (w *Worker) nack(c *transport.Command, detail string) transport.Ack {
	a := w.ack(c, transport.StatusNack)
	a.Detail = detail
	return a
}

// stoppedAck is the STOP ack; r must be done. If the final snapshot was
// lost, the detail says so: the aggregator will never see final_seq.
func (w *Worker) stoppedAck(r *run, cmdSeq uint64) transport.Ack {
	seq := r.finalSeq
	a := transport.Ack{WorkerID: w.cfg.ID, RunID: r.id, CmdSeq: cmdSeq, Status: transport.StatusStopped, FinalSeq: &seq}
	if r.finalErr != nil {
		a.Detail = r.finalErr.Error()
	}
	return a
}

// send sends a to replyTo and, if r is set, caches it so that a repeat of
// the same cmd_seq gets the same answer. w.mu must be held.
func (w *Worker) send(replyTo string, r *run, typ transport.CmdType, a transport.Ack) {
	b, err := json.Marshal(a)
	if err != nil {
		log.Printf("worker %s: encode ack: %v", w.cfg.ID, err)
		return
	}
	if r != nil {
		r.acks[a.CmdSeq] = cachedAck{typ: typ, ack: b}
	}
	if replyTo == "" {
		return
	}
	if err := w.nc.Publish(replyTo, b); err != nil {
		log.Printf("worker %s: run %s: ack %d: %v", w.cfg.ID, a.RunID, a.CmdSeq, err)
	}
}

func (w *Worker) reply(msg *nats.Msg, r *run, c *transport.Command, a transport.Ack) {
	w.send(msg.Reply, r, c.Type, a)
}
