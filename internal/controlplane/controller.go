package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/loadforge/loadforge/internal/runstate"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

// Controller drives runs. Each run it drives holds a lease; any number of
// controllers can share one Postgres, Redis and NATS.
type Controller struct {
	cfg    Config
	st     Store
	leases Leaser
	nc     *nats.Conn
	live   LiveCache
	reg    *Registry

	mu      sync.Mutex
	drivers map[string]*driver
	closing bool

	sweepStop chan struct{}
	sweepDone chan struct{}
	closeOnce sync.Once
}

// New builds a controller. live may be nil, in which case live progress
// comes from Postgres only.
func New(cfg Config, st Store, leases Leaser, nc *nats.Conn, live LiveCache) (*Controller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = "controlplane"
	}
	return &Controller{cfg: cfg, st: st, leases: leases, nc: nc, live: live, drivers: map[string]*driver{}}, nil
}

// Start subscribes to worker heartbeats, recovers orphaned runs and starts
// the periodic orphan sweep.
func (c *Controller) Start() error {
	reg, err := NewRegistry(c.nc)
	if err != nil {
		return err
	}
	c.reg = reg
	c.sweepStop, c.sweepDone = make(chan struct{}), make(chan struct{})
	c.sweep()
	go c.sweepLoop()
	return nil
}

func (c *Controller) sweepLoop() {
	defer close(c.sweepDone)
	t := time.NewTicker(c.cfg.OrphanSweep)
	defer t.Stop()
	for {
		select {
		case <-c.sweepStop:
			return
		case <-t.C:
			c.sweep()
		}
	}
}

// Close aborts every run this controller drives (reason
// controller_shutdown) and waits for them, up to ctx.
func (c *Controller) Close(ctx context.Context) error {
	var err error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closing = true
		ds := make([]*driver, 0, len(c.drivers))
		for _, d := range c.drivers {
			ds = append(ds, d)
		}
		c.mu.Unlock()
		if c.sweepStop != nil {
			close(c.sweepStop)
			<-c.sweepDone
		}
		for _, d := range ds {
			d.cancel(aborting("controller_shutdown"))
		}
		for _, d := range ds {
			select {
			case <-d.done:
			case <-ctx.Done():
				err = fmt.Errorf("controlplane: shutdown: run %s still ending: %w", d.runID, ctx.Err())
				return
			}
		}
		if c.reg != nil {
			err = c.reg.Close()
		}
	})
	return err
}

// CreateRun validates a request, takes the run's lease, records the run as
// PENDING and starts driving it.
func (c *Controller) CreateRun(ctx context.Context, req RunRequest) (store.Run, error) {
	v, err := c.validate(req)
	if err != nil {
		return store.Run{}, err
	}
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	if closing {
		return store.Run{}, refuse(http.StatusServiceUnavailable, "controller is shutting down")
	}

	// Fence, then lease, then row: a run never exists without a lease
	// holder, so a leaseless non-terminal run is always an orphan.
	token, err := c.st.NextFenceToken(ctx)
	if err != nil {
		return store.Run{}, err
	}
	key := store.RunLeaseKey(v.id)
	acquiredAt := time.Now()
	ok, err := c.leases.Acquire(ctx, key, token)
	if err != nil {
		return store.Run{}, err
	}
	if !ok {
		return store.Run{}, refuse(http.StatusConflict, "run %s already exists", v.id)
	}
	keeper := newLeaseKeeper(ctx, c.cfg, c.leases, key, token, acquiredAt)
	cfgJSON, err := json.Marshal(v.prep)
	if err != nil {
		keeper.close(ctx, true)
		return store.Run{}, err
	}
	err = c.st.CreateRun(ctx, store.NewRun{ID: v.id, Config: cfgJSON, Thresholds: v.thresholds, Workers: v.prep.Workers, Fence: token})
	if err != nil {
		keeper.close(ctx, true)
		if errors.Is(err, store.ErrExists) {
			return store.Run{}, refuse(http.StatusConflict, "run %s already exists", v.id)
		}
		return store.Run{}, err
	}
	run := store.Run{ID: v.id, Config: cfgJSON, Thresholds: v.thresholds, State: runstate.Pending, FenceToken: token}
	d, rctx := newDriver(ctx, c, run, v.prep.Workers, v.prep, token, keeper)
	if !c.adopt(d) {
		// Close began after the check above; the run is recorded, so let
		// the abort path end it like any other run of a closing controller.
		d.cancel(aborting("controller_shutdown"))
	}
	go d.run(context.WithoutCancel(ctx), rctx)
	log.Printf("controlplane: run %s created (fence %d, workers %v)", v.id, token, v.prep.Workers)
	return run, nil
}

// RequestStop records a stop request in Postgres, where whichever
// controller drives the run will see it, and tells the local driver at
// once if there is one.
func (c *Controller) RequestStop(ctx context.Context, runID string) error {
	if err := c.st.RequestStop(ctx, runID); err != nil {
		return err
	}
	c.mu.Lock()
	d := c.drivers[runID]
	c.mu.Unlock()
	if d != nil {
		d.requestStop()
	}
	return nil
}

// adopt registers a driver; false if the controller is closing.
func (c *Controller) adopt(d *driver) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drivers[d.runID] = d
	return !c.closing
}

func (c *Controller) forget(runID string, d *driver) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.drivers[runID] == d {
		delete(c.drivers, runID)
	}
}

func (c *Controller) driving(runID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drivers[runID] != nil
}

// sweep recovers every non-terminal run that no controller holds a lease
// for: its controller crashed, or lost its lease and gave the run up.
func (c *Controller) sweep() {
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	if closing {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.StoreTimeout)
	defer cancel()
	runs, err := c.st.ActiveRuns(ctx)
	if err != nil {
		log.Printf("controlplane: orphan sweep: %v", err)
		return
	}
	for _, r := range runs {
		if c.driving(r.ID) {
			continue
		}
		if _, held, err := c.leases.Holder(ctx, store.RunLeaseKey(r.ID)); err != nil || held {
			if err != nil {
				log.Printf("controlplane: orphan sweep: run %s: %v", r.ID, err)
			}
			continue
		}
		if err := c.recoverRun(ctx, r); err != nil {
			log.Printf("controlplane: recover run %s: %v", r.ID, err)
		}
	}
}

// recoverRun takes an orphaned run's lease under a new, higher fence token
// and aborts it: STOP to its workers, ABORTED with verdict INVALID.
func (c *Controller) recoverRun(ctx context.Context, r store.Run) error {
	token, err := c.st.NextFenceToken(ctx)
	if err != nil {
		return err
	}
	key := store.RunLeaseKey(r.ID)
	acquiredAt := time.Now()
	ok, err := c.leases.Acquire(ctx, key, token)
	if err != nil || !ok {
		return err // someone else took it first
	}
	keeper := newLeaseKeeper(ctx, c.cfg, c.leases, key, token, acquiredAt)
	workers, err := c.st.RunWorkers(ctx, r.ID)
	if err != nil {
		keeper.close(ctx, true)
		return err
	}
	members := make([]string, len(workers))
	for i, w := range workers {
		members[i] = w.WorkerID
	}
	var prep transport.Prepare
	if err := json.Unmarshal(r.Config, &prep); err != nil {
		keeper.close(ctx, true)
		return fmt.Errorf("config: %w", err)
	}
	d, _ := newDriver(ctx, c, r, members, prep, token, keeper)
	if !c.adopt(d) {
		c.forget(r.ID, d)
		keeper.close(ctx, true)
		return nil
	}
	log.Printf("controlplane: recovering orphaned run %s (%s) with fence %d", r.ID, r.State, token)
	go d.recover(context.WithoutCancel(ctx))
	return nil
}
