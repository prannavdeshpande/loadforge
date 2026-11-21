// Package controlplane drives runs: it validates them, takes a fenced lease
// per run, issues PREPARE, START and STOP over NATS with explicit acks from
// every worker, keeps workers alive with gated heartbeats, recovers runs
// whose controller is gone, and records every run's verdict (design.md §6,
// §8).
package controlplane

import (
	"errors"
	"fmt"
	"time"

	"github.com/loadforge/loadforge/internal/safety"
)

// Config holds every duration the controller uses. Production values come
// from DefaultConfig; tests scale them down. Validate enforces the timing
// invariants that keep two controllers from ever driving the same run.
type Config struct {
	// InstanceID names this controller in logs and events.
	InstanceID string

	// LeaseTTL must equal the Leaser's TTL (store.Leases uses
	// safety.LeaseTTL).
	LeaseTTL      time.Duration
	RenewInterval time.Duration
	// HeartbeatInterval is how often lf.run.<id>.hb is published.
	HeartbeatInterval time.Duration
	// DeadManTimeout is the workers' dead-man's switch timeout.
	DeadManTimeout time.Duration
	// GateMargin is the slack in the heartbeat gate; see HeartbeatGate.
	GateMargin time.Duration

	// WorkerSilence is how long a worker may go without a heartbeat on
	// lf.worker.<id>.hb before it is unseen (validation) or lost (in a run).
	WorkerSilence time.Duration
	// LivenessCheck is how often a run's members are checked.
	LivenessCheck time.Duration

	PrepareTimeout time.Duration // all PREPARE acks
	StartLead      time.Duration // start_at = now + StartLead
	StartAckMargin time.Duration // START acks are due by start_at − StartAckMargin
	// FinalSnapshotTimeout is the workers' budget for delivering the final
	// snapshot before acking STOP (worker.Config.FinalTimeout).
	FinalSnapshotTimeout time.Duration
	StopMargin           time.Duration // added to drain + final snapshot for the STOP deadline
	// RetryInterval is the first re-send interval for an unacked command;
	// it doubles up to MaxRetryInterval. Re-sends reuse the cmd_seq.
	RetryInterval    time.Duration
	MaxRetryInterval time.Duration

	// FinalizeWait is how long the controller waits for the aggregator to
	// finalize a stopped run on its own before setting finalize_requested.
	FinalizeWait time.Duration
	FinalizePoll time.Duration
	// StopPoll is how often a driver checks Postgres for a stop request
	// made through another controller instance.
	StopPoll time.Duration
	// OrphanSweep is how often non-terminal runs without a lease holder
	// are recovered. A sweep also runs at startup.
	OrphanSweep time.Duration
	// StoreTimeout bounds one store or Redis call.
	StoreTimeout time.Duration

	Limits Limits
}

// Limits are the controller's safety limits on a run request. They protect
// the system under test from a mistyped config; workers enforce their own
// rate cap as well.
type Limits struct {
	MaxRatePerWorker safety.RateCap
	MaxTotalRate     float64
	MaxDuration      time.Duration
	MaxWorkers       int
}

// DefaultConfig is the production configuration. Limits are left zero,
// which refuses every run until they are set.
func DefaultConfig() Config {
	return Config{
		LeaseTTL:             safety.LeaseTTL,
		RenewInterval:        safety.LeaseRenewInterval,
		HeartbeatInterval:    safety.HeartbeatInterval,
		DeadManTimeout:       safety.DeadManTimeout,
		GateMargin:           time.Second,
		WorkerSilence:        safety.DeadManTimeout,
		LivenessCheck:        250 * time.Millisecond,
		PrepareTimeout:       10 * time.Second,
		StartLead:            2 * time.Second,
		StartAckMargin:       500 * time.Millisecond,
		FinalSnapshotTimeout: 30 * time.Second,
		StopMargin:           2 * time.Second,
		RetryInterval:        500 * time.Millisecond,
		MaxRetryInterval:     4 * time.Second,
		FinalizeWait:         30 * time.Second,
		FinalizePoll:         250 * time.Millisecond,
		StopPoll:             time.Second,
		OrphanSweep:          safety.LeaseTTL,
		StoreTimeout:         5 * time.Second,
	}
}

// HeartbeatGate is how long after the last confirmed lease renewal the
// controller may still publish heartbeats:
//
//	G = LeaseTTL − DeadManTimeout − GateMargin
//
// Measured from when the confirmed renewal was sent, the last heartbeat
// goes out before G, workers trip before G + DeadManTimeout, and the lease
// cannot expire before LeaseTTL. So workers always stop before another
// controller can take the lease (design.md §3.2, §6.3).
func (c Config) HeartbeatGate() time.Duration {
	return c.LeaseTTL - c.DeadManTimeout - c.GateMargin
}

// Validate checks every duration and the timing invariants.
func (c Config) Validate() error {
	for name, d := range map[string]time.Duration{
		"LeaseTTL": c.LeaseTTL, "RenewInterval": c.RenewInterval, "HeartbeatInterval": c.HeartbeatInterval,
		"DeadManTimeout": c.DeadManTimeout, "GateMargin": c.GateMargin, "WorkerSilence": c.WorkerSilence,
		"LivenessCheck": c.LivenessCheck, "PrepareTimeout": c.PrepareTimeout, "StartLead": c.StartLead,
		"StartAckMargin": c.StartAckMargin, "FinalSnapshotTimeout": c.FinalSnapshotTimeout, "StopMargin": c.StopMargin,
		"RetryInterval": c.RetryInterval, "MaxRetryInterval": c.MaxRetryInterval, "FinalizeWait": c.FinalizeWait,
		"FinalizePoll": c.FinalizePoll, "StopPoll": c.StopPoll, "OrphanSweep": c.OrphanSweep, "StoreTimeout": c.StoreTimeout,
	} {
		if d <= 0 {
			return fmt.Errorf("controlplane: %s must be positive", name)
		}
	}
	g := c.HeartbeatGate()
	switch {
	case c.DeadManTimeout >= c.LeaseTTL:
		return fmt.Errorf("controlplane: DeadManTimeout (%v) must be below LeaseTTL (%v)", c.DeadManTimeout, c.LeaseTTL)
	case g+c.DeadManTimeout >= c.LeaseTTL:
		return errors.New("controlplane: heartbeat gate + DeadManTimeout must be below LeaseTTL")
	case g <= c.RenewInterval:
		return fmt.Errorf("controlplane: heartbeat gate (%v) must exceed RenewInterval (%v), or heartbeats pause between renewals", g, c.RenewInterval)
	case c.HeartbeatInterval*2 > c.DeadManTimeout:
		return fmt.Errorf("controlplane: HeartbeatInterval (%v) must be at most half of DeadManTimeout (%v)", c.HeartbeatInterval, c.DeadManTimeout)
	case c.StartAckMargin >= c.StartLead:
		return fmt.Errorf("controlplane: StartAckMargin (%v) must be below StartLead (%v)", c.StartAckMargin, c.StartLead)
	case c.MaxRetryInterval < c.RetryInterval:
		return errors.New("controlplane: MaxRetryInterval must be at least RetryInterval")
	case c.OrphanSweep > c.LeaseTTL:
		return fmt.Errorf("controlplane: OrphanSweep (%v) must be at most LeaseTTL (%v)", c.OrphanSweep, c.LeaseTTL)
	}
	return nil
}

// heartbeatGap is how long since the last published heartbeat before the
// driver must assume workers may have tripped their dead-man's switch.
func (c Config) heartbeatGap() time.Duration { return c.DeadManTimeout - c.GateMargin }

// stopTimeout is the STOP ack deadline for a run: workers drain for up to
// drain_timeout, then deliver the final snapshot before acking.
func (c Config) stopTimeout(drain time.Duration) time.Duration {
	return drain + c.FinalSnapshotTimeout + c.StopMargin
}
