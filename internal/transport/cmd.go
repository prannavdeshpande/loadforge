package transport

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"time"
)

// NATS subjects (design.md §6.1).
const (
	// CmdSubjectAll matches every run's command subject. Workers subscribe
	// to it and act only on runs whose PREPARE lists them.
	CmdSubjectAll = "lf.run.*.cmd"
)

// CmdSubject is where the controller sends commands for a run.
func CmdSubject(runID string) string { return "lf.run." + runID + ".cmd" }

// RunHeartbeatSubject is where the controller publishes its heartbeat.
func RunHeartbeatSubject(runID string) string { return "lf.run." + runID + ".hb" }

// WorkerHeartbeatSubject is where a worker publishes its liveness.
func WorkerHeartbeatSubject(workerID string) string { return "lf.worker." + workerID + ".hb" }

// IDs appear in NATS subjects and in the Kafka key "{run_id}:{worker_id}",
// so they exclude '.', '*', '>', ':' and whitespace.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidateID checks a run or worker ID.
func ValidateID(field, id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("transport: %s %q must be 1-64 characters of [A-Za-z0-9_-]", field, id)
	}
	return nil
}

// CmdType is a control command (design.md §6.2). There is no ABORT: the
// controller aborts a run by sending STOP and recording the abort itself.
type CmdType string

const (
	CmdPrepare CmdType = "PREPARE"
	CmdStart   CmdType = "START"
	CmdRate    CmdType = "RATE"
	CmdStop    CmdType = "STOP"
)

// Command is a controller request. Exactly the payload matching Type is set.
// Deadline, StartAt and EffectiveAt are wall-clock instants; a worker turns
// them into monotonic deadlines on receipt.
type Command struct {
	Type       CmdType   `json:"type"`
	RunID      string    `json:"run_id"`
	CmdSeq     uint64    `json:"cmd_seq"`     // from 1, increasing per run
	FenceToken uint64    `json:"fence_token"` // from Postgres fence_seq
	Deadline   time.Time `json:"deadline"`

	Prepare *Prepare `json:"prepare,omitempty"`
	Start   *Start   `json:"start,omitempty"`
	Rate    *Rate    `json:"rate,omitempty"`
}

// Prepare is the run configuration. Workers lists every worker in the run;
// a worker not listed does not reply to any of the run's commands.
type Prepare struct {
	Workers []string `json:"workers"`

	Method    string `json:"method"`
	TargetURL string `json:"target_url"`

	Rate             float64 `json:"rate"` // per worker, requests per second
	DurationMS       int64   `json:"duration_ms"`
	MaxInFlight      int     `json:"max_in_flight"`
	QueueDepth       int     `json:"queue_depth"`
	RequestTimeoutMS int64   `json:"request_timeout_ms"`
	DrainTimeoutMS   int64   `json:"drain_timeout_ms"`
}

// Lists reports whether workerID is a member of the run.
func (p *Prepare) Lists(workerID string) bool {
	for _, w := range p.Workers {
		if w == workerID {
			return true
		}
	}
	return false
}

// Start arms the barrier: every worker uses StartAt as t0.
type Start struct {
	StartAt time.Time `json:"start_at"`
}

// Rate restarts the schedule at EffectiveAt with a new rate.
type Rate struct {
	Rate        float64   `json:"rate"`
	EffectiveAt time.Time `json:"effective_at"`
}

// Validate checks the envelope and the payload's shape. Whether a worker
// can run the configuration is decided by the worker (engine limits, rate
// cap) and reported in a NACK.
func (c *Command) Validate() error {
	if err := ValidateID("run_id", c.RunID); err != nil {
		return err
	}
	switch {
	case c.CmdSeq == 0:
		return errors.New("transport: cmd_seq is missing")
	case c.FenceToken == 0:
		return errors.New("transport: fence_token is missing")
	case c.Deadline.IsZero():
		return errors.New("transport: deadline is missing")
	}
	payloads := 0
	for _, set := range []bool{c.Prepare != nil, c.Start != nil, c.Rate != nil} {
		if set {
			payloads++
		}
	}
	want := 1
	if c.Type == CmdStop {
		want = 0
	}
	if payloads != want {
		return fmt.Errorf("transport: %s carries %d payloads, want %d", c.Type, payloads, want)
	}
	switch c.Type {
	case CmdPrepare:
		if c.Prepare == nil {
			return errors.New("transport: PREPARE without its payload")
		}
		return c.Prepare.validate()
	case CmdStart:
		if c.Start == nil || c.Start.StartAt.IsZero() {
			return errors.New("transport: START without start_at")
		}
	case CmdRate:
		if c.Rate == nil || c.Rate.EffectiveAt.IsZero() || !positive(c.Rate.Rate) {
			return errors.New("transport: RATE needs a positive rate and effective_at")
		}
	case CmdStop:
	default:
		return fmt.Errorf("transport: unknown command type %q", c.Type)
	}
	return nil
}

func (p *Prepare) validate() error {
	if len(p.Workers) == 0 {
		return errors.New("transport: PREPARE lists no workers")
	}
	seen := make(map[string]bool, len(p.Workers))
	for _, w := range p.Workers {
		if err := ValidateID("worker_id", w); err != nil {
			return err
		}
		if seen[w] {
			return fmt.Errorf("transport: PREPARE lists worker %q twice", w)
		}
		seen[w] = true
	}
	if p.Method == "" {
		return errors.New("transport: PREPARE without method")
	}
	u, err := url.Parse(p.TargetURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("transport: target_url %q must be an absolute http(s) URL", p.TargetURL)
	}
	if !positive(p.Rate) {
		return fmt.Errorf("transport: rate %v must be a positive finite number", p.Rate)
	}
	if p.DurationMS <= 0 || p.RequestTimeoutMS <= 0 || p.DrainTimeoutMS <= 0 {
		return errors.New("transport: duration_ms, request_timeout_ms and drain_timeout_ms must be positive")
	}
	return nil
}

func positive(f float64) bool { return f > 0 && !math.IsInf(f, 0) && !math.IsNaN(f) }

// AckStatus is a worker's answer to a command.
type AckStatus string

const (
	StatusReady   AckStatus = "ready"   // PREPARE accepted
	StatusArmed   AckStatus = "armed"   // START accepted; t0 = start_at
	StatusStopped AckStatus = "stopped" // STOP done; FinalSeq is set
	StatusNack    AckStatus = "nack"    // refused; Detail says why
)

// Ack is a worker's reply to a command.
type Ack struct {
	WorkerID string    `json:"worker_id"`
	RunID    string    `json:"run_id"`
	CmdSeq   uint64    `json:"cmd_seq"`
	Status   AckStatus `json:"status"`
	Detail   string    `json:"detail,omitempty"`

	// PREPARE only. null means unmeasured, which is always the case for
	// now; the verdict engine must not treat an unmeasured signal as VALID.
	ClockOffsetNS *int64   `json:"clock_offset_ns"`
	CapacityRPS   *float64 `json:"capacity_rps"`

	// STOP only: the seq of the final snapshot, so the aggregator knows
	// when the worker's stream is complete.
	FinalSeq *uint64 `json:"final_seq,omitempty"`
}

// Heartbeat is the controller's keep-alive for a run (design.md §6.3).
type Heartbeat struct {
	RunID      string `json:"run_id"`
	FenceToken uint64 `json:"fence_token"`
}

// WorkerHeartbeat is a worker's liveness signal. RunID is empty when idle.
type WorkerHeartbeat struct {
	WorkerID string `json:"worker_id"`
	RunID    string `json:"run_id,omitempty"`
}
