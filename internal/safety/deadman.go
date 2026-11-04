package safety

import (
	"sync"
	"time"
)

// Timing invariants (design.md §6.3). DeadManTimeout must stay below
// LeaseTTL so that a worker stops before a new controller can take the
// run's lease; TestDeadManTimeoutBelowLeaseTTL enforces it.
const (
	DeadManTimeout    = 5 * time.Second
	LeaseTTL          = 10 * time.Second // lf:lease:run:{id}, SET NX PX
	HeartbeatInterval = time.Second
)

// DeadMan trips when no valid controller heartbeat has arrived for its
// timeout. A heartbeat is valid if its fence token is at least the highest
// token seen for the run, from heartbeats or commands. Tripping is final:
// when in doubt, stop, and a late heartbeat does not restart load.
//
// All timing uses the monotonic clock.
type DeadMan struct {
	timeout time.Duration
	onTrip  func()

	mu      sync.Mutex
	last    time.Time // last valid heartbeat
	fence   uint64    // highest fence token seen
	timer   *time.Timer
	tripped bool
	closed  bool
}

// NewDeadMan arms a switch that calls onTrip, once and from its own
// goroutine, if no valid heartbeat arrives within timeout of now or of the
// previous one. fence is the token of the command that armed it.
func NewDeadMan(timeout time.Duration, fence uint64, onTrip func()) *DeadMan {
	d := &DeadMan{timeout: timeout, onTrip: onTrip, last: time.Now(), fence: fence}
	d.mu.Lock()
	d.timer = time.AfterFunc(timeout, d.check)
	d.mu.Unlock()
	return d
}

// Beat records a controller heartbeat. It reports false, and does not count
// toward keeping the worker running, if the token is stale or the switch has
// already tripped or been closed.
func (d *DeadMan) Beat(fence uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tripped || d.closed || fence < d.fence {
		return false
	}
	d.fence = fence
	d.last = time.Now()
	return true
}

// Observe raises the highest fence token seen, for example from a command,
// so that later heartbeats with a lower token are ignored.
func (d *DeadMan) Observe(fence uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if fence > d.fence {
		d.fence = fence
	}
}

// Tripped reports whether the switch has fired.
func (d *DeadMan) Tripped() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.tripped
}

// Close disarms the switch, for a run that ended normally. It does not undo
// a trip.
func (d *DeadMan) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.timer.Stop()
}

// check runs when the timer fires. A heartbeat may have arrived since it
// was set, so it re-arms for the time remaining instead of tripping.
func (d *DeadMan) check() {
	d.mu.Lock()
	if d.tripped || d.closed {
		d.mu.Unlock()
		return
	}
	if remaining := d.timeout - time.Since(d.last); remaining > 0 {
		d.timer.Reset(remaining)
		d.mu.Unlock()
		return
	}
	d.tripped = true
	d.mu.Unlock()
	d.onTrip()
}
