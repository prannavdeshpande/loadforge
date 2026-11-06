package safety

import (
	"sync/atomic"
	"testing"
	"time"
)

// The tests scale the 5s timeout down; the switch only compares elapsed
// monotonic time with its timeout.
const testTimeout = 100 * time.Millisecond

func newTestDeadMan(t *testing.T, fence uint64) (*DeadMan, *atomic.Int32) {
	t.Helper()
	var trips atomic.Int32
	d := NewDeadMan(testTimeout, fence, func() { trips.Add(1) })
	t.Cleanup(d.Close)
	return d, &trips
}

func TestDeadManTimeoutBelowLeaseTTL(t *testing.T) {
	if DeadManTimeout >= LeaseTTL {
		t.Fatalf("DeadManTimeout %v ≥ LeaseTTL %v: two controllers could drive one run", DeadManTimeout, LeaseTTL)
	}
	// A worker must be able to miss several heartbeats before tripping.
	if DeadManTimeout < 3*HeartbeatInterval {
		t.Errorf("DeadManTimeout %v allows fewer than 3 missed heartbeats at %v", DeadManTimeout, HeartbeatInterval)
	}
}

func TestLeaseRenewToleratesTwoMissedRenewals(t *testing.T) {
	if 3*LeaseRenewInterval >= LeaseTTL {
		t.Fatalf("LeaseRenewInterval %v: three renewal periods reach the %v TTL", LeaseRenewInterval, LeaseTTL)
	}
}

func TestDeadManTripsWithoutHeartbeat(t *testing.T) {
	d, trips := newTestDeadMan(t, 1)
	time.Sleep(testTimeout / 2)
	if d.Tripped() {
		t.Fatal("tripped before the timeout")
	}
	time.Sleep(testTimeout)
	if !d.Tripped() || trips.Load() != 1 {
		t.Fatalf("tripped=%v calls=%d after 1.5× the timeout, want tripped once", d.Tripped(), trips.Load())
	}
}

func TestDeadManHeartbeatsKeepItAlive(t *testing.T) {
	d, trips := newTestDeadMan(t, 1)
	for range 10 { // 10 × timeout/4 = 2.5 timeouts
		time.Sleep(testTimeout / 4)
		if !d.Beat(1) {
			t.Fatal("valid heartbeat refused")
		}
	}
	if d.Tripped() || trips.Load() != 0 {
		t.Fatal("tripped despite regular heartbeats")
	}
}

func TestDeadManStaleFenceDoesNotKeepItAlive(t *testing.T) {
	d, _ := newTestDeadMan(t, 5)
	d.Observe(7) // a command from the current controller
	for range 6 {
		time.Sleep(testTimeout / 4)
		if d.Beat(6) {
			t.Fatal("heartbeat with a token below the highest seen was accepted")
		}
	}
	if !d.Tripped() {
		t.Fatal("stale heartbeats kept the worker running")
	}
}

func TestDeadManHigherFenceRaisesTheBar(t *testing.T) {
	d, _ := newTestDeadMan(t, 1)
	if !d.Beat(3) {
		t.Fatal("higher token refused")
	}
	if d.Beat(2) {
		t.Error("token 2 accepted after 3")
	}
	if !d.Beat(3) {
		t.Error("repeated current token refused")
	}
}

func TestDeadManTripIsFinal(t *testing.T) {
	d, trips := newTestDeadMan(t, 1)
	time.Sleep(2 * testTimeout)
	if d.Beat(1) {
		t.Error("heartbeat accepted after the trip")
	}
	time.Sleep(2 * testTimeout)
	if trips.Load() != 1 {
		t.Errorf("onTrip called %d times, want once", trips.Load())
	}
}

func TestDeadManCloseDisarms(t *testing.T) {
	d, trips := newTestDeadMan(t, 1)
	d.Close()
	time.Sleep(2 * testTimeout)
	if trips.Load() != 0 {
		t.Error("tripped after Close")
	}
}

func TestRateCap(t *testing.T) {
	c := RateCap(1000)
	if err := c.Check(1000); err != nil {
		t.Errorf("rate at the cap refused: %v", err)
	}
	if err := c.Check(1000.5); err == nil {
		t.Error("rate above the cap accepted")
	}
	if err := RateCap(0).Check(1); err == nil {
		t.Error("zero cap accepted a rate")
	}
}
