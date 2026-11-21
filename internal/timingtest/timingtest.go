// Package timingtest serialises LoadForge's timing-sensitive tests across
// test processes.
//
// `go test ./...` runs packages' test binaries in parallel. The tests that
// assert on dispatch lag (the coordinated-omission stall tests and the
// dispatcher tests) then share the CPU with each other's 100 req/s
// generators and stub servers, and the scheduler, not the code under test,
// adds tens to hundreds of milliseconds of lag. Their thresholds are not
// loosened for that (CLAUDE.md, workflow rule 5); instead each such test
// holds a machine-wide lock, so at most one runs at a time while every
// other test still runs in parallel.
//
// While a test holds the lock, a host-stall monitor samples the scheduler
// every millisecond. If the test fails, it logs the longest stall it saw,
// so a failure caused by the host (a VM descheduled, swapping, another
// process) is told apart from one caused by the code. It only observes:
// no threshold depends on it and it never changes a test's outcome.
package timingtest

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// LockPath is the lock file shared by every test process on the machine.
func LockPath() string { return filepath.Join(os.TempDir(), "loadforge-timing-tests.lock") }

// Exclusive blocks until no other timing test on this machine holds the
// lock, and holds it until t ends. Call it first in the test, before
// computing any start time: time spent waiting must not count as lag.
// If t fails, the host-stall monitor's report is logged.
func Exclusive(t testing.TB) {
	t.Helper()
	exclusiveAt(t, LockPath())
}

func exclusiveAt(t testing.TB, path string) {
	t.Helper()
	exclusiveWith(t, path, realClock)
}

func exclusiveWith(t testing.TB, path string, c clock) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("timing lock: %v", err)
	}
	start := time.Now()
	if err := lock(f); err != nil {
		_ = f.Close()
		t.Fatalf("timing lock: %v", err)
	}
	if waited := time.Since(start); waited > 100*time.Millisecond {
		t.Logf("waited %v for the timing lock", waited.Round(time.Millisecond))
	}
	// Registered first, this cleanup runs after every other one of the
	// test's, so t.Failed() is final when it reads it.
	stop := startStallMonitor(c, monitorInterval)
	t.Cleanup(func() {
		if r := stop(); t.Failed() {
			t.Log(r)
		}
		_ = unlock(f)
		_ = f.Close()
	})
}
