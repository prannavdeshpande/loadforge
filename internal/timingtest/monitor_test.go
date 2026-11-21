package timingtest

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock advances only when slept on, and stalls once by extra after
// the stallAfter-th sleep. It signals ready once it has slept enough.
type fakeClock struct {
	mu         sync.Mutex
	t          time.Time
	sleeps     int
	stallAfter int
	extra      time.Duration
	enough     int
	ready      chan struct{}
}

func newFakeClock(stallAfter int, extra time.Duration, enough int) *fakeClock {
	return &fakeClock{t: time.Unix(1_000_000, 0), stallAfter: stallAfter, extra: extra, enough: enough, ready: make(chan struct{})}
}

func (f *fakeClock) clock() clock {
	return clock{
		now: func() time.Time {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.t
		},
		sleep: func(d time.Duration) {
			f.mu.Lock()
			f.sleeps++
			f.t = f.t.Add(d)
			if f.sleeps == f.stallAfter {
				f.t = f.t.Add(f.extra)
			}
			if f.sleeps == f.enough {
				close(f.ready)
			}
			f.mu.Unlock()
			runtime.Gosched()
		},
	}
}

func TestStallMonitorReportsTheLongestGapAndWhen(t *testing.T) {
	fc := newFakeClock(10, 50*time.Millisecond, 100)
	stop := startStallMonitor(fc.clock(), time.Millisecond)
	<-fc.ready
	r := stop()
	if r.maxGap != 50*time.Millisecond {
		t.Errorf("max gap %v, want the injected 50ms", r.maxGap)
	}
	// Ten 1ms sleeps, the tenth overrunning by 50ms.
	if r.maxGapAt != 60*time.Millisecond {
		t.Errorf("max gap at +%v, want +60ms", r.maxGapAt)
	}
	if r.samples < 100 {
		t.Errorf("%d samples, want at least 100", r.samples)
	}
	if s := r.String(); !strings.Contains(s, "longest scheduler gap 50ms at +60ms") {
		t.Errorf("report %q", s)
	}
}

func TestStallMonitorOnTheRealClock(t *testing.T) {
	stop := startStallMonitor(realClock, monitorInterval)
	time.Sleep(30 * time.Millisecond)
	r := stop()
	if r.samples == 0 || r.maxGap < 0 {
		t.Errorf("report %+v", r)
	}
	if again := stop(); again != r {
		t.Errorf("a second stop changed the report: %+v then %+v", r, again)
	}
}

// recordingTB is a testing.TB whose outcome, logs and cleanups the test
// controls, so Exclusive can be observed on a test that fails.
type recordingTB struct {
	testing.TB
	failed   bool
	logs     []string
	cleanups []func()
}

func (r *recordingTB) Helper()          {}
func (r *recordingTB) Failed() bool     { return r.failed }
func (r *recordingTB) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }
func (r *recordingTB) Log(args ...any)  { r.logs = append(r.logs, fmt.Sprint(args...)) }
func (r *recordingTB) Logf(format string, a ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, a...))
}

func (r *recordingTB) end() {
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		r.cleanups[i]()
	}
}

func TestExclusiveLogsTheStallReportOnlyOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timing.lock")
	for _, failed := range []bool{false, true} {
		fc := newFakeClock(5, 2*time.Second, 50)
		tb := &recordingTB{TB: t}
		exclusiveWith(tb, path, fc.clock())
		<-fc.ready
		tb.failed = failed
		tb.end()

		var report string
		for _, l := range tb.logs {
			if strings.Contains(l, "host-stall monitor") {
				report = l
			}
		}
		switch {
		case !failed && report != "":
			t.Errorf("a passing test logged %q", report)
		case failed && !strings.Contains(report, "longest scheduler gap 2s"):
			t.Errorf("a failing test logged %q, want the 2s stall", report)
		}

		// The lock was released: it can be taken again at once.
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- lock(f) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("lock still held after the test ended")
		}
		_ = unlock(f)
		_ = f.Close()
	}
}
