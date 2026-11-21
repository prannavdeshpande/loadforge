package timingtest

import (
	"fmt"
	"sync"
	"time"
)

// monitorInterval is how often the host-stall monitor samples the
// scheduler: it asks to sleep this long and records by how much the sleep
// overran.
const monitorInterval = time.Millisecond

// stallReport is what the host-stall monitor saw.
type stallReport struct {
	samples  int64
	maxGap   time.Duration // the longest overrun of a requested sleep
	maxGapAt time.Duration // when it ended, relative to the monitor's start
}

func (r stallReport) String() string {
	if r.samples == 0 {
		return "timingtest: host-stall monitor: no samples"
	}
	return fmt.Sprintf("timingtest: host-stall monitor: longest scheduler gap %v at +%v (%v sampling, %d samples). "+
		"A gap near the failed measurement means the host (VM, swap, another process) stalled this test, not the code under test.",
		r.maxGap.Round(time.Microsecond), r.maxGapAt.Round(time.Millisecond), monitorInterval, r.samples)
}

// clock is what the monitor reads time and sleeps with; tests inject one.
type clock struct {
	now   func() time.Time
	sleep func(time.Duration)
}

var realClock = clock{now: time.Now, sleep: time.Sleep}

// startStallMonitor samples the scheduler every interval until stop is
// called; stop returns the report. It only observes: it never fails,
// skips or retries a test.
func startStallMonitor(c clock, interval time.Duration) (stop func() stallReport) {
	done := make(chan struct{})
	exited := make(chan struct{})
	var (
		mu sync.Mutex
		r  stallReport
	)
	start := c.now()
	go func() {
		defer close(exited)
		prev := start
		for {
			select {
			case <-done:
				return
			default:
			}
			c.sleep(interval)
			now := c.now()
			gap := now.Sub(prev) - interval
			prev = now
			mu.Lock()
			r.samples++
			if gap > r.maxGap {
				r.maxGap, r.maxGapAt = gap, now.Sub(start)
			}
			mu.Unlock()
		}
	}()
	var once sync.Once
	return func() stallReport {
		once.Do(func() { close(done) })
		<-exited
		mu.Lock()
		defer mu.Unlock()
		return r
	}
}
