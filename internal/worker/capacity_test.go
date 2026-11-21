package worker

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/loadforge/loadforge/internal/transport"
)

// A calibrated worker refuses a rate above its capacity, like one above
// its rate cap, and reports the capacity as measured when it accepts.
func TestCalibratedCapacityIsEnforcedAndReported(t *testing.T) {
	h := newHarness(t, &memPub{}, func(c *Config) { c.CapacityRPS = 150 })

	a := h.send(h.prepare("over", 1, 1)) // the harness asks for 200 req/s
	expectStatus(t, a, transport.StatusNack)
	if !strings.Contains(a.Detail, "calibrated capacity of 150") {
		t.Errorf("detail %q", a.Detail)
	}
	if a.CapacityRPS != nil {
		t.Errorf("a NACK reports capacity %v", *a.CapacityRPS)
	}

	a = h.send(h.prepare("at", 1, 1, func(p *transport.Prepare) { p.Rate = 150 }))
	expectStatus(t, a, transport.StatusReady)
	if a.CapacityRPS == nil || *a.CapacityRPS != 150 {
		t.Errorf("ready ack capacity %v, want 150", a.CapacityRPS)
	}
	if a.ClockOffsetNS != nil {
		t.Errorf("clock offset %v reported, but it is not measured", *a.ClockOffsetNS)
	}
}

func TestNewRejectsAnInvalidCapacity(t *testing.T) {
	nc := startNATS(t)
	for _, c := range []float64{-1, math.NaN(), math.Inf(1)} {
		w, err := New(Config{ID: "w1", RateCap: 100, CapacityRPS: c}, nc, &memPub{})
		if err == nil {
			_ = w.Close(context.Background())
			t.Errorf("capacity %v accepted", c)
		}
	}
}
