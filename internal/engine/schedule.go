package engine

import (
	"fmt"
	"math"
	"math/bits"
	"time"
)

// rateScale is the rate resolution: rates are held internally in
// milli-requests per second so that the schedule can be computed with exact
// integer arithmetic.
const rateScale = 1000

// Schedule fixes the intended send time of every request in advance:
// t_i = t0 + i/rate (design.md §4.2). Each time is computed directly from i,
// never by adding a period to the previous time, so rounding never
// accumulates into drift.
type Schedule struct {
	t0    time.Time // must carry a monotonic reading (derived from time.Now)
	mrate uint64    // requests per second × rateScale
}

// NewSchedule returns the schedule for rate requests per second starting at
// t0. The rate is rounded to the nearest 0.001 req/s.
func NewSchedule(t0 time.Time, rate float64) (Schedule, error) {
	mrate, err := scaleRate(rate)
	if err != nil {
		return Schedule{}, err
	}
	return Schedule{t0: t0, mrate: mrate}, nil
}

func scaleRate(rate float64) (uint64, error) {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 {
		return 0, fmt.Errorf("engine: rate must be a positive finite number, got %v", rate)
	}
	m := math.Round(rate * rateScale)
	if m < 1 || m > 1e15 {
		return 0, fmt.Errorf("engine: rate %v req/s is outside the supported range", rate)
	}
	return uint64(m), nil
}

// At returns the intended send time of request i, rounded to the nearest
// nanosecond. Offsets beyond the range of time.Duration (about 292 years)
// saturate.
func (s Schedule) At(i uint64) time.Time {
	// offset_ns = i × 1e9 × rateScale / mrate, in 128-bit to stay exact.
	hi, lo := bits.Mul64(i, 1e9*rateScale)
	if hi >= s.mrate { // quotient would not fit in 64 bits
		return s.t0.Add(math.MaxInt64)
	}
	q, r := bits.Div64(hi, lo, s.mrate)
	if r >= s.mrate-r {
		q++
	}
	if q > math.MaxInt64 {
		return s.t0.Add(math.MaxInt64)
	}
	return s.t0.Add(time.Duration(q))
}
