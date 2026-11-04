package safety

import "fmt"

// RateCap is the highest per-worker rate, in requests per second, that a
// worker accepts. It protects the system under test from a mistyped
// config; it is not the generator's capacity, which comes from calibration.
type RateCap float64

// Check refuses a rate above the cap. A zero cap refuses everything, so a
// worker started without one cannot generate load.
func (c RateCap) Check(rate float64) error {
	if rate > float64(c) {
		return fmt.Errorf("safety: rate %v req/s exceeds this worker's cap of %v req/s", rate, float64(c))
	}
	return nil
}
