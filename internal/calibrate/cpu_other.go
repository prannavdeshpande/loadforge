//go:build !unix

package calibrate

import (
	"errors"
	"time"
)

// processCPU is not measured on this platform. Calibration then fails: an
// unmeasured CPU limit would make the ramp's capacity unjudged.
func processCPU() (time.Duration, error) {
	return 0, errors.New("calibrate: process CPU time is only measured on unix")
}
