//go:build unix

package calibrate

import (
	"syscall"
	"time"
)

// processCPU is the CPU time, user plus system, this process has used.
func processCPU() (time.Duration, error) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, err
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), nil
}
