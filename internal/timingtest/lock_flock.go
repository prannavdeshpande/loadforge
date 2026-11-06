//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package timingtest

import (
	"os"
	"syscall"
)

// flock locks are per open file and released if the process dies, so a
// crashed test never leaves the lock held.
func lock(f *os.File) error   { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX) }
func unlock(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
