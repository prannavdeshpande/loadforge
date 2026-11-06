//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package timingtest

import "os"

// Without flock the tests are not serialised; run them with -p 1.
func lock(*os.File) error   { return nil }
func unlock(*os.File) error { return nil }
