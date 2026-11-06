package timingtest

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A second holder must wait until the first releases. flock locks belong to
// an open file, so a second open of the lock file in this process contends
// exactly as another test process would. The test uses a lock file of its
// own: the shared one may be held by a timing test in another package.
func TestExclusiveExcludesAnotherHolder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timing.lock")
	got := make(chan error, 1)
	var other *os.File
	t.Cleanup(func() {
		if other != nil {
			_ = unlock(other)
			_ = other.Close()
		}
	})

	t.Run("held", func(t *testing.T) {
		exclusiveAt(t, path) // released when this subtest ends
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		other = f
		go func() { got <- lock(f) }()
		select {
		case err := <-got:
			t.Fatalf("second holder took the lock while it was held (err %v)", err)
		case <-time.After(500 * time.Millisecond):
		}
	})
	if t.Failed() {
		return
	}
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second holder never got the lock after it was released")
	}
}
