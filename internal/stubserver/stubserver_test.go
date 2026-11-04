package stubserver

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func get(t *testing.T, url string) (time.Duration, int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return 0, 0
	}
	resp.Body.Close()
	return time.Since(start), resp.StatusCode
}

func TestBaseLatency(t *testing.T) {
	srv := httptest.NewServer(New(50 * time.Millisecond))
	defer srv.Close()

	took, code := get(t, srv.URL)
	if code != http.StatusOK || took < 50*time.Millisecond || took > 500*time.Millisecond {
		t.Errorf("status %d after %v, want 200 after ≥ 50ms", code, took)
	}
}

func TestStallBlocksInFlightAndNewArrivals(t *testing.T) {
	stub := New(100 * time.Millisecond)
	srv := httptest.NewServer(stub)
	defer srv.Close()

	start := time.Now()
	var wg sync.WaitGroup
	done := make([]time.Duration, 3)
	launch := func(k int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			get(t, srv.URL)
			done[k] = time.Since(start)
		}()
	}

	launch(0)                         // in flight when the stall begins
	time.Sleep(20 * time.Millisecond) // let it reach the handler
	stub.Stall(500 * time.Millisecond)
	launch(1)                          // arrives at the start of the stall
	time.Sleep(300 * time.Millisecond) //
	launch(2)                          // arrives mid-stall
	wg.Wait()

	for k, d := range done {
		if d < 500*time.Millisecond || d > 900*time.Millisecond {
			t.Errorf("request %d finished at %v, want at the stall end (~520ms)", k, d)
		}
	}
	// After the stall, back to base latency.
	if took, _ := get(t, srv.URL); took > 400*time.Millisecond {
		t.Errorf("post-stall request took %v, want ≈ 100ms", took)
	}
}

func TestStallNeverShortensRunningStall(t *testing.T) {
	stub := New(0)
	stub.Stall(time.Second)
	stub.Stall(10 * time.Millisecond)
	stub.mu.Lock()
	remaining := time.Until(stub.stallUntil)
	stub.mu.Unlock()
	if remaining < 900*time.Millisecond {
		t.Errorf("stall remaining %v after a shorter Stall, want ≈ 1s", remaining)
	}
}

func TestAdminStallEndpoint(t *testing.T) {
	srv := httptest.NewServer(New(0))
	defer srv.Close()

	for _, c := range []struct {
		method, query string
		want          int
	}{
		{http.MethodGet, "?d=1s", http.StatusMethodNotAllowed},
		{http.MethodPost, "?d=bogus", http.StatusBadRequest},
		{http.MethodPost, "?d=-1s", http.StatusBadRequest},
		{http.MethodPost, "?d=300ms", http.StatusNoContent},
	} {
		req, _ := http.NewRequest(c.method, srv.URL+StallPath+c.query, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s %s: status %d, want %d", c.method, c.query, resp.StatusCode, c.want)
		}
	}

	if took, _ := get(t, srv.URL); took < 250*time.Millisecond {
		t.Errorf("request after POST d=300ms took %v, want ≈ 300ms", took)
	}
}
