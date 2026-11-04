// Package stubserver is an HTTP server with configurable latency and
// injectable stalls, used to prove that the engine reports server stalls
// instead of hiding them (coordinated omission).
package stubserver

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// StallPath is the admin endpoint: POST /_stall?d=2s stalls the server for d.
const StallPath = "/_stall"

// Server answers every request with 200 after its base latency. During a
// stall, no request completes: requests already in flight and requests that
// arrive during the stall all finish no earlier than the stall's end, like a
// server frozen by a GC pause.
type Server struct {
	mu         sync.Mutex
	latency    time.Duration
	stallUntil time.Time
}

// New returns a server that delays each response by latency.
func New(latency time.Duration) *Server {
	return &Server{latency: latency}
}

// SetLatency changes the base latency for requests that arrive afterwards.
func (s *Server) SetLatency(d time.Duration) {
	s.mu.Lock()
	s.latency = d
	s.mu.Unlock()
}

// Stall freezes the server for d from now. A stall never shortens one that
// is already running.
func (s *Server) Stall(d time.Duration) {
	until := time.Now().Add(d)
	s.mu.Lock()
	if until.After(s.stallUntil) {
		s.stallUntil = until
	}
	s.mu.Unlock()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == StallPath {
		s.serveStall(w, r)
		return
	}

	s.mu.Lock()
	latency := s.latency
	s.mu.Unlock()
	if !wait(r.Context(), latency) {
		return
	}
	// A stall can start or be extended while we wait, so re-check until clear.
	for {
		s.mu.Lock()
		remaining := time.Until(s.stallUntil)
		s.mu.Unlock()
		if remaining <= 0 {
			break
		}
		if !wait(r.Context(), remaining) {
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) serveStall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	d, err := time.ParseDuration(r.URL.Query().Get("d"))
	if err != nil || d <= 0 {
		http.Error(w, "d must be a positive duration, e.g. d=2s", http.StatusBadRequest)
		return
	}
	s.Stall(d)
	w.WriteHeader(http.StatusNoContent)
}

// wait sleeps for d and reports false if ctx ended first.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
