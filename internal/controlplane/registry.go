package controlplane

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/loadforge/loadforge/internal/transport"
)

// workerHeartbeatAll matches every worker's liveness subject.
const workerHeartbeatAll = "lf.worker.*.hb"

// Registry tracks worker liveness from lf.worker.*.hb. Times are monotonic
// receipt times; the heartbeat carries no timestamp.
type Registry struct {
	sub *nats.Subscription

	mu   sync.Mutex
	seen map[string]sighting
}

type sighting struct {
	at    time.Time
	runID string
}

// NewRegistry subscribes to every worker heartbeat on nc.
func NewRegistry(nc *nats.Conn) (*Registry, error) {
	r := &Registry{seen: map[string]sighting{}}
	sub, err := nc.Subscribe(workerHeartbeatAll, r.handle)
	if err != nil {
		return nil, err
	}
	r.sub = sub
	return r, nc.Flush()
}

func (r *Registry) handle(msg *nats.Msg) {
	var hb transport.WorkerHeartbeat
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		log.Printf("controlplane: undecodable worker heartbeat on %s: %v", msg.Subject, err)
		return
	}
	// The subject names the worker; a payload that disagrees is ignored,
	// so one worker cannot keep another alive.
	if transport.ValidateID("worker_id", hb.WorkerID) != nil || msg.Subject != transport.WorkerHeartbeatSubject(hb.WorkerID) {
		log.Printf("controlplane: worker heartbeat for %q on %s ignored", hb.WorkerID, msg.Subject)
		return
	}
	r.mu.Lock()
	r.seen[hb.WorkerID] = sighting{at: time.Now(), runID: strings.TrimSpace(hb.RunID)}
	r.mu.Unlock()
}

// Last returns when the worker was last heard from and the run it named.
func (r *Registry) Last(workerID string) (at time.Time, runID string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.seen[workerID]
	return s.at, s.runID, ok
}

// Close unsubscribes.
func (r *Registry) Close() error { return r.sub.Unsubscribe() }
