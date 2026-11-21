package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/loadforge/loadforge/internal/aggregator"
	"github.com/loadforge/loadforge/internal/health"
	"github.com/loadforge/loadforge/internal/store"
	"github.com/loadforge/loadforge/internal/transport"
)

// Store is the part of *store.Store the controller uses. Every write that
// takes a fence token is guarded (design.md §3.3).
type Store interface {
	NextFenceToken(ctx context.Context) (uint64, error)
	CreateRun(ctx context.Context, r store.NewRun) error
	GetRun(ctx context.Context, id string) (store.Run, error)
	ActiveRuns(ctx context.Context) ([]store.Run, error)
	Transition(ctx context.Context, t store.Transition) error
	AppendEvent(ctx context.Context, runID string, fenceToken uint64, e store.Event) error
	RecordPrepareAck(ctx context.Context, runID string, fenceToken uint64, a transport.Ack) error
	RecordStopAck(ctx context.Context, runID string, fenceToken uint64, a transport.Ack) error
	MarkWorkerLost(ctx context.Context, runID string, fenceToken uint64, workerID string, detail json.RawMessage) error
	RequestFinalize(ctx context.Context, runID string, fenceToken uint64) error
	RequestStop(ctx context.Context, runID string) error
	MaxCmdSeq(ctx context.Context, runID string) (uint64, error)
	FinalReasons(ctx context.Context, runID string) (map[string]transport.StopReason, error)
	RunWorkers(ctx context.Context, runID string) ([]store.RunWorker, error)
	Events(ctx context.Context, runID string) ([]store.StoredEvent, error)
	GetResult(ctx context.Context, runID string) (store.ResultRow, error)
	RunProgress(ctx context.Context, runID string) (store.Progress, bool, error)
	VerdictInputs(ctx context.Context, runID string) (health.Inputs, health.Thresholds, error)
}

var _ Store = (*store.Store)(nil)

// Leaser is the run lease in Redis (store.Leases).
type Leaser interface {
	Acquire(ctx context.Context, key string, token uint64) (bool, error)
	Renew(ctx context.Context, key string, token uint64) (bool, error)
	Release(ctx context.Context, key string, token uint64) (bool, error)
	Holder(ctx context.Context, key string) (uint64, bool, error)
}

var _ Leaser = (*store.Leases)(nil)

// LiveCache reads the aggregator's live progress hash. ok is false when
// there is none.
type LiveCache interface {
	Progress(ctx context.Context, runID string) (fields map[string]string, ok bool, err error)
}

// RedisLive reads lf:progress:run:{id} (design.md §3.2).
type RedisLive struct{ rdb redis.UniversalClient }

// NewRedisLive reads progress from rdb.
func NewRedisLive(rdb redis.UniversalClient) *RedisLive { return &RedisLive{rdb: rdb} }

// Progress implements LiveCache.
func (r *RedisLive) Progress(ctx context.Context, runID string) (map[string]string, bool, error) {
	m, err := r.rdb.HGetAll(ctx, aggregator.ProgressKey(runID)).Result()
	if errors.Is(err, redis.Nil) || (err == nil && len(m) == 0) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("controlplane: live progress %s: %w", runID, err)
	}
	return m, true, nil
}
