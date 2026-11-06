package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/loadforge/loadforge/internal/safety"
)

// RunLeaseKey is the Redis key of a run's controller lease (design.md §3.2).
func RunLeaseKey(runID string) string { return "lf:lease:run:" + runID }

// Leases are Redis locks whose value is a fencing token drawn from
// Postgres. Redis is ephemeral: losing a lease only pauses work, and the
// token keeps every write it guards safe (CLAUDE.md rule 3).
type Leases struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

// NewLeases uses safety.LeaseTTL. The TTL is not configurable: the
// dead-man's switch relies on it (safety.DeadManTimeout < LeaseTTL).
func NewLeases(rdb redis.UniversalClient) *Leases {
	return &Leases{rdb: rdb, ttl: safety.LeaseTTL}
}

// Only the holder, identified by its token, may renew or release.
var (
	renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0`)
	releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)
)

// Acquire takes the lease with SET NX PX. It reports false, with no error,
// if someone else holds it. token must be fresh from Store.NextFenceToken.
func (l *Leases) Acquire(ctx context.Context, key string, token uint64) (bool, error) {
	if token == 0 {
		return false, errors.New("store: lease needs a fencing token")
	}
	ok, err := l.rdb.SetNX(ctx, key, strconv.FormatUint(token, 10), l.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("store: acquire %s: %w", key, err)
	}
	return ok, nil
}

// Renew extends the lease if token still holds it. false means the lease
// was lost (expired, or Redis restarted and someone else took it): the
// caller must stop at once.
func (l *Leases) Renew(ctx context.Context, key string, token uint64) (bool, error) {
	n, err := renewScript.Run(ctx, l.rdb, []string{key}, strconv.FormatUint(token, 10), l.ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("store: renew %s: %w", key, err)
	}
	return n == 1, nil
}

// Release deletes the lease if token still holds it.
func (l *Leases) Release(ctx context.Context, key string, token uint64) (bool, error) {
	n, err := releaseScript.Run(ctx, l.rdb, []string{key}, strconv.FormatUint(token, 10)).Int()
	if err != nil {
		return false, fmt.Errorf("store: release %s: %w", key, err)
	}
	return n == 1, nil
}

// Holder returns the token holding the lease, if any.
func (l *Leases) Holder(ctx context.Context, key string) (uint64, bool, error) {
	v, err := l.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: read %s: %w", key, err)
	}
	t, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("store: %s holds %q, not a token", key, v)
	}
	return t, true, nil
}
