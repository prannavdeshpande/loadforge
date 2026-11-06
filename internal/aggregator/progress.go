package aggregator

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/loadforge/loadforge/internal/store"
)

// ProgressTTL is how long live progress outlives the last update. It is a
// cache: an expired or lost key only hides progress until the next merge.
const ProgressTTL = 30 * time.Second

// ProgressKey is the Redis hash holding a run's live progress.
func ProgressKey(runID string) string { return "lf:progress:run:" + runID }

// PendingVerdict marks live progress: it is never a result, and a run's
// verdict is only ever read from Postgres (CLAUDE.md rule 5).
const PendingVerdict = "PENDING"

// LiveProgress is the provisional view of a run while it is merged.
type LiveProgress struct {
	store.Progress
	Finalized bool // run_results is written; the verdict is still the controller's
	UpdatedAt time.Time
}

// Fields is the Redis hash for p. provisional and verdict are always set,
// so a reader can never mistake progress for a result.
func (p LiveProgress) Fields() map[string]string {
	i := func(v int64) string { return strconv.FormatInt(v, 10) }
	return map[string]string{
		"provisional":       "true",
		"verdict":           PendingVerdict,
		"finalized":         strconv.FormatBool(p.Finalized),
		"seconds_merged":    i(p.Seconds),
		"snapshots_merged":  i(p.SnapshotsMerged),
		"latest_seq":        strconv.FormatUint(p.LatestSeq, 10),
		"interval_ns_total": i(p.IntervalNSTotal),
		"intended":          i(p.Intended),
		"ok":                i(p.OK),
		"err":               i(p.Err),
		"timeout":           i(p.Timeout),
		"dropped":           i(p.Dropped),
		"unfinished":        i(p.Unfinished),
		"clamped":           i(p.Clamped),
		// Percentiles of the latest merged second only; never averaged.
		"latest_p50_us":     i(p.Latest.P50),
		"latest_p90_us":     i(p.Latest.P90),
		"latest_p99_us":     i(p.Latest.P99),
		"latest_p999_us":    i(p.Latest.P999),
		"latest_max_us":     i(p.Latest.Max),
		"latest_lag_p99_us": i(p.Latest.LagP99),
		"updated_at_unixms": i(p.UpdatedAt.UnixMilli()),
	}
}

// ProgressSink caches live progress.
type ProgressSink interface {
	Put(ctx context.Context, runID string, p LiveProgress) error
}

// RedisProgress writes live progress to Redis.
type RedisProgress struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

// NewRedisProgress returns a sink writing to rdb with ProgressTTL.
func NewRedisProgress(rdb redis.UniversalClient) *RedisProgress {
	return &RedisProgress{rdb: rdb, ttl: ProgressTTL}
}

// putIfNewer writes the progress hash only if it is not older than the one
// stored. Several aggregators publish the same run, each reading Postgres
// and then writing here, so without the check a slow one could overwrite
// newer progress with older, and nothing would correct it until the next
// merge. Progress is ordered by finalized (a finalized hash is never
// replaced by an unfinalized one), then by snapshots_merged, which only
// grows. The TTL is refreshed either way.
//
// KEYS[1] key; ARGV[1] TTL ms, ARGV[2] finalized, ARGV[3] snapshots_merged,
// ARGV[4..] field/value pairs.
var putIfNewer = redis.NewScript(`
local cur = redis.call('HMGET', KEYS[1], 'finalized', 'snapshots_merged')
local curFin, curN = cur[1] == 'true', tonumber(cur[2])
local newFin, newN = ARGV[2] == 'true', tonumber(ARGV[3])
local stale = curN ~= nil and ((curFin and not newFin) or (curFin == newFin and newN < curN))
if not stale then
  redis.call('HSET', KEYS[1], unpack(ARGV, 4))
end
redis.call('PEXPIRE', KEYS[1], ARGV[1])
if stale then return 0 end
return 1
`)

// Put writes the run's progress hash and refreshes its TTL atomically,
// unless the stored progress is newer (see putIfNewer).
func (r *RedisProgress) Put(ctx context.Context, runID string, p LiveProgress) error {
	fields := p.Fields()
	args := make([]any, 0, 3+2*len(fields))
	args = append(args, r.ttl.Milliseconds(), fields["finalized"], fields["snapshots_merged"])
	for k, v := range fields {
		args = append(args, k, v)
	}
	return putIfNewer.Run(ctx, r.rdb, []string{ProgressKey(runID)}, args...).Err()
}
