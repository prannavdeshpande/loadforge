# LoadForge

A distributed load generator written in Go, kept in one repo (monorepo). **Measuring correctly comes before everything else**, including peak throughput, convenience and delivery speed. A fast run that reports wrong numbers is a bug. Read `docs/design.md` before changing anything in `internal/engine`, `internal/health`, `internal/runstate` or `internal/transport`.

## Commands

| Command | What it does |
|---|---|
| `make up` | Starts Postgres, Redis, NATS and Kafka (`deploy/compose/docker-compose.yml`), waits until they're healthy and creates the Kafka topics |
| `make down` | Stops the stack and **deletes its volumes** |
| `make test` | Unit tests: `go test -race ./...` |
| `make test-integration` | Integration tests (`-tags=integration`); run `make up` first |
| `make lint` | gofmt check, `go vet` and golangci-lint (pinned version, installed into `./bin`) |

## Workflow

1. Run `make lint test` before you call any change done. If you touched `internal/store`, `internal/transport` or a protocol, also run `make up && make test-integration`.
2. A change to scheduling, timing or histogram code needs a test that proves there's no coordinated omission. The standard case: the stubbed server stalls for N seconds, and the test asserts that the recorded latencies cover the stall instead of hiding it.
3. Schema changes go in a new migration under `internal/store/migrations/`. Never edit a migration that has already been applied.
4. Adding infrastructure, a message type or a Kafka topic means updating `docs/design.md` in the same change.
5. Never weaken a verdict threshold or a safety check to get a test passing.

## Layout

- `cmd/controlplane`: run lifecycle, NATS command issuer, verdicts
- `cmd/worker`: open-loop load generation, HDR recording, snapshot emission
- `cmd/aggregator`: consumes snapshots, merges histograms, writes results to Postgres
- `cmd/calibrate`: measures a host's generator capacity and scheduler jitter baseline
- `internal/engine`: scheduler and request execution
- `internal/health`: generator health signals that feed the verdict engine
- `internal/runstate`: run state machine, persisted in Postgres
- `internal/safety`: dead-man's switch, rate caps, abort paths
- `internal/transport`: NATS command protocol and Kafka snapshot codec
- `internal/store`: Postgres repositories, migrations, Redis leases

## Non-negotiable rules

### 1. Open-loop measurement
- The scheduler fixes each request's **intended send time** in advance: `t_i = t_0 + i / rate`. It dispatches on that schedule whether or not earlier requests have finished.
- **latency = completion time − intended send time.** Never measure from the actual send time; doing so hides coordinated omission.
- Never let the dispatch loop wait on a response. When dispatch falls behind, record the lag (`actual_send − intended_send`) as a health signal. Never skip or quietly drop a request.
- Take all durations from Go's monotonic clock (`time.Since`, `time.Time.Sub` on `time.Now()` values). Never subtract wall-clock timestamps that were serialised.

### 2. Histograms, not samples
- Workers record into HDR histograms covering 1µs to 60s at 3 significant digits.
- Each second, a worker rotates its interval histogram and publishes one snapshot to the Kafka topic `loadforge.hdr.snapshots`, with message key **`{run_id}:{worker_id}`** so each worker's messages stay in order.
- Every snapshot carries `run_id`, `worker_id`, a contiguous `seq`, the interval start and end, the encoded histogram, ok/error/timeout counts and scheduler-lag statistics.
- Raw per-request samples never leave the worker. Percentiles are never averaged; only histograms are merged.

### 3. State: Postgres is the record, Redis is ephemeral
- Anything that has to survive a restart lives in **Postgres**: runs, state transitions, configs, results and verdicts.
- **Redis** holds only leases, locks and caches. Losing Redis may delay work but must never lose or corrupt data. Redis persistence is turned off on purpose.
- Every lease carries a **fencing token** taken from a Postgres sequence, so it keeps increasing across Redis restarts. Every write a lease guards includes that token, and Postgres rejects a token older than the one it already has. If a write comes back stale, the holder must give up the lease straight away.

### 4. NATS control plane
- Control commands are `PREPARE`, `START`, `RATE` and `STOP`. Each is a request/reply that needs an **explicit ack** from every worker in the run. The controller does not move a run forward until it has every ack or the deadline passes. A missed deadline aborts the run; it never proceeds with only some workers.
- Every command includes `run_id`, `cmd_seq` and `fence_token`. Workers handle a repeated `(run_id, cmd_seq)` idempotently and ignore any token lower than the highest they have seen.
- `START` carries an absolute start time, which acts as a barrier so every worker begins together.
- **Dead-man's switch:** a worker stops generating load if it hears no controller heartbeat within 5 seconds. When in doubt, stop. The heartbeat timeout must stay shorter than the lease TTL.

### 5. Verdicts
- Every run ends in exactly one verdict: `VALID`, `DEGRADED` or `INVALID`, computed from generator health. Results are never shown without their verdict. See `docs/design.md` §5.
