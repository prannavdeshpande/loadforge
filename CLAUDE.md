# LoadForge

A distributed load generator written in Go, kept in one repo (monorepo). **Measuring correctly comes before everything else**, including peak throughput, convenience and delivery speed. A fast run that reports wrong numbers is a bug. Read `docs/design.md` before changing anything in `internal/engine`, `internal/health`, `internal/runstate` or `internal/transport`.

## Commands

We use plain Go and Docker commands; there is no Makefile.

| Step | Command |
|---|---|
| Unit tests | `go test -race ./...` (add `-short` to skip the ~25s coordinated-omission tests while iterating) |
| Lint | `gofmt -l .` (must print nothing), `go vet ./...`, `GOTOOLCHAIN=go1.23.0 go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0 run ./...` |
| Start stack | `docker compose -f deploy/compose/docker-compose.yml up -d --wait`, then create the topic: `docker compose -f deploy/compose/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic loadforge.hdr.snapshots --partitions 12 --replication-factor 1` |
| Integration tests | `go test -race -tags=integration ./...` (start the stack first) |
| Stop stack | `docker compose -f deploy/compose/docker-compose.yml down -v --remove-orphans` (**deletes its volumes**) |
| Stub target | `go run ./cmd/calibrate serve -addr :8080 -latency 1ms`; stall it with `curl -X POST 'localhost:8080/_stall?d=2s'` |

Run the timing tests on an otherwise idle machine. They assert on dispatch lag, and a parallel build, another process, or a host that swaps (a small WSL2 VM, say) can starve the scheduler and fail them. While a timing test holds the `internal/timingtest` lock, a host-stall monitor samples the scheduler every 1ms; if the test fails it logs `timingtest: host-stall monitor: longest scheduler gap …`. A gap of tens of milliseconds or more around the failed measurement means the host stalled the test, not the code; it never changes a pass or a fail. Whether running packages one at a time (`-p 1`) helps is not established: the one measurement so far ran alongside a CI build on the same VM. For integration, `go test -race -count=1 -tags=integration -run '^TestIntegration' ./...` skips re-running every unit test (what CI runs). Never re-run a failed timing test until it passes: that hides a real coordinated-omission regression.

## CI (Jenkins)

`loadforge-ci` is a single Pipeline job (not multibranch) defined by `Jenkinsfile`. Its `BRANCH` parameter picks the branch to build (blank means `main`); images are pushed only for `main`.

**One-time job setup**, in this order:
1. `loadforge-ci` > Configure > Pipeline: Definition **Pipeline script from SCM**, SCM **Git**, Repository URL `https://github.com/prannavdeshpande/loadforge.git`, Branch Specifier **`*/main`** (the only entry: delete any other), Script Path `Jenkinsfile`, **Lightweight checkout unticked**. Save.
2. **Build Now** once. It builds `main` and registers the `BRANCH` parameter.
3. From then on, **Build with Parameters** with any branch that exists on origin.

The Jenkinsfile is always read from `main`, and its Checkout stage validates `BRANCH` (`git check-ref-format`), confirms it exists on origin (`git ls-remote`) and checks it out. A Jenkinsfile change on a branch takes effect once merged; try it first with Replay. Never use `*/${BRANCH}` as the Branch Specifier: the parameter does not exist until the Jenkinsfile has been read once, so the build cannot resolve it and keeps building an old revision.

**The agent** must be otherwise idle while tests run. Both test stages hold a per-node lock, fail if any `loadforge-*` container is already running (stop it: `docker compose -p <project> -f deploy/compose/docker-compose.yml stop`), and record CPU/memory/IO pressure, `free -m` and swap counters before and after into the archived `reports/host-stats.txt` (`deploy/ci/host-stats.sh`). The integration stage starts and removes its own compose stack; keep any other stack stopped. A timing failure with rising `pswpin`/`pswpout` or memory pressure `full` totals between `before` and `after`, or a large host-stall monitor gap, is the host, not the code: give the agent more memory (for WSL2, `%UserProfile%\.wslconfig`) rather than re-running.

## Workflow

1. Run lint and unit tests (above) before you call any change done. If you touched `internal/store`, `internal/transport` or a protocol, also start the stack and run the integration tests.
2. A change to scheduling, timing or histogram code needs a test that proves there's no coordinated omission. The standard case: the stubbed server stalls for N seconds, and the test asserts that the recorded latencies cover the stall instead of hiding it.
3. Schema changes go in a new migration under `internal/store/migrations/`. Never edit a migration that has already been applied.
4. Adding infrastructure, a message type or a Kafka topic means updating `docs/design.md` in the same change.
5. Never weaken a verdict threshold or a safety check to get a test passing.

## Layout

- `cmd/controlplane`: run lifecycle, NATS command issuer, verdicts
- `cmd/worker`: open-loop load generation, HDR recording, snapshot emission
- `cmd/aggregator`: consumes snapshots, merges histograms, writes results to Postgres
- `cmd/calibrate`: measures a host's generator capacity and scheduler jitter baseline
- `internal/controlplane`: run driver, lease keeper and heartbeat gate, worker liveness, orphan recovery, HTTP API
- `internal/engine`: scheduler and request execution
- `internal/stubserver`: HTTP target with configurable latency and injectable stalls, for coordinated-omission tests
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
