# LoadForge design

Status: Phase 4 (control plane) · Last updated: 2026-11-21

## 1. Goals and non-goals

**Goals**
- Latency numbers that stay correct when the system under test stalls, which means no coordinated omission.
- Many workers acting as one load generator: synchronised start, coordinated rate changes, merged results.
- Every result is labelled with whether the generator itself could be trusted during the run.

**Non-goals (Phase 0)**
- Running across several regions.
- Scripting in a language other than Go.
- Storing raw per-request traces.

## 2. Architecture

```
                         ┌───────────────────────────┐
     CLI / API ────────▶ │       controlplane        │ ── SQL ──────────▶ ┌────────────┐
                         │ run FSM · barrier · verdict│                    │  Postgres  │
                         └──────┬───────────┬────────┘ ◀─ lease+fence ─▶ │  (record)  │
                                │           │               ┌───────┐     └─────▲──────┘
                                │           └──────────────▶│ Redis │           │
          NATS req/reply:       │                           │leases │           │
          PREPARE/START/RATE/   │  heartbeat lf.run.<id>.hb └───────┘           │
          STOP (+ acks)         ▼                                               │
         ┌────────────┬────────────┬────────────┐                               │
         │  worker 1  │  worker 2  │  worker N  │ ═══ load ═══▶ system under test
         └─────┬──────┴─────┬──────┴─────┬──────┘                               │
               └──────── Kafka: loadforge.hdr.snapshots ───────┐                │
                         key = run_id:worker_id, 1 msg/s/worker │                │
                                                               ▼                │
                                                        ┌────────────┐          │
                                                        │ aggregator │──────────┘
                                                        └────────────┘
```

| Component | Owns | Talks to |
|---|---|---|
| controlplane | Run lifecycle, command issuing, verdicts | Postgres, Redis, NATS |
| worker | Open-loop scheduling, HDR recording, local safety | NATS, Kafka, system under test |
| aggregator | Merging snapshots, completeness tracking, result rows | Kafka, Postgres |
| calibrate | Per-host capacity and jitter baseline (§9) | Postgres (optional, `calibration_profiles`), a profile file the worker loads |

## 3. Storage: two kinds of state

### 3.1 Postgres is the system of record
Postgres holds everything that has to be durable or auditable:

| Table | Purpose |
|---|---|
| `runs` | Run config, current state, `fence_token`, verdict, `finalize_requested`, `stop_requested_at` (migration 0002, §8.5) |
| `run_events` | Append-only log of state transitions, commands (logged before they are sent, §3.3), acks, stop requests and recoveries |
| `run_workers` | Workers joined to a run, their PREPARE ack data (clock offset, capacity) |
| `run_seconds` | Per-second merged histogram and counts from the aggregator |
| `run_snapshots` | Every ingested snapshot, keyed `(run_id, worker_id, seq)` so redelivery is stored once (§4.4) |
| `snapshot_rejects` | Kafka messages that failed to decode or validate, keyed by `(topic, partition, offset)` |
| `run_results` | Final merged histogram, percentiles and health summary |
| `calibration_profiles` | Output of `cmd/calibrate` (migration 0003): one append-only row per calibration; the latest `measured_at` per host class wins (§9) |
| `schema_migrations` | Applied migrations with their SHA-256, maintained by `store.Migrate` |

- **Migrations** live in `internal/store/migrations/NNNN_name.sql`, numbered without gaps, and are embedded in every binary. `store.Migrate` applies the pending ones under a Postgres advisory lock, each in its own transaction. It refuses to start if an applied migration's checksum no longer matches its file (`ErrMigrationEdited`) or if the database has a version the binary doesn't know (`ErrSchemaTooNew`). `TestShippedMigrationsAreUnchanged` pins every shipped checksum, so an edit fails in CI before it reaches a database.
- **The schema enforces the verdict rule:** a run has a verdict exactly when it is `COMPLETED` or `ABORTED`, and an `ABORTED` run is always `INVALID`.
- **`run_events` is append-only**; a trigger rejects `UPDATE` and `DELETE`.
- **Unmeasured is `NULL`:** `run_workers.clock_offset_ns` and `capacity_rps` stay `NULL` when a worker reports nothing, and the verdict engine treats `NULL` as not VALID.

Run state machine, stored in `runs.state` with every transition written to `run_events`:

```
PENDING → PREPARING → READY → RUNNING → STOPPING → COMPLETED
    └──────────┴─────────┴────────┴──────────┴────▶ ABORTED
```

The legal transitions are defined once, in `internal/runstate`. `store.Transition` checks them inside the fenced transaction that writes the new state and its `run_events` row, and it also requires the run to still be in the `from` state the caller expected (`ErrStateConflict` otherwise). There is no other edge: stopping a run before START means ABORTED.

### 3.2 Redis holds ephemeral coordination only
- **Leases:** `lf:lease:run:{run_id}` is set with `SET NX PX 10000`, and its value is the fencing token. The holder renews every 3s (`safety.LeaseRenewInterval`), so it survives two failed renewals. Renew and release are Lua compare-and-set scripts: only the holder of the current token can extend or delete the lease. A failed renewal means the lease is lost, and the holder stops at once.
- **Caches:** worker registry and recent run summaries. All of it can be rebuilt from Postgres.
- **Live progress:** `lf:progress:run:{run_id}` is a hash the aggregator rewrites after each merge, with `PX 30000`. It holds the run's summed counts so far and the percentiles of the **latest merged second only** (percentiles are never summed or averaged). Every write sets `provisional=true` and `verdict=PENDING`, so progress can never be mistaken for a result: the verdict is only ever read from Postgres. Several aggregators publish the same run, so the write is a Lua script that drops stale progress: a hash with `finalized=true` is never replaced by one without, and otherwise the hash with the higher `snapshots_merged` (the sum over the run's seconds, which only grows) wins. A failed write is logged and ignored.
- **Heartbeat gate.** A controller publishes a run's heartbeat only while `time.Since(send time of the last confirmed renewal) < G`, with `G = LeaseTTL − DeadManTimeout − 1s = 4s` (`controlplane.Config.HeartbeatGate`). The renewal's `PEXPIRE` lands after it was sent, so the lease cannot expire before that send time + 10s; the last heartbeat goes out before + 4s, and workers trip before + 9s. Workers therefore always stop before another controller can take the lease. One failed renewal leaves a 2s heartbeat gap (the next confirmation comes 6s after the last), well inside the dead-man timeout; two consecutive failures stop the run. `Config.Validate` enforces `G + DeadManTimeout < LeaseTTL` and `G > LeaseRenewInterval`.
- Persistence (AOF and RDB) is off. If Redis restarts, every lease is lost: each holder's next renewal is refused, it stops heartbeats and gives the run up, the workers' dead-man's switches stop the load, and the orphan sweep (§8.6) records the runs ABORTED / INVALID. Runs in flight are lost as measurements, but nothing durable is lost or corrupted.

### 3.3 Fencing tokens
1. Before taking a lease, the controller runs `SELECT nextval('fence_seq')`. Drawing the token from Postgres means it keeps increasing even if Redis loses everything.
2. The controller runs `SET lf:lease:run:{id} <token> NX PX 10000`.
3. Every guarded write includes the token:
   ```sql
   UPDATE runs SET state = $2, fence_token = $3
    WHERE id = $1 AND fence_token <= $3;
   ```
   If no rows are updated, the token is stale. The controller must drop the run immediately.

   In `internal/store` every guarded write runs in one transaction that first locks the run's row (`SELECT … FOR UPDATE`), fails with `ErrStaleFence` if the caller's token is lower than `runs.fence_token`, and otherwise raises `runs.fence_token` to the caller's token using the predicate above. Raising it means that once a new controller has written, every older token is rejected. The guarded writes are state transitions, events, PREPARE and STOP acks, marking a worker lost, and requesting finalization.
   - A worker's `final_seq` is recorded once. A STOP ack with a different value doesn't overwrite it: it is logged as a `protocol_violation` event, which feeds the verdict, and the write returns `ErrProtocolViolation`.
4. **`cmd_seq` survives a change of controller.** Before sending a command the controller appends a fenced `command` event carrying its `cmd_seq` (re-sends of the same command reuse it and log nothing). A controller that takes over a run continues at `MaxCmdSeq + 1`, so a worker never mistakes its STOP for a repeat of an older command, or ignores it as a lower seq.
5. A controller recovering an orphaned run first appends a `recovered` event with its new token. That raises `runs.fence_token`, so the old holder, if it was only paused, can write nothing more.
6. Every NATS command carries the token too. Workers remember the highest token they have seen for each run and reject anything lower, so a controller that paused and came back cannot issue commands.

## 4. Open-loop timing model

### 4.1 The problem: coordinated omission
A closed-loop generator waits for a response before sending the next request. When the server stalls, the generator sends less, and the requests that would have run into the stall never get recorded. The reported p99 then looks far better than what users actually experience.

### 4.2 The model
- When a run starts at `t_0` with rate `r`, request `i` has intended send time `t_i = t_0 + i/r`. After a `RATE` change at time `t_c`, the schedule starts again from `t_c` at the new rate.
- The dispatcher releases request `i` at `t_i` and does not wait on earlier requests.
- **Recorded latency = completion time − `t_i`.** If the server stalls for 2s, requests queued behind the stall record up to about 2s of latency, which is correct.
- **Dispatch lag** (`actual_send − t_i`) goes into its own HDR histogram. It measures the generator's own health, not the server's.
- **In-flight cap:** every worker has a maximum concurrency. Once it's reached, new requests queue locally and their latency is still measured from `t_i`. If the local queue overflows, the request is counted as `generator_dropped` and that count feeds the verdict. It is never silently removed.
- All timing uses the monotonic clock. Wall-clock time is used only for the `START` barrier and the snapshot interval labels.

### 4.3 Per-second snapshots
- Every second a worker swaps its active histogram for a fresh one (double buffering, no lock on the hot path) and publishes a snapshot.
- Topic: `loadforge.hdr.snapshots`. Key: `{run_id}:{worker_id}`, so each worker's snapshots go in order to one partition.
- Payload (protobuf, hand-encoded in `internal/transport/snapshot.go`, which holds the field numbers): `version, run_id, worker_id, seq, interval_start, interval_end, latency_hist, lag_hist, send_delay_hist, ok, err, timeout, generator_dropped, intended, unfinished, clamped, final, reason, interval_ns`.
  - The current version is **2**. Version 2 added `interval_ns` (field 19) and requires it; a decoder rejects any other version.
  - `interval_ns` is the interval's length on the worker's monotonic clock. The worker takes it from the engine's interval before writing the wall-clock labels, which carry no monotonic reading. An interval from a run that never started has length 0.
  - Histograms are HDR V2 compressed (base64, the HDR interchange format) and must cover 1µs–60s at 3 significant digits; a decoder rejects any other range.
  - `intended` is the number of requests released in the interval. The verdict needs it for "dropped ÷ intended" and "achieved ÷ target rate".
  - `interval_start` and `interval_end` are wall-clock unix nanoseconds, used as labels only. A worker computes each label as `wall(t_0) + (monotonic time since t_0)`, so a wall-clock step during a run cannot make labels go backwards or overlap.
  - `reason` is set on the final snapshot only: `completed`, `stop`, `deadman` or `shutdown`.
  - Per interval, `latency_hist` and `send_delay_hist` each hold `ok + err + timeout` values and `lag_hist` holds `intended` values. Encoders and decoders reject a snapshot that breaks this.
- Rotation runs on the grid `t_0 + k·1s`. The interval still open when the run ends is published as the final snapshot, so the snapshots of a run sum exactly to its totals.
- A snapshot that cannot be published still uses up its `seq`, so the aggregator sees the gap. A `seq` is never reused.
  - Rotation never waits on Kafka: a worker buffers up to 60 snapshots, and drops (and counts) any beyond that.
  - The final snapshot waits for buffer room instead, and the STOP ack is sent only after Kafka has acked it. If it could not be delivered, the ack's `detail` says so.
- A worker that acked PREPARE always publishes exactly one final snapshot, even if the run is stopped before START (an empty interval).
- The aggregator expects `seq` values to be contiguous for each `(run_id, worker_id)`. A gap is counted as missing seconds.
- The aggregator merges histograms per second across workers and writes them to `run_seconds`. At the end it merges everything into `run_results`.

### 4.4 Idempotent ingestion
Kafka delivers at least once. The aggregator writes are safe without a lease because each one is idempotent or monotonic:
- `run_snapshots` inserts use `ON CONFLICT (run_id, worker_id, seq) DO NOTHING`, and offsets are committed only after the insert commits, so a crash means redelivery and never double-counting.
- `snapshot_rejects` is deduplicated on `(topic, partition, offset)`. The table has no foreign key to `runs`, so a stray message can't block a partition.
- A `run_seconds` row is replaced only by a merge of **at least as many** snapshots (`snapshots_merged`). Snapshots never change and only accumulate, so the merge with more inputs is the newer one, and a slow aggregator can't overwrite a fresher merge.
- `run_results` is written once (`ON CONFLICT DO NOTHING`).
- `run_snapshots.interval_ns` is the interval's length on the worker's monotonic clock, carried by snapshot field `interval_ns` (§4.3). Summed into `run_seconds.interval_ns_total` and `run_results.interval_ns_total`, it gives worker-seconds of recording without subtracting wall-clock labels. It is **not** the dispatch window: the final interval runs until the drain ends, and the emitter keeps rotating during a long drain, so it includes time in which nothing was intended (§5, §7).

### 4.5 The aggregator (`internal/aggregator`, `cmd/aggregator`)
**Ingest.** One Kafka consumer group (`loadforge-aggregator`) reads `loadforge.hdr.snapshots`. Offsets are committed only for records marked after their Postgres write committed (`AutoCommitMarks`, with marks committed on revoke and on shutdown), and a rebalance waits until the current poll is handled. A Postgres failure is retried with backoff and never skipped, so the partition waits instead of losing a snapshot. For each record:
1. Decode the payload (v2, standard histogram ranges, the counter invariants).
2. Check the key equals `{run_id}:{worker_id}` from the payload.
3. Check the worker is a member of the run (`run_workers`).
4. Check the record against the worker's stream: no seq after its final seq (from the STOP ack, else from a stored final snapshot); a final snapshot exactly at the final seq and nowhere else; no final snapshot below a stored seq.
5. Insert into `run_snapshots`. If the key already exists, compare content: a redelivery is a no-op, a different snapshot under a reused seq is rejected.

A record failing steps 1–5 goes to `snapshot_rejects`, attributed to the run when the key or payload names one. Every reject attributed to a run counts as a protocol violation for it (§5).

**Merging.** Each second the aggregator merges every second of every unfinalized run that has more stored snapshots than its `run_seconds` row was merged from. It merges from Postgres, holding no state of its own, so a restart or several aggregators at once is safe. Workers share the START barrier and the rotation grid, so `seq k` is the same second on every worker.

**Finalization.** A run in `STOPPING`, `COMPLETED` or `ABORTED` with no result is finalized when every member's stream `0..final_seq` is stored, or straight away once the controller has set `finalize_requested`. The finalizer re-merges every second, then merges every snapshot inside each worker's stream into `run_results` (written once).
- Expected snapshots per worker are `final_seq + 1` when the final seq is known.
- A worker whose stream never ended (lost, or finalization forced) is expected to have reached the highest seq any member reached. That is a lower bound, so completeness is never overstated.
- `rejected_snapshots` counts the run's rejects.

**Who writes the verdict.** The aggregator holds no lease, so it never writes `runs`. The controller builds the inputs with `store.VerdictInputs`, calls `health.Evaluate` and moves the run to its terminal state with the verdict and `verdict_detail` in one fenced `Transition`. Before then `run_results` is internal and is never displayed.

## 5. Verdict engine

Each run gets one verdict, which is the **worst** of its health signals. Thresholds are defaults, can be set per run (`runs.thresholds`, a JSON object of `health.Thresholds` keys; `{}` means the defaults), and are stored alongside the verdict. An unknown key or a malformed band (a VALID bound looser than its DEGRADED bound, a ratio outside [0, 1]) is an error, not a silent default. The VALID bounds for drops (0) and completeness (100%) are fixed and cannot be overridden.

| Signal | VALID | DEGRADED | INVALID |
|---|---|---|---|
| Dispatch lag p99 | ≤ 1 ms | ≤ 10 ms | > 10 ms |
| Achieved ÷ target rate | ≥ 99% | ≥ 95% | < 95% |
| `generator_dropped` ÷ intended | 0 | ≤ 0.1% | > 0.1% |
| Snapshot completeness | 100% | ≥ 99% | < 99% |
| Worker CPU p95 (from calibration headroom) | ≤ 70% | ≤ 85% | > 85% |
| Clock offset spread across workers | ≤ 1 ms | ≤ 5 ms | > 5 ms |
| Worker lost mid-run | none | n/a | any |
| Fencing or protocol violation | none | n/a | any |

How each signal is measured (`internal/health`):

| Signal | Measured as | Unmeasured when |
|---|---|---|
| Dispatch lag p99 | p99 of the run's merged lag histogram (`run_results.lag_p99_us`) | nothing was released |
| Achieved ÷ target rate | Σintended ÷ (per-worker rate × Σdispatch window) | **always, for now:** workers do not report their dispatch window (§4.4, §7) |
| `generator_dropped` ÷ intended | from `run_results` | nothing was released |
| Snapshot completeness | received ÷ expected snapshots (§4.5) | nothing was expected |
| Worker CPU p95 | highest p95 across workers | **always, for now:** workers do not report CPU |
| Clock offset spread | max − min of `run_workers.clock_offset_ns` | any worker's offset is `NULL` (**always, for now**: §6.1) |
| Worker lost mid-run | any `run_workers.lost` | never |
| Fencing or protocol violation | `protocol_violation` events plus `snapshot_rejects` attributed to the run | never |

- **Unmeasured is DEGRADED, never VALID**, and `verdict_detail.unmeasured` lists each one. With CPU, clock offset and dispatch window all unreported today, **no run can be VALID yet**: that is the honest result until those signals are measured.
- With no `run_results` row, every snapshot-derived signal is unmeasured.
- An `ABORTED` run is INVALID whatever its signals.
- **A stream that ended early aborts the run.** If any member's stored final snapshot has reason `deadman` or `shutdown`, that worker stopped generating load before the run's end (for example on a one-way partition that dropped controller heartbeats but not the worker's). The controller then records the run ABORTED / INVALID instead of COMPLETED (`FinalReasons`, §8.4).
- `verdict_detail` is the full `health.Result`: the verdict, every signal with its value and band, the thresholds used, and the lists of degraded, invalid and unmeasured signals.

- **VALID:** the generator kept to its schedule, so latency reflects the system under test.
- **DEGRADED:** results are usable but the generator was under strain; the report lists which signals degraded.
- **INVALID:** the generator itself distorted the measurement. Results are kept for debugging but are never shown as headline numbers.

## 6. NATS control protocol

### 6.1 Subjects
| Subject | Direction | Purpose |
|---|---|---|
| `lf.run.<run_id>.cmd` | controller → workers (request/reply) | PREPARE, START, RATE, STOP. Workers subscribe to `lf.run.*.cmd`. |
| `lf.run.<run_id>.hb` | controller → workers (publish, every 1s) | Controller heartbeat that keeps workers running |
| `lf.worker.<worker_id>.hb` | worker → controller (publish, every 1s) | Worker liveness: `{worker_id, run_id}`, with `run_id` empty when idle |

Every command carries `{run_id, cmd_seq, fence_token, deadline}`. Every ack carries `{worker_id, run_id, cmd_seq, status, detail}`. Commands and acks are JSON (`internal/transport/cmd.go`).

- **Membership:** PREPARE lists every worker in the run (`workers`). A worker that isn't listed never replies to any of that run's commands, so the controller sees missing acks rather than replies from outsiders.
- **Run and worker IDs** are 1–64 characters of `[A-Za-z0-9_-]`, because they appear in NATS subjects and in the Kafka key.
- **Ack status:** `ready` (PREPARE), `armed` (START), `stopped` (STOP, with `final_seq`) or `nack` (with `detail`).
- **Repeats and stale tokens:** a repeated `(run_id, cmd_seq)` gets the cached ack again. A `cmd_seq` below the last one handled, or a `fence_token` below the highest seen, is ignored with no action and no reply. A reused `cmd_seq` carrying a different command type is a protocol violation: the worker logs it and ignores it.
- **One run at a time:** a worker NACKs a PREPARE (`busy with run …`) while another run is prepared, running or stopping.
- **START:** if `start_at` has already passed when the START arrives, the worker NACKs it.
- **PREPARE warm-up is DNS resolution and TCP connects only.** No HTTP request reaches the target before `t_0`.
  - A worker opens `min(max_in_flight, 64)` connections. The run's HTTP client uses them before dialing new ones.
  - Before a warm connection is used, a non-blocking peek checks that the target has not closed it while idle. A dead one is discarded, so the generator's own stale connection is never recorded as a target error.
  - If the name does not resolve or no connection opens, the PREPARE is NACKed with a `warm-up:` detail.
  - Warm-up runs only for a PREPARE the worker would accept, so a refused config never contacts the target.
  - Requests go directly to the target with no HTTP proxy, because the warm connections do.
- **RATE** is not implemented yet. Workers NACK it with `detail=unsupported`, which aborts the run.
- **There is no ABORT command.** The controller aborts by sending STOP and recording ABORTED itself. A worker that receives SIGTERM stops gracefully, with `reason=shutdown`.
- **Clock offset** in the PREPARE ack is `null`, meaning unmeasured. The verdict engine must not count an unmeasured signal as VALID.
- **Capacity** in the PREPARE ack is the worker's calibrated capacity when it was started with `-profile` (§9), and `null` (unmeasured) otherwise. A calibrated worker NACKs a PREPARE whose per-worker rate exceeds it, exactly as it NACKs one above its rate cap.

### 6.2 Barrier sequence
```
controller                                   workers (all N)
    │── PREPARE(config, fence) ──────────────▶│ load config, warm connections,
    │                                         │ measure clock offset
    │◀── ACK(ready, clock_offset, capacity) ──│
    │   all N acks before deadline? else ABORT
    │   state: READY
    │── START(start_at = now + 2s) ──────────▶│ arm the timer for start_at
    │◀── ACK(armed) ──────────────────────────│
    │   all N acks before start_at − 500ms? else STOP + ABORT
    │   state: RUNNING              at start_at: t_0 = start_at, dispatching begins
    │── RATE(rate, effective_at) ────────────▶│ restart the schedule at effective_at
    │◀── ACK ─────────────────────────────────│
    │── STOP ────────────────────────────────▶│ stop dispatching, drain in-flight up to
    │                                         │ the timeout, publish final snapshot
    │◀── ACK(final_seq) ──────────────────────│
    │   aggregator waits until it has seq 0..final_seq for each worker
    │   state: COMPLETED → verdict
```
- Any NACK or missed deadline during PREPARE or START aborts the whole run. The run never starts with only some workers.
- **Deadlines** (production values, `controlplane.DefaultConfig`): PREPARE acks within 10s (warm-up dials with a 5s timeout); START acks by `start_at − 500ms` with `start_at = now + 2s`; STOP acks within `drain_timeout + 30s + 2s`, the 30s being the worker's budget to deliver its final snapshot before acking. A missed STOP deadline aborts the run too (§8.1).
- **Re-sends.** Every member subscribes to the same subject, so the controller re-publishes an unacked command to all of them, every 500ms doubling to at most 4s, with the **same `cmd_seq`**. A worker that already answered replays its cached ack, which the controller drops as a duplicate.
- `start_at` is the only wall-clock value the controller produces. It takes one `time.Now()` reading and derives both the wall-clock barrier and the run's end on its monotonic clock (`t_0 + duration`).
- `final_seq` in the STOP ack tells the aggregator exactly when a worker's stream is complete.

### 6.3 Dead-man's switch
- **Worker side:** if a worker goes 5s without a controller heartbeat for its run (`DeadManTimeout`), it stops dispatching, drains, publishes a final snapshot marked `final=true, reason=deadman`, and leaves the run. The switch is armed at PREPARE, and once it trips it stays tripped.
- **Stopping gracefully** (STOP, dead-man, SIGTERM): requests intended before the stop are still released, including any the dispatcher is behind on. Queued and in-flight requests then drain for up to `drain_timeout`, so a stall in progress at the stop is still measured.
- **Invariant:** `DeadManTimeout (5s) < lease TTL (10s)`. Workers stop before a new controller can take the lease, so two controllers can never be driving load for the same run.
- **Controller side:** a worker silent for 5s on `lf.worker.<id>.hb`, or naming another run there, is marked lost while the run is between PREPARING and the end of STOP. The controller sends STOP to the rest and records the run ABORTED with verdict **INVALID** (§8.3).
- The controller publishes heartbeats only while the heartbeat gate is open (§3.2). If more than `DeadManTimeout − 1s` passes without one being published while the run is live, it assumes the workers may have tripped and aborts the run.
- Heartbeats carry the fence token. A heartbeat with a token lower than the highest seen is ignored, so it does not count toward keeping the worker running.

## 7. Open questions
- **Recovering a run that only needed its verdict.** A run found orphaned in STOPPING with every `final_seq` recorded is aborted (§8.6), although its data may be complete. Resuming finalization and computing the real verdict is possible but would need every recorded ack re-checked; deferred.
- ~~Exactly-once versus at-least-once snapshot ingestion~~: resolved, at-least-once with deduplication in Postgres (§4.4).
- Clock-sync requirement for workers (chrony on the host versus measuring the offset at PREPARE).
- Whether DEGRADED thresholds should come from each host's calibration profile. Profiles now exist (§9), but no threshold reads them yet.
- **Worker CPU p95** is still unmeasured: workers do not report CPU during a run. The next step is a per-snapshot CPU figure (snapshot version 3), judged against the calibrated headroom.
- **Achieved ÷ target rate needs a dispatch window.** Σ`interval_ns` includes the drain after the last release, so a target that stalls at the end would read as a generator falling short. And because the engine never skips a request, Σintended always matches the schedule's count for the window that was dispatched, so the ratio as specified can only detect a dispatcher cut off early. Until this is decided, the signal is unmeasured (DEGRADED). One option is for each snapshot to carry the part of its interval during which the dispatcher was scheduling.

## 8. Control plane (`internal/controlplane`, `cmd/controlplane`)

`cmd/controlplane` only parses flags (`-pg`, `-redis`, `-nats`, `-listen`, and the required safety limits `-max-rate-per-worker`, `-max-total-rate`, `-max-duration`, `-max-workers`), runs migrations and wires the dependencies. Any number of controllers can share one Postgres, Redis and NATS: each run is driven by whichever controller holds its lease.

### 8.1 Run driver
1. **Create** (`POST /runs`): validate the shape (400), the safety limits (422), and that every listed worker sent a heartbeat within 5s naming no run (409). Then draw a fence token, take the lease, and only then insert the PENDING run, so a run never exists without a lease holder. Renewals start at once.
2. **PREPARING:** heartbeats start (workers arm their dead-man's switch on PREPARE), then PREPARE `cmd_seq=1`. Each ack is recorded as it arrives (`RecordPrepareAck`). Any NACK, an ack with another status (a protocol violation), or a missed deadline aborts.
3. **READY**, then START `cmd_seq=2` with the barrier (§6.2). All armed: **RUNNING**.
4. Wait on the monotonic clock until `t_0 + duration`, or a stop request.
5. **STOPPING:** STOP with the next `cmd_seq`. Each `stopped` ack's `final_seq` is recorded once (`RecordStopAck`; a contradiction is a protocol violation and the run continues to its now-INVALID verdict). A NACK or a missed deadline aborts. Heartbeats stop once every worker has acked.
6. **Finalization and verdict** (§8.4), then **COMPLETED** in one fenced write, and the lease is released.

**Abort path** (from any non-terminal state): STOP to every member not already lost (unless the run is PENDING, or a STOP deadline already passed), mark lost workers, set `finalize_requested` if PREPARE was ever sent, and record ABORTED / INVALID. `verdict_detail` is the full health result with `Aborted` set plus `abort_reason`.

**Losing the run:** a refused renewal, a renewal that keeps failing for a whole TTL, or any guarded write returning `ErrStaleFence` or `ErrStateConflict` makes the driver stop heartbeats, send no further command, attempt no further write, and release the lease (a compare-and-delete, harmless if another controller holds it). If the run is still non-terminal, the orphan sweep recovers it (§8.6).

### 8.2 Leases and heartbeats
Renewal every 3s, heartbeat every 1s gated as in §3.2. Every command, heartbeat and guarded write carries the lease's token.

### 8.3 Worker liveness
The controller subscribes to `lf.worker.*.hb` and keeps each worker's last receipt time (monotonic) and the run it named. A payload whose `worker_id` does not match its subject is ignored, so one worker cannot keep another alive. Validation uses it to refuse unseen or busy workers; a running driver checks its members every 250ms (§6.3).

### 8.4 Finalization and verdict
After every STOP ack the driver waits for `run_results`. If the aggregator has not finalized the run on its own within 30s, the driver sets `finalize_requested` and keeps waiting. **A run is never recorded COMPLETED without a result**: an aggregator outage leaves it in STOPPING, with the lease held, until the aggregator is back. Once the result exists, a final snapshot ending `deadman` or `shutdown` aborts the run (§5); otherwise `VerdictInputs` → `health.Evaluate` → `Transition(STOPPING → COMPLETED, verdict, verdict_detail)`.

### 8.5 HTTP API
| Endpoint | Behaviour |
|---|---|
| `POST /runs` | Body: `id` (optional), `workers`, `method`, `target_url`, `rate_per_worker`, `duration_ms`, `max_in_flight`, `queue_depth`, `request_timeout_ms`, `drain_timeout_ms`, `thresholds`. The rate is **per worker**; the stored config is the PREPARE payload. 202 with the run's ID. |
| `POST /runs/{id}/stop` | 202. Writes `runs.stop_requested_at` **without a fence** (drawing a token would fence out the driver); any controller instance can take it. The driver sees it at once if local, else within 1s by polling, logs a fenced `stop_requested` event, and stops: RUNNING → STOPPING → COMPLETED, or ABORTED before START. 404 unknown, 409 terminal. |
| `GET /runs/{id}` | State, config, workers, events. **Results only once the run has its verdict**, always carrying it; an INVALID run's numbers are under `diagnostics`, never `results`. |
| `GET /runs/{id}/live` | The aggregator's Redis progress hash, falling back to `store.RunProgress`. Always `provisional: true, verdict: "PENDING"`, whatever the cache holds; the verdict is only read from Postgres. |

### 8.6 Orphan recovery and shutdown
- **Orphan sweep**, at startup and every LeaseTTL: each non-terminal run with no lease holder (its controller crashed, or gave the run up after losing its lease) is taken under a new, higher token. The controller writes `recovered`, continues at `MaxCmdSeq + 1`, sends STOP (workers that restarted stay silent, which is tolerated), sets `finalize_requested` and records ABORTED / INVALID. This includes a run found in STOPPING whose STOP acks were all recorded: it is aborted rather than finalized (a deliberate choice, see §7).
- **SIGTERM:** the controller aborts every run it drives with reason `controller_shutdown` (STOP, then ABORTED / INVALID), within `-shutdown-timeout`.

## 9. Calibration (`internal/calibrate`, `cmd/calibrate run`)

A profile records what one host can generate while staying within the verdict engine's **VALID** bounds, so a worker never accepts a rate at which its own measurement would degrade.

```
calibrate run -host-class NAME [-out profile.json] [-pg DSN] [-target URL]
```

1. **Jitter baseline.** The real engine, with the HTTP client a worker uses, runs at 100 req/s for 20s on the idle generator. Dispatch lag p50 / p99 / p999 / max is what the host adds to every schedule before any load.
2. **Capacity ramp.** 500, 1000, 2000, … req/s (`-start-rate`, `-factor`, up to `-max-rate`), 10s per step, with a 2s pause between measurements. A step passes only if dispatch-lag p99 ≤ 1ms, `generator_dropped` = 0 and process CPU ≤ 70%: the VALID bounds of §5. The ramp stops at the first failing step. **Capacity is the last passing rate**, never interpolated, and 0 if the first step failed. If every step passed, capacity is at least `-max-rate` and the output says so.
3. **CPU** is the process's user + system time (`getrusage`) over the step, including its drain, divided by wall time × GOMAXPROCS. It is only measured on unix; elsewhere calibration fails rather than leave the CPU limit unjudged.
4. **Target.** By default a zero-latency stub runs in the same process on loopback. It shares the CPU with the generator, so capacity is **conservative**. `-target URL` calibrates against an external stub (`calibrate serve`) instead; the profile records which.

**The profile** (JSON, `MethodVersion` 1) holds the host class, hostname, CPU count, GOMAXPROCS, Go version, target, jitter, limits, every step and `measured_at`. It is written to `-out` and/or appended to `calibration_profiles`. `MethodVersion` changes whenever the method does, and a worker refuses a profile with another version.

**Workers** load a profile with `-profile`. A worker refuses to start if the profile was measured with a different CPU count or GOMAXPROCS, or if its capacity is 0 (that host cannot generate load within the VALID bounds). Otherwise it reports the capacity in every PREPARE ack (`run_workers.capacity_rps`, shown by `GET /runs/{id}`) and NACKs any rate above it (§6.1). Without a profile, capacity stays `null` and only the rate cap applies.

Calibrate on an otherwise idle host: anything else using its CPU lowers the measured capacity.
