# LoadForge design

Status: Phase 0 · Last updated: 2026-10-27

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
| calibrate | Per-host capacity and jitter baseline | Postgres (writes the calibration profile) |

## 3. Storage: two kinds of state

### 3.1 Postgres is the system of record
Postgres holds everything that has to be durable or auditable:

| Table | Purpose |
|---|---|
| `runs` | Run config, current state, `fence_token`, verdict |
| `run_events` | Append-only log of state transitions and command acks |
| `run_workers` | Workers joined to a run, their PREPARE ack data (clock offset, capacity) |
| `run_seconds` | Per-second merged histogram and counts from the aggregator |
| `run_results` | Final merged histogram, percentiles and health summary |
| `calibration_profiles` | Output of `cmd/calibrate` for each host class |

Run state machine, stored in `runs.state` with every transition written to `run_events`:

```
PENDING → PREPARING → READY → RUNNING → STOPPING → COMPLETED
    └──────────┴─────────┴────────┴──────────┴────▶ ABORTED
```

### 3.2 Redis holds ephemeral coordination only
- **Leases:** `lf:lease:run:{run_id}` is set with `SET NX PX 10000`, and its value is the fencing token. The holder renews every 3s.
- **Caches:** worker registry and recent run summaries. All of it can be rebuilt from Postgres.
- Persistence (AOF and RDB) is off. If Redis restarts, leases are lost and runs pause until a controller takes the lease again. Nothing durable is lost.

### 3.3 Fencing tokens
1. Before taking a lease, the controller runs `SELECT nextval('fence_seq')`. Drawing the token from Postgres means it keeps increasing even if Redis loses everything.
2. The controller runs `SET lf:lease:run:{id} <token> NX PX 10000`.
3. Every guarded write includes the token:
   ```sql
   UPDATE runs SET state = $2, fence_token = $3
    WHERE id = $1 AND fence_token <= $3;
   ```
   If no rows are updated, the token is stale. The controller must drop the run immediately.
4. Every NATS command carries the token too. Workers remember the highest token they have seen for each run and reject anything lower, so a controller that paused and came back cannot issue commands.

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
- Payload (protobuf): `run_id, worker_id, seq, interval_start, interval_end, latency_hist (HDR V2 compressed), lag_hist, ok, err, timeout, generator_dropped, final`.
- The aggregator expects `seq` values to be contiguous for each `(run_id, worker_id)`. A gap is counted as missing seconds.
- The aggregator merges histograms per second across workers and writes them to `run_seconds`. At the end it merges everything into `run_results`.

## 5. Verdict engine

Each run gets one verdict, which is the **worst** of its health signals. Thresholds are defaults, can be set per run, and are stored alongside the verdict.

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

- **VALID:** the generator kept to its schedule, so latency reflects the system under test.
- **DEGRADED:** results are usable but the generator was under strain; the report lists which signals degraded.
- **INVALID:** the generator itself distorted the measurement. Results are kept for debugging but are never shown as headline numbers.

## 6. NATS control protocol

### 6.1 Subjects
| Subject | Direction | Purpose |
|---|---|---|
| `lf.run.<run_id>.cmd` | controller → workers (request/reply) | PREPARE, START, RATE, STOP |
| `lf.run.<run_id>.hb` | controller → workers (publish, every 1s) | Controller heartbeat that keeps workers running |
| `lf.worker.<worker_id>.hb` | worker → controller (publish, every 1s) | Worker liveness |

Every command carries `{run_id, cmd_seq, fence_token, deadline}`. Every ack carries `{worker_id, cmd_seq, status, detail}`.

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
- `final_seq` in the STOP ack tells the aggregator exactly when a worker's stream is complete.

### 6.3 Dead-man's switch
- **Worker side:** if a worker goes 5s without a controller heartbeat for its run (`DeadManTimeout`), it stops dispatching, drains, publishes a final snapshot marked `final=true, reason=deadman`, and leaves the run.
- **Invariant:** `DeadManTimeout (5s) < lease TTL (10s)`. Workers stop before a new controller can take the lease, so two controllers can never be driving load for the same run.
- **Controller side:** a worker silent for 5s is marked lost. The controller sends STOP to the rest and the run's verdict is **INVALID**.
- Heartbeats carry the fence token. A heartbeat with a token lower than the highest seen is ignored, so it does not count toward keeping the worker running.

## 7. Open questions
- Exactly-once versus at-least-once snapshot ingestion: the plan is to dedupe on `(run_id, worker_id, seq)` in Postgres.
- Clock-sync requirement for workers (chrony on the host versus measuring the offset at PREPARE).
- Whether DEGRADED thresholds should come from each host's calibration profile.
