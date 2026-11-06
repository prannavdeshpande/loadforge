-- 0001_init: system of record for runs, results and verdicts (design.md §3.1).
-- Never edit this file once applied; add 0002_*.sql instead.

-- Fencing tokens come from Postgres so they keep increasing even if Redis
-- loses every lease (design.md §3.3).
CREATE SEQUENCE fence_seq AS bigint MINVALUE 1;

CREATE TABLE runs (
    id                 text        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_-]{1,64}$'),
    config             jsonb       NOT NULL,
    thresholds         jsonb       NOT NULL,
    state              text        NOT NULL CHECK (state IN ('PENDING', 'PREPARING', 'READY', 'RUNNING', 'STOPPING', 'COMPLETED', 'ABORTED')),
    fence_token        bigint      NOT NULL CHECK (fence_token > 0),
    verdict            text        CHECK (verdict IN ('VALID', 'DEGRADED', 'INVALID')),
    verdict_detail     jsonb,
    finalize_requested boolean     NOT NULL DEFAULT false,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    -- Every terminal run has exactly one verdict, and only terminal runs
    -- have one (CLAUDE.md rule 5).
    CONSTRAINT runs_verdict_iff_terminal CHECK ((state IN ('COMPLETED', 'ABORTED')) = (verdict IS NOT NULL)),
    -- An aborted run is never VALID.
    CONSTRAINT runs_aborted_is_invalid CHECK (state <> 'ABORTED' OR verdict = 'INVALID')
);

CREATE INDEX runs_active ON runs (created_at) WHERE state NOT IN ('COMPLETED', 'ABORTED');

-- Append-only log of transitions, command acks and health events.
CREATE TABLE run_events (
    id          bigserial   PRIMARY KEY,
    run_id      text        NOT NULL REFERENCES runs (id),
    at          timestamptz NOT NULL DEFAULT now(),
    fence_token bigint      NOT NULL CHECK (fence_token > 0),
    kind        text        NOT NULL,
    from_state  text,
    to_state    text,
    worker_id   text,
    cmd_seq     bigint      CHECK (cmd_seq > 0),
    detail      jsonb       NOT NULL DEFAULT '{}'
);

CREATE INDEX run_events_by_run ON run_events (run_id, id);

CREATE FUNCTION run_events_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'run_events is append-only';
END
$$;

CREATE TRIGGER run_events_append_only
    BEFORE UPDATE OR DELETE ON run_events
    FOR EACH ROW EXECUTE FUNCTION run_events_append_only();

-- Workers in a run, fixed at creation, and what they reported.
CREATE TABLE run_workers (
    run_id          text    NOT NULL REFERENCES runs (id),
    worker_id       text    NOT NULL CHECK (worker_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    prepare_status  text    CHECK (prepare_status IN ('ready', 'nack')),
    prepare_detail  text,
    clock_offset_ns bigint,           -- NULL = unmeasured; never treated as VALID
    capacity_rps    double precision, -- NULL = unmeasured
    final_seq       bigint  CHECK (final_seq >= 0),
    stop_detail     text,
    lost            boolean NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, worker_id)
);

-- Every snapshot ingested, deduplicated on (run_id, worker_id, seq) so that
-- at-least-once delivery from Kafka counts each one exactly once. These are
-- encoded histograms, not raw samples (CLAUDE.md rule 2). There is no
-- foreign key: the aggregator checks the run exists and records a reject
-- otherwise, so a stray message cannot block a partition.
CREATE TABLE run_snapshots (
    run_id             text        NOT NULL,
    worker_id          text        NOT NULL,
    seq                bigint      NOT NULL CHECK (seq >= 0),
    -- Wall-clock labels in unix ns, for display only. Durations come from
    -- interval_ns, which the worker measures on its monotonic clock.
    interval_start_ns  bigint      NOT NULL,
    interval_end_ns    bigint      NOT NULL,
    interval_ns        bigint      NOT NULL CHECK (interval_ns >= 0),
    intended           bigint      NOT NULL CHECK (intended >= 0),
    ok                 bigint      NOT NULL CHECK (ok >= 0),
    err                bigint      NOT NULL CHECK (err >= 0),
    timeout            bigint      NOT NULL CHECK (timeout >= 0),
    dropped            bigint      NOT NULL CHECK (dropped >= 0),
    unfinished         bigint      NOT NULL CHECK (unfinished >= 0),
    clamped            bigint      NOT NULL CHECK (clamped >= 0),
    latency_hist       bytea       NOT NULL,
    lag_hist           bytea       NOT NULL,
    send_delay_hist    bytea       NOT NULL,
    final              boolean     NOT NULL,
    reason             text        CHECK (reason IN ('completed', 'stop', 'deadman', 'shutdown')),
    ingested_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, worker_id, seq),
    CONSTRAINT run_snapshots_reason_iff_final CHECK (final = (reason IS NOT NULL))
);

-- The aggregator merges one second at a time across workers.
CREATE INDEX run_snapshots_by_second ON run_snapshots (run_id, seq);

-- Kafka messages that could not be decoded or validated. Keyed by offset so
-- that redelivery records each once. run_id and worker_id are parsed from
-- the message key when it is well formed, and feed the protocol-violation
-- signal for that run.
CREATE TABLE snapshot_rejects (
    id           bigserial   PRIMARY KEY,
    topic        text        NOT NULL,
    partition    integer     NOT NULL,
    kafka_offset bigint      NOT NULL,
    kafka_key    bytea,
    run_id       text,
    worker_id    text,
    error        text        NOT NULL,
    at           timestamptz NOT NULL DEFAULT now(),
    UNIQUE (topic, partition, kafka_offset)
);

CREATE INDEX snapshot_rejects_by_run ON snapshot_rejects (run_id) WHERE run_id IS NOT NULL;

-- Columns shared by run_seconds and run_results: summed counts, merged
-- histograms (HDR V2 compressed) and percentiles read from the merged
-- latency histogram, in microseconds. Percentiles are never averaged.
CREATE TABLE run_seconds (
    run_id            text        NOT NULL REFERENCES runs (id),
    seq               bigint      NOT NULL CHECK (seq >= 0),
    -- Snapshots merged into this row. Snapshots never change and the set
    -- only grows, so a row is only replaced by one merged from at least as
    -- many (see store.UpsertSecond).
    snapshots_merged  integer     NOT NULL CHECK (snapshots_merged > 0),
    interval_ns_total bigint      NOT NULL CHECK (interval_ns_total >= 0),
    intended          bigint      NOT NULL CHECK (intended >= 0),
    ok                bigint      NOT NULL CHECK (ok >= 0),
    err               bigint      NOT NULL CHECK (err >= 0),
    timeout           bigint      NOT NULL CHECK (timeout >= 0),
    dropped           bigint      NOT NULL CHECK (dropped >= 0),
    unfinished        bigint      NOT NULL CHECK (unfinished >= 0),
    clamped           bigint      NOT NULL CHECK (clamped >= 0),
    latency_hist      bytea       NOT NULL,
    lag_hist          bytea       NOT NULL,
    send_delay_hist   bytea       NOT NULL,
    p50_us            bigint      NOT NULL,
    p90_us            bigint      NOT NULL,
    p99_us            bigint      NOT NULL,
    p999_us           bigint      NOT NULL,
    max_us            bigint      NOT NULL,
    lag_p99_us        bigint      NOT NULL,
    updated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);

-- Final merged results, written once by the aggregator.
CREATE TABLE run_results (
    run_id             text        PRIMARY KEY REFERENCES runs (id),
    expected_snapshots bigint      NOT NULL CHECK (expected_snapshots >= 0),
    received_snapshots bigint      NOT NULL CHECK (received_snapshots >= 0),
    rejected_snapshots bigint      NOT NULL CHECK (rejected_snapshots >= 0),
    interval_ns_total  bigint      NOT NULL CHECK (interval_ns_total >= 0),
    intended           bigint      NOT NULL CHECK (intended >= 0),
    ok                 bigint      NOT NULL CHECK (ok >= 0),
    err                bigint      NOT NULL CHECK (err >= 0),
    timeout            bigint      NOT NULL CHECK (timeout >= 0),
    dropped            bigint      NOT NULL CHECK (dropped >= 0),
    unfinished         bigint      NOT NULL CHECK (unfinished >= 0),
    clamped            bigint      NOT NULL CHECK (clamped >= 0),
    latency_hist       bytea       NOT NULL,
    lag_hist           bytea       NOT NULL,
    send_delay_hist    bytea       NOT NULL,
    p50_us             bigint      NOT NULL,
    p90_us             bigint      NOT NULL,
    p99_us             bigint      NOT NULL,
    p999_us            bigint      NOT NULL,
    max_us             bigint      NOT NULL,
    lag_p99_us         bigint      NOT NULL,
    finalized_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT run_results_received_le_expected CHECK (received_snapshots <= expected_snapshots)
);
