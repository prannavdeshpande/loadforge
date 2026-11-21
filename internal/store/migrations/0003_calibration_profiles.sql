-- 0003_calibration_profiles: output of cmd/calibrate (design.md §3.1, §9).
-- Never edit this file once applied; add 0004_*.sql instead.

-- One row per calibration of one host. Rows are append-only: a host class
-- is recalibrated by adding a row, and the latest measured_at wins.
CREATE TABLE calibration_profiles (
    id             bigserial        PRIMARY KEY,
    host_class     text             NOT NULL CHECK (host_class ~ '^[A-Za-z0-9_-]{1,64}$'),
    hostname       text             NOT NULL,
    method_version integer          NOT NULL CHECK (method_version > 0),
    num_cpu        integer          NOT NULL CHECK (num_cpu > 0),
    gomaxprocs     integer          NOT NULL CHECK (gomaxprocs > 0),
    go_version     text             NOT NULL,
    target         text             NOT NULL,  -- 'in-process' or the target URL
    -- Highest per-worker rate sustained within the VALID bounds; 0 if none.
    capacity_rps   double precision NOT NULL CHECK (capacity_rps >= 0),
    -- Dispatch lag at a low, steady rate on an idle generator.
    jitter_p50_us  bigint           NOT NULL CHECK (jitter_p50_us >= 0),
    jitter_p99_us  bigint           NOT NULL CHECK (jitter_p99_us >= 0),
    jitter_p999_us bigint           NOT NULL CHECK (jitter_p999_us >= 0),
    jitter_max_us  bigint           NOT NULL CHECK (jitter_max_us >= 0),
    profile        jsonb            NOT NULL,  -- the whole profile, every ramp step included
    measured_at    timestamptz      NOT NULL,
    recorded_at    timestamptz      NOT NULL DEFAULT now()
);

CREATE INDEX calibration_profiles_latest ON calibration_profiles (host_class, measured_at DESC, id DESC);

CREATE FUNCTION calibration_profiles_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'calibration_profiles is append-only';
END
$$;

CREATE TRIGGER calibration_profiles_append_only
    BEFORE UPDATE OR DELETE ON calibration_profiles
    FOR EACH ROW EXECUTE FUNCTION calibration_profiles_append_only();
