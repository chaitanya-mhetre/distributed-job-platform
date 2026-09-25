-- M1: the job record (source of truth for status) and per-attempt history.
-- status is TEXT + CHECK rather than a Postgres ENUM: adding a value to an ENUM needs
-- ALTER TYPE (awkward inside transactions); a CHECK constraint is a one-line change.
CREATE TABLE IF NOT EXISTS jobs (
    id               uuid PRIMARY KEY,
    type             text        NOT NULL,
    priority         text        NOT NULL CHECK (priority IN ('high', 'default', 'low')),
    payload          jsonb       NOT NULL,
    idempotency_key  text,
    status           text        NOT NULL CHECK (status IN
                        ('queued', 'scheduled', 'running', 'succeeded', 'failed', 'dead', 'cancelled')),
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL,
    timeout_s        int         NOT NULL,
    run_at           timestamptz NOT NULL,
    -- NULL until the job has been handed to Redis. The scheduler's reconciliation loop
    -- re-dispatches rows that stay NULL (crash between INSERT and XADD). "Outbox-lite".
    dispatched_at    timestamptz,
    locked_by        text,
    result           jsonb,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency
    ON jobs (type, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_status_run_at ON jobs (status, run_at);
CREATE INDEX IF NOT EXISTS jobs_type_created ON jobs (type, created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_created ON jobs (created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS jobs_undispatched ON jobs (created_at) WHERE dispatched_at IS NULL;

CREATE TABLE IF NOT EXISTS job_attempts (
    id           bigserial PRIMARY KEY,
    job_id       uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt      int         NOT NULL,
    worker_id    text        NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    outcome      text,       -- succeeded | retry | failed | dead | cancelled | timeout | lost
    error        text,
    duration_ms  int
);
CREATE INDEX IF NOT EXISTS job_attempts_job ON job_attempts (job_id, attempt);
