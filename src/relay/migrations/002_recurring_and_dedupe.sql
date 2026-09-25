-- M3: cron-style recurring jobs, and a durable "run this side effect once" table.
CREATE TABLE IF NOT EXISTS recurring_jobs (
    id                uuid PRIMARY KEY,
    name              text        NOT NULL UNIQUE,
    cron              text        NOT NULL,          -- 5 fields, or 6 with seconds last
    type              text        NOT NULL,
    payload           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    priority          text        NOT NULL DEFAULT 'default',
    enabled           boolean     NOT NULL DEFAULT true,
    last_enqueued_at  timestamptz,                   -- the *scheduled* fire time, not wall clock
    created_at        timestamptz NOT NULL DEFAULT now()
);

-- ctx.once(key): INSERT ... ON CONFLICT DO NOTHING. Postgres, not Redis, because "did we
-- already charge this card?" must survive a Redis restart.
CREATE TABLE IF NOT EXISTS job_dedupe (
    key         text PRIMARY KEY,
    job_id      uuid NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
