-- M4: demo of a resource protected by fencing tokens (see relay/locks.py).
-- A write is applied only if its token is newer than the last applied one, so a lock holder
-- whose lease expired (GC pause, network stall) can't overwrite a newer holder's work.
CREATE TABLE IF NOT EXISTS fenced_writes (
    resource    text PRIMARY KEY,
    fence       bigint      NOT NULL,
    value       jsonb       NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
