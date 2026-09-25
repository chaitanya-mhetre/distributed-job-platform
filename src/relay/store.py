"""JobStore: every SQL statement Relay runs lives here.

Postgres is the source of truth for job status and history. Redis only moves job *ids* around.
Every status change goes through `_transition`, which only updates the row if its current
status is an allowed "from" state (see `models.TRANSITIONS`). That turns races such as
"cancel arrives while the worker finishes" into a harmless no-op instead of a corrupted row.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import RowMapping, text
from sqlalchemy.ext.asyncio import AsyncEngine

from relay.models import Job, JobStatus, Priority, allowed_from


@dataclass(frozen=True, slots=True)
class NewJob:
    type: str
    payload: dict[str, Any]
    priority: Priority = Priority.DEFAULT
    run_at: datetime | None = None
    max_attempts: int = 3
    timeout_s: int = 60
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class Attempt:
    attempt: int
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    outcome: str | None
    error: str | None
    duration_ms: int | None


def _row_to_job(row: RowMapping) -> Job:
    return Job(
        id=row["id"],
        type=row["type"],
        priority=Priority(row["priority"]),
        payload=row["payload"],
        status=JobStatus(row["status"]),
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
        timeout_s=row["timeout_s"],
        run_at=row["run_at"],
        idempotency_key=row["idempotency_key"],
        result=row["result"],
        last_error=row["last_error"],
        locked_by=row["locked_by"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def encode_cursor(created_at: datetime, job_id: UUID) -> str:
    return base64.urlsafe_b64encode(f"{created_at.isoformat()}|{job_id}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    ts, job_id = raw.split("|", 1)
    return datetime.fromisoformat(ts), UUID(job_id)


class JobStore:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        # For single-statement writes on the hot path: no BEGIN/COMMIT round trips. Each
        # statement is still atomic on its own (Postgres runs it in an implicit transaction).
        self.autocommit = engine.execution_options(isolation_level="AUTOCOMMIT")

    # --- create / read ----------------------------------------------------------------

    async def create(self, new: NewJob) -> tuple[Job, bool]:
        """Insert a job. Returns (job, created). created=False means an idempotent hit:
        a job with the same (type, idempotency_key) already exists and is returned instead."""
        now = datetime.now(UTC)
        run_at = new.run_at or now
        status = JobStatus.SCHEDULED if run_at > now else JobStatus.QUEUED
        params = {
            "id": uuid4(),
            "type": new.type,
            "priority": new.priority.value,
            "payload": json.dumps(new.payload),
            "key": new.idempotency_key,
            "status": status.value,
            "max_attempts": new.max_attempts,
            "timeout_s": new.timeout_s,
            "run_at": run_at,
        }
        async with self.engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            """
                        INSERT INTO jobs (id, type, priority, payload, idempotency_key, status,
                                          max_attempts, timeout_s, run_at)
                        VALUES (:id, :type, :priority, CAST(:payload AS jsonb), :key, :status,
                                :max_attempts, :timeout_s, :run_at)
                        ON CONFLICT (type, idempotency_key) WHERE idempotency_key IS NOT NULL
                        DO NOTHING
                        RETURNING *
                        """
                        ),
                        params,
                    )
                )
                .mappings()
                .first()
            )
            if row is not None:
                return _row_to_job(row), True
            existing = (
                (
                    await conn.execute(
                        text("SELECT * FROM jobs WHERE type = :type AND idempotency_key = :key"),
                        {"type": new.type, "key": new.idempotency_key},
                    )
                )
                .mappings()
                .one()
            )
            return _row_to_job(existing), False

    async def get(self, job_id: UUID) -> Job | None:
        async with self.engine.connect() as conn:
            row = (
                (await conn.execute(text("SELECT * FROM jobs WHERE id = :id"), {"id": job_id}))
                .mappings()
                .first()
            )
        return _row_to_job(row) if row else None

    async def get_many(self, job_ids: Sequence[UUID]) -> dict[UUID, Job]:
        if not job_ids:
            return {}
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("SELECT * FROM jobs WHERE id = ANY(:ids)"), {"ids": list(job_ids)}
                )
            ).mappings()
            return {r["id"]: _row_to_job(r) for r in rows}

    async def list_jobs(
        self,
        *,
        status: JobStatus | None = None,
        job_type: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[Job], str | None]:
        """Newest first, keyset-paginated on (created_at, id). Keyset beats OFFSET because the
        database seeks straight to the cursor instead of scanning and discarding rows."""
        clauses = ["TRUE"]
        params: dict[str, Any] = {"limit": limit + 1}
        if status is not None:
            clauses.append("status = :status")
            params["status"] = status.value
        if job_type is not None:
            clauses.append("type = :type")
            params["type"] = job_type
        if cursor is not None:
            params["c_ts"], params["c_id"] = decode_cursor(cursor)
            clauses.append("(created_at, id) < (:c_ts, :c_id)")
        sql = (
            f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} "  # noqa: S608 - clauses are constants
            "ORDER BY created_at DESC, id DESC LIMIT :limit"
        )
        async with self.engine.connect() as conn:
            rows = list((await conn.execute(text(sql), params)).mappings())
        jobs = [_row_to_job(r) for r in rows[:limit]]
        next_cursor = None
        if len(rows) > limit and jobs:
            last = jobs[-1]
            assert last.created_at is not None
            next_cursor = encode_cursor(last.created_at, last.id)
        return jobs, next_cursor

    async def attempts(self, job_id: UUID) -> list[Attempt]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT attempt, worker_id, started_at, finished_at, outcome, error,"
                        " duration_ms FROM job_attempts WHERE job_id = :id ORDER BY id"
                    ),
                    {"id": job_id},
                )
            ).mappings()
            return [Attempt(**dict(r)) for r in rows]

    # --- dispatch bookkeeping -----------------------------------------------------------

    async def mark_dispatched(self, job_ids: Sequence[UUID]) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text("UPDATE jobs SET dispatched_at = now() WHERE id = ANY(:ids)"),
                {"ids": list(job_ids)},
            )

    async def undispatched(self, older_than_s: float, limit: int = 500) -> list[Job]:
        """Queued/scheduled rows never handed to Redis (crash between INSERT and XADD)."""
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        """
                        SELECT * FROM jobs
                        WHERE dispatched_at IS NULL
                          AND status IN ('queued', 'scheduled')
                          AND created_at < now() - make_interval(secs => :age)
                        ORDER BY created_at LIMIT :limit
                        """
                    ),
                    {"age": older_than_s, "limit": limit},
                )
            ).mappings()
            return [_row_to_job(r) for r in rows]

    # --- state transitions --------------------------------------------------------------

    async def _transition(
        self,
        job_id: UUID,
        target: JobStatus,
        sets: str = "",
        params: dict[str, Any] | None = None,
    ) -> Job | None:
        """Move a job to `target` if (and only if) its current status allows it.
        Returns the updated job, or None if the transition was not allowed."""
        extra = f", {sets}" if sets else ""
        sql = (
            f"UPDATE jobs SET status = :target, updated_at = now(){extra} "  # noqa: S608
            "WHERE id = :id AND status = ANY(:from) RETURNING *"
        )
        async with self.engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(sql),
                        {"id": job_id, "target": target.value, "from": allowed_from(target)}
                        | (params or {}),
                    )
                )
                .mappings()
                .first()
            )
        return _row_to_job(row) if row else None

    async def start_attempt(self, job_id: UUID, worker_id: str) -> Job | None:
        """queued -> running, attempts += 1, and open a job_attempts row.

        One statement (a data-modifying CTE), so it's atomic and costs one round trip. The
        first version used a transaction with two statements; see docs/benchmarks.md for
        what that cost.
        """
        async with self.autocommit.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            """
                        WITH j AS (
                            UPDATE jobs SET status = 'running', attempts = attempts + 1,
                                   locked_by = :worker, started_at = now(), updated_at = now()
                            WHERE id = :id AND status = 'queued'
                            RETURNING *
                        ), a AS (
                            INSERT INTO job_attempts (job_id, attempt, worker_id)
                            SELECT id, attempts, :worker FROM j
                        )
                        SELECT * FROM j
                        """
                        ),
                        {"id": job_id, "worker": worker_id},
                    )
                )
                .mappings()
                .first()
            )
        return _row_to_job(row) if row else None

    async def finish(
        self,
        job_id: UUID,
        target: JobStatus,
        *,
        attempt: int,
        outcome: str,
        error: str | None,
        duration_ms: int,
        sets: str = "",
        params: dict[str, Any] | None = None,
    ) -> Job | None:
        """Record the end of an attempt: move the job to `target` (if allowed) and close the
        job_attempts row, in one statement / one round trip."""
        extra = f", {sets}" if sets else ""
        sql = f"""
            WITH j AS (
                UPDATE jobs SET status = :target, updated_at = now(){extra}
                WHERE id = :id AND status = ANY(:from)
                RETURNING *
            ), a AS (
                UPDATE job_attempts SET finished_at = now(), outcome = :outcome,
                       error = :error, duration_ms = :ms
                WHERE job_id = :id AND attempt = :attempt AND finished_at IS NULL
            )
            SELECT * FROM j
        """  # noqa: S608 - `sets` is built by the worker from constants, never user input
        async with self.autocommit.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text(sql),
                        {
                            "id": job_id,
                            "target": target.value,
                            "from": allowed_from(target),
                            "attempt": attempt,
                            "outcome": outcome,
                            "error": error,
                            "ms": duration_ms,
                        }
                        | (params or {}),
                    )
                )
                .mappings()
                .first()
            )
        return _row_to_job(row) if row else None

    async def close_attempt(
        self, job_id: UUID, attempt: int, outcome: str, error: str | None, duration_ms: int
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE job_attempts SET finished_at = now(), outcome = :outcome,
                           error = :error, duration_ms = :ms
                    WHERE job_id = :id AND attempt = :attempt AND finished_at IS NULL
                    """
                ),
                {
                    "id": job_id,
                    "attempt": attempt,
                    "outcome": outcome,
                    "error": error,
                    "ms": duration_ms,
                },
            )

    async def mark_succeeded(self, job_id: UUID, result: Any) -> Job | None:
        return await self._transition(
            job_id,
            JobStatus.SUCCEEDED,
            "result = CAST(:result AS jsonb), finished_at = now(), locked_by = NULL,"
            " last_error = NULL",
            {"result": json.dumps(result)},
        )

    async def mark_failed(self, job_id: UUID, error: str) -> Job | None:
        return await self._transition(
            job_id,
            JobStatus.FAILED,
            "last_error = :error, finished_at = now(), locked_by = NULL",
            {"error": error},
        )

    async def mark_dead(self, job_id: UUID, error: str) -> Job | None:
        return await self._transition(
            job_id,
            JobStatus.DEAD,
            "last_error = :error, finished_at = now(), locked_by = NULL",
            {"error": error},
        )

    async def mark_retry(self, job_id: UUID, run_at: datetime, error: str) -> Job | None:
        return await self._transition(
            job_id,
            JobStatus.SCHEDULED,
            "run_at = :run_at, last_error = :error, locked_by = NULL",
            {"run_at": run_at, "error": error},
        )

    async def defer(self, job_id: UUID, run_at: datetime) -> Job | None:
        """queued -> scheduled without using an attempt (per-type concurrency limit full)."""
        return await self._transition(
            job_id, JobStatus.SCHEDULED, "run_at = :run_at", {"run_at": run_at}
        )

    async def cancel_if_pending(self, job_id: UUID) -> Job | None:
        """queued/scheduled -> cancelled. Running jobs are cancelled cooperatively instead."""
        async with self.engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "UPDATE jobs SET status = 'cancelled', finished_at = now(),"
                            " updated_at = now()"
                            " WHERE id = :id AND status IN ('queued', 'scheduled') RETURNING *"
                        ),
                        {"id": job_id},
                    )
                )
                .mappings()
                .first()
            )
        return _row_to_job(row) if row else None

    async def mark_cancelled(self, job_id: UUID) -> Job | None:
        return await self._transition(
            job_id, JobStatus.CANCELLED, "finished_at = now(), locked_by = NULL"
        )

    async def mark_queued(self, job_id: UUID, *, reset_attempts: bool = False) -> Job | None:
        sets = "locked_by = NULL, finished_at = NULL"
        if reset_attempts:
            sets += ", attempts = 0, last_error = NULL"
        return await self._transition(job_id, JobStatus.QUEUED, sets)

    async def promote_many(self, job_ids: Sequence[UUID]) -> None:
        """scheduled -> queued for jobs the scheduler just moved from the ZSET to a stream."""
        if not job_ids:
            return
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE jobs SET status = 'queued', updated_at = now()"
                    " WHERE id = ANY(:ids) AND status = 'scheduled'"
                ),
                {"ids": list(job_ids)},
            )

    async def status_counts(self) -> dict[str, int]:
        async with self.engine.connect() as conn:
            rows = await conn.execute(text("SELECT status, count(*) FROM jobs GROUP BY status"))
            return {str(s): int(c) for s, c in rows}

    # --- once / dedupe ------------------------------------------------------------------

    async def once(self, key: str, job_id: UUID) -> bool:
        """True the first time `key` is seen, False ever after."""
        async with self.engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "INSERT INTO job_dedupe (key, job_id) VALUES (:key, :job_id)"
                        " ON CONFLICT (key) DO NOTHING RETURNING key"
                    ),
                    {"key": key, "job_id": job_id},
                )
            ).first()
        return row is not None

    # --- recurring jobs -----------------------------------------------------------------

    async def create_recurring(self, r: NewRecurring) -> RecurringJob:
        async with self.engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            """
                        INSERT INTO recurring_jobs (id, name, cron, type, payload, priority,
                                                    enabled, last_enqueued_at)
                        VALUES (:id, :name, :cron, :type, CAST(:payload AS jsonb), :priority,
                                :enabled, now())
                        RETURNING *
                        """
                        ),
                        {
                            "id": uuid4(),
                            "name": r.name,
                            "cron": r.cron,
                            "type": r.type,
                            "payload": json.dumps(r.payload),
                            "priority": r.priority.value,
                            "enabled": r.enabled,
                        },
                    )
                )
                .mappings()
                .one()
            )
        return RecurringJob.of(row)

    async def list_recurring(self, *, enabled_only: bool = False) -> list[RecurringJob]:
        sql = "SELECT * FROM recurring_jobs"
        if enabled_only:
            sql += " WHERE enabled"
        async with self.engine.connect() as conn:
            rows = (await conn.execute(text(sql + " ORDER BY name"))).mappings()
            return [RecurringJob.of(r) for r in rows]

    async def get_recurring(self, rid: UUID) -> RecurringJob | None:
        async with self.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text("SELECT * FROM recurring_jobs WHERE id = :id"), {"id": rid}
                    )
                )
                .mappings()
                .first()
            )
        return RecurringJob.of(row) if row else None

    async def update_recurring(self, rid: UUID, **fields: Any) -> RecurringJob | None:
        allowed = {"cron", "payload", "priority", "enabled"}
        sets: list[str] = []
        params: dict[str, Any] = {"id": rid}
        for k, v in fields.items():
            if k not in allowed or v is None:
                continue
            if k == "payload":
                sets.append("payload = CAST(:payload AS jsonb)")
                params[k] = json.dumps(v)
            else:
                sets.append(f"{k} = :{k}")
                params[k] = v.value if isinstance(v, Priority) else v
        if not sets:
            return await self.get_recurring(rid)
        async with self.engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        # column names come from the `allowed` set above, never from user input
                        text(
                            f"UPDATE recurring_jobs SET {', '.join(sets)}"
                            " WHERE id = :id RETURNING *"
                        ),
                        params,
                    )
                )
                .mappings()
                .first()
            )
        return RecurringJob.of(row) if row else None

    async def delete_recurring(self, rid: UUID) -> bool:
        async with self.engine.begin() as conn:
            res = await conn.execute(text("DELETE FROM recurring_jobs WHERE id = :id"), {"id": rid})
        return bool(res.rowcount)

    async def set_last_enqueued(self, rid: UUID, fire_at: datetime) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE recurring_jobs SET last_enqueued_at = :t"
                    " WHERE id = :id AND (last_enqueued_at IS NULL OR last_enqueued_at < :t)"
                ),
                {"id": rid, "t": fire_at},
            )


@dataclass(frozen=True, slots=True)
class NewRecurring:
    name: str
    cron: str
    type: str
    payload: dict[str, Any]
    priority: Priority = Priority.DEFAULT
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class RecurringJob:
    id: UUID
    name: str
    cron: str
    type: str
    payload: dict[str, Any]
    priority: Priority
    enabled: bool
    last_enqueued_at: datetime | None
    created_at: datetime

    @classmethod
    def of(cls, row: RowMapping) -> RecurringJob:
        return cls(
            id=row["id"],
            name=row["name"],
            cron=row["cron"],
            type=row["type"],
            payload=row["payload"],
            priority=Priority(row["priority"]),
            enabled=row["enabled"],
            last_enqueued_at=row["last_enqueued_at"],
            created_at=row["created_at"],
        )


async def fenced_write(engine: AsyncEngine, resource: str, token: int, value: Any) -> bool:
    """Write `value` for `resource` only if `token` is newer than the last applied token.
    Returns False when a stale lock holder is rejected."""
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text(
                    """
                    INSERT INTO fenced_writes (resource, fence, value)
                    VALUES (:r, :t, CAST(:v AS jsonb))
                    ON CONFLICT (resource) DO UPDATE
                        SET fence = EXCLUDED.fence, value = EXCLUDED.value, updated_at = now()
                        WHERE fenced_writes.fence < EXCLUDED.fence
                    RETURNING resource
                    """
                ),
                {"r": resource, "t": token, "v": json.dumps(value)},
            )
        ).first()
    return row is not None
