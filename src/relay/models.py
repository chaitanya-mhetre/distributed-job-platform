"""Core domain types: job status state machine, priorities, and the job record.

The state machine lives here as *data* (a dict of allowed moves) so both the pure unit tests
and the SQL layer (which only updates rows whose current status is an allowed "from" state)
use exactly the same rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class JobStatus(StrEnum):
    QUEUED = "queued"  # in a stream, waiting for a worker
    SCHEDULED = "scheduled"  # in the delayed ZSET (future run_at, or waiting for a retry)
    RUNNING = "running"  # a worker is executing it
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # handler raised PermanentError: no retry, no DLQ
    DEAD = "dead"  # retries exhausted: moved to the dead-letter stream
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL


TERMINAL = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD, JobStatus.CANCELLED})

# status -> statuses it may move to
TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset(
        # SCHEDULED: deferred because a per-type concurrency limit was full
        {JobStatus.RUNNING, JobStatus.CANCELLED, JobStatus.SCHEDULED}
    ),
    JobStatus.SCHEDULED: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.DEAD,
            JobStatus.SCHEDULED,  # retry with backoff
            JobStatus.CANCELLED,
            JobStatus.QUEUED,  # reclaimed from a dead worker
        }
    ),
    JobStatus.FAILED: frozenset({JobStatus.QUEUED}),  # manual retry
    JobStatus.DEAD: frozenset({JobStatus.QUEUED}),  # manual retry / DLQ replay
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}


def can_transition(current: JobStatus, target: JobStatus) -> bool:
    return target in TRANSITIONS[current]


def allowed_from(target: JobStatus) -> list[str]:
    """Every status that may move to `target`. Used in SQL: `WHERE status = ANY(:from)`."""
    return sorted(s.value for s, targets in TRANSITIONS.items() if target in targets)


class Priority(StrEnum):
    HIGH = "high"
    DEFAULT = "default"
    LOW = "low"


class PermanentError(Exception):
    """Raise from a handler when retrying cannot help (bad input, 4xx from an API...)."""


@dataclass(frozen=True, slots=True)
class Job:
    id: UUID
    type: str
    priority: Priority
    payload: dict[str, Any]
    status: JobStatus
    attempts: int
    max_attempts: int
    timeout_s: int
    run_at: datetime
    idempotency_key: str | None = None
    result: Any = None
    last_error: str | None = None
    locked_by: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
