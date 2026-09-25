"""Request/response models for the HTTP API."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from relay.broker import DeadLetter
from relay.models import Job, JobStatus, Priority
from relay.store import Attempt


class JobIn(BaseModel):
    type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Priority | None = None
    run_at: datetime | None = None
    max_attempts: int | None = Field(default=None, ge=1, le=50)
    timeout_s: int | None = Field(default=None, ge=1, le=24 * 3600)
    idempotency_key: str | None = Field(default=None, max_length=200)


class AttemptOut(BaseModel):
    attempt: int
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    outcome: str | None
    error: str | None
    duration_ms: int | None

    @classmethod
    def of(cls, a: Attempt) -> AttemptOut:
        return cls(**{f: getattr(a, f) for f in cls.model_fields})


class JobOut(BaseModel):
    id: UUID
    type: str
    priority: Priority
    status: JobStatus
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    timeout_s: int
    run_at: datetime
    idempotency_key: str | None
    result: Any
    last_error: str | None
    created_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    history: list[AttemptOut] | None = None

    @classmethod
    def of(cls, job: Job, history: list[Attempt] | None = None) -> JobOut:
        fields = {f: getattr(job, f) for f in cls.model_fields if f != "history"}
        return cls(**fields, history=[AttemptOut.of(a) for a in history] if history else None)


class JobPage(BaseModel):
    items: list[JobOut]
    next_cursor: str | None


class DeadLetterOut(BaseModel):
    entry_id: str
    job_id: UUID
    type: str
    error: str

    @classmethod
    def of(cls, d: DeadLetter) -> DeadLetterOut:
        return cls(entry_id=d.entry_id, job_id=d.job_id, type=d.job_type, error=d.error)
