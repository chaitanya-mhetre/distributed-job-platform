"""FastAPI app. `create_app(relay)` so the API and workers share one job registry."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status

from relay.api.schemas import JobIn, JobOut, JobPage
from relay.app import Relay, UnknownJobTypeError
from relay.models import JobStatus


def create_app(relay: Relay) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await relay.setup()
        yield
        await relay.close()

    app = FastAPI(title="Relay", version="0.1.0", lifespan=lifespan)

    def require_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
        expected = relay.settings.api_key
        if expected and x_api_key != expected:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-API-Key")

    async def limit_body(request: Request) -> None:
        length = request.headers.get("content-length")
        if length and int(length) > relay.settings.max_payload_bytes:
            raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "payload too large")

    auth = [Depends(require_key)]

    @app.post(
        "/v1/jobs",
        response_model=JobOut,
        dependencies=[*auth, Depends(limit_body)],
        status_code=status.HTTP_201_CREATED,
    )
    async def submit(body: JobIn, response: Response) -> JobOut:
        try:
            res = await relay.enqueue(
                body.type,
                body.payload,
                priority=body.priority,
                run_at=body.run_at,
                max_attempts=body.max_attempts,
                timeout_s=body.timeout_s,
                idempotency_key=body.idempotency_key,
            )
        except UnknownJobTypeError as exc:
            raise HTTPException(422, f"unknown job type {exc}") from exc
        if not res.created:
            response.status_code = status.HTTP_200_OK  # idempotent hit: same job as before
        return JobOut.of(res.job)

    @app.get("/v1/jobs/{job_id}", response_model=JobOut, dependencies=auth)
    async def get_job(job_id: UUID) -> JobOut:
        job = await relay.store.get(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        return JobOut.of(job, await relay.store.attempts(job_id))

    @app.get("/v1/jobs", response_model=JobPage, dependencies=auth)
    async def list_jobs(
        status_: Annotated[JobStatus | None, Query(alias="status")] = None,
        type_: Annotated[str | None, Query(alias="type")] = None,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> JobPage:
        jobs, nxt = await relay.store.list_jobs(
            status=status_, job_type=type_, cursor=cursor, limit=limit
        )
        return JobPage(items=[JobOut.of(j) for j in jobs], next_cursor=nxt)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> dict[str, str]:
        try:
            await relay.broker.redis.ping()
            await relay.store.status_counts()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(503, f"not ready: {exc}") from exc
        return {"status": "ready"}

    return app
