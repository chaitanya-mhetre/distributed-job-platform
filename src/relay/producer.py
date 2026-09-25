"""BatchingProducer: coalesce many concurrent enqueue() calls into enqueue_many() batches.

Why: a single `enqueue()` costs three round trips (INSERT, XADD, UPDATE dispatched_at). A
producer submitting hundreds of jobs a second spends its time waiting on those round trips and
on the connection pool, not doing work. Micro-batching trades a tiny, bounded delay
(`max_delay_ms`) for amortising the three round trips over up to `max_batch` jobs.

    async with app.batching(max_batch=200, max_delay_ms=5) as producer:
        result = await producer.enqueue("send_email", {"to": "a@b.c"})

Each caller still gets its own EnqueueResult (or exception) back. A batch is flushed when it is
full or when its oldest item has waited `max_delay_ms`, whichever comes first. Several batches
can be in flight at once (up to `max_in_flight`), so one slow round trip doesn't stall the
producer. If a batch fails, every caller in that batch gets the exception; nothing is retried
silently, because the caller is the one who knows whether retrying is safe (use
idempotency_key if it has to be).
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from types import TracebackType
from typing import Any

from relay.app import EnqueueResult, JobSpec, Relay
from relay.models import Priority


class ProducerClosedError(RuntimeError):
    pass


class BatchingProducer:
    def __init__(
        self,
        relay: Relay,
        *,
        max_batch: int = 200,
        max_delay_ms: float = 5.0,
        max_in_flight: int = 4,
    ) -> None:
        if max_batch < 1:
            raise ValueError("max_batch must be >= 1")
        if max_delay_ms < 0:
            raise ValueError("max_delay_ms must be >= 0")
        self.relay = relay
        self.max_batch = max_batch
        self.max_delay_s = max_delay_ms / 1000
        self._pending: list[tuple[JobSpec, asyncio.Future[EnqueueResult]]] = []
        self._timer: asyncio.TimerHandle | None = None
        self._in_flight: set[asyncio.Task[None]] = set()
        self._slots = asyncio.Semaphore(max_in_flight)
        self._closed = False
        self.batches_sent = 0  # observability / tests: how well calls are being coalesced

    async def __aenter__(self) -> BatchingProducer:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: Priority | str | None = None,
        run_at: datetime | None = None,
        max_attempts: int | None = None,
        timeout_s: int | None = None,
        idempotency_key: str | None = None,
    ) -> EnqueueResult:
        """Same arguments and result as Relay.enqueue(); resolves when the batch is written."""
        if self._closed:
            raise ProducerClosedError("producer is closed")
        spec = JobSpec(
            job_type, payload, priority, run_at, max_attempts, timeout_s, idempotency_key
        )
        # Validate now, so a bad job type fails its own caller instead of the whole batch.
        self.relay._new_job(
            spec.type,
            spec.payload,
            priority=spec.priority,
            run_at=spec.run_at,
            max_attempts=spec.max_attempts,
            timeout_s=spec.timeout_s,
            idempotency_key=spec.idempotency_key,
        )
        fut: asyncio.Future[EnqueueResult] = asyncio.get_running_loop().create_future()
        self._pending.append((spec, fut))
        if len(self._pending) >= self.max_batch:
            self._flush()
        elif self._timer is None:
            self._timer = asyncio.get_running_loop().call_later(self.max_delay_s, self._flush)
        return await fut

    def _flush(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._pending:
            return
        batch, self._pending = self._pending, []
        task = asyncio.create_task(self._send(batch))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

    async def _send(self, batch: list[tuple[JobSpec, asyncio.Future[EnqueueResult]]]) -> None:
        async with self._slots:
            try:
                results = await self.relay.enqueue_many([spec for spec, _ in batch])
            except Exception as exc:  # every caller in the batch sees the failure
                for _, fut in batch:
                    if not fut.done():
                        fut.set_exception(exc)
                return
            self.batches_sent += 1
            for (_, fut), result in zip(batch, results, strict=True):
                if not fut.done():  # the caller may have been cancelled meanwhile
                    fut.set_result(result)

    async def aclose(self) -> None:
        """Stop accepting jobs, flush what is pending and wait for in-flight batches."""
        self._closed = True
        self._flush()
        if self._in_flight:
            await asyncio.gather(*self._in_flight, return_exceptions=True)
