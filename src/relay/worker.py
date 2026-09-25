"""Worker: reads job ids from Redis, runs the handler, records the outcome in Postgres.

M1 version: a single async loop with a concurrency limit. Failure handling (timeouts,
retries, heartbeats, graceful shutdown) arrives in M2.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
import uuid

from relay.app import Relay
from relay.broker import Message
from relay.context import JobContext
from relay.models import Priority

log = logging.getLogger("relay.worker")


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


class Worker:
    def __init__(self, relay: Relay, *, concurrency: int = 10, block_ms: int = 1000) -> None:
        self.relay = relay
        self.concurrency = concurrency
        self.block_ms = block_ms
        self.worker_id = make_worker_id()
        self._slots = asyncio.Semaphore(concurrency)
        self._tasks: set[asyncio.Task[None]] = set()
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        await self.relay.setup()
        log.info("worker started", extra={"worker_id": self.worker_id})
        order = [Priority.DEFAULT]
        while not self._stopping.is_set():
            free = self.concurrency - len(self._tasks)
            if free <= 0:
                await asyncio.sleep(0.01)
                continue
            msgs = await self.relay.broker.read(self.worker_id, order, free, self.block_ms)
            for msg in msgs:
                task = asyncio.create_task(self._handle(msg))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _handle(self, msg: Message) -> None:
        async with self._slots:
            store, broker = self.relay.store, self.relay.broker
            job = await store.start_attempt(msg.job_id, self.worker_id)
            if job is None:  # cancelled, or a duplicate delivery of a finished job
                await broker.ack(msg)
                return
            definition = self.relay.registry.get(job.type)
            started = time.monotonic()
            if definition is None:
                await store.mark_failed(job.id, f"no handler registered for {job.type!r}")
                await broker.ack(msg)
                return
            ctx = JobContext(job, self.worker_id, self.relay)
            try:
                result = await definition.fn(ctx, **job.payload)
            except Exception as exc:  # noqa: BLE001 - any handler error is a job failure
                error = f"{type(exc).__name__}: {exc}"
                await store.mark_failed(job.id, error)
                await store.close_attempt(job.id, job.attempts, "failed", error, _ms(started))
            else:
                await store.mark_succeeded(job.id, result)
                await store.close_attempt(job.id, job.attempts, "succeeded", None, _ms(started))
            await broker.ack(msg)


def _ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)
