"""JobContext: what a handler receives as its first argument."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncConnection

from relay.locks import Lease
from relay.models import Job

if TYPE_CHECKING:
    from relay.app import Relay


@dataclass
class JobContext:
    job: Job
    worker_id: str
    relay: Relay
    log: logging.LoggerAdapter[logging.Logger] = field(init=False)
    cancel_requested: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self.log = logging.LoggerAdapter(
            logging.getLogger("relay.job"),
            {"job_id": str(self.job.id), "job_type": self.job.type, "attempt": self.attempt},
        )

    @property
    def attempt(self) -> int:
        return self.job.attempts

    async def once(self, key: str) -> bool:
        """Durable "do this side effect only once" guard for idempotent handlers.

        Relay delivers at least once: a worker can finish a job and crash before acking, and
        the job runs again. Wrap non-repeatable side effects:

            if await ctx.once(f"charge:{order_id}"):
                await charge_card(order_id)

        Note the gap: if the process dies *after* once() but *before* the side effect, the
        retry will skip it. If the side effect is a write to Relay's Postgres database, use
        once_atomic() instead (no gap). For external calls (email, payments), pass an
        idempotency key to the downstream API and let it deduplicate.
        """
        return await self.relay.store.once(key, self.job.id)

    async def once_atomic(
        self, key: str, effect: Callable[[AsyncConnection], Awaitable[None]]
    ) -> bool:
        """Exactly-once *effect* for database writes: the dedupe key and the effect commit in
        the same transaction, so a crash can neither duplicate nor lose it.

            async def credit(conn: AsyncConnection) -> None:
                await conn.execute(text("INSERT INTO ledger ..."), {...})

            await ctx.once_atomic(f"credit:{order_id}", credit)

        Returns True if the effect ran now, False if it had already been committed.
        """
        return await self.relay.store.once_atomic(key, self.job.id, effect)

    def lock(
        self, resource: str, ttl_ms: int = 30_000, wait_s: float = 10.0
    ) -> AbstractAsyncContextManager[Lease]:
        """Distributed lock with a fencing token:

        async with ctx.lock(f"user:{user_id}") as lease:
            await write_somewhere(..., fence=lease.token)
        """
        return self.relay.locks.hold(resource, ttl_ms, wait_s)

    async def raise_if_cancelled(self) -> None:
        """For handlers doing long loops between awaits: check the cancel flag explicitly."""
        broker = self.relay.broker
        if await broker.redis.exists(broker.keys.cancel(str(self.job.id))):
            self.cancel_requested = True
            raise asyncio.CancelledError
