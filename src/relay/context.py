"""JobContext: what a handler receives as its first argument."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from relay.models import Job

if TYPE_CHECKING:
    from relay.app import Relay


@dataclass
class JobContext:
    job: Job
    worker_id: str
    relay: Relay
    log: logging.LoggerAdapter[logging.Logger] = field(init=False)

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
        retry will skip it. For money, prefer an idempotency key on the downstream API call.
        """
        return await self.relay.store.once(key, self.job.id)
