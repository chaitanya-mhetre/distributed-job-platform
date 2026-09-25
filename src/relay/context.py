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
