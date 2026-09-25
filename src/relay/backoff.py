"""Retry delay: capped exponential backoff with jitter.

    delay = min(cap, base * 2 ** (attempt - 1)) * uniform(0.5, 1.5)

Exponential growth gives a struggling dependency room to recover. The random factor
("jitter") spreads retries out: without it, 1,000 jobs that failed together during an outage
would all retry at the same instant and knock the dependency over again (thundering herd).
"""

from __future__ import annotations

import random


def backoff_seconds(
    attempt: int,
    *,
    base_s: float = 1.0,
    cap_s: float = 300.0,
    rng: random.Random | None = None,
) -> float:
    """Delay before retrying after failed attempt number `attempt` (1-based)."""
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    raw = min(cap_s, base_s * 2.0 ** (attempt - 1))
    uniform = rng.uniform if rng else random.uniform
    jitter: float = uniform(0.5, 1.5)
    return raw * jitter
