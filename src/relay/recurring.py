"""Cron helpers for recurring jobs."""

from __future__ import annotations

from datetime import datetime

from croniter import croniter


def validate_cron(expr: str) -> None:
    if not croniter.is_valid(expr):
        raise ValueError(f"invalid cron expression {expr!r}")


def latest_fire(expr: str, after: datetime, now: datetime) -> datetime | None:
    """The most recent fire time in (after, now], or None if nothing is due.

    If the scheduler was down for several periods we fire *once* for the latest missed period
    rather than replaying every missed run (no backfill). Most recurring work ("send the daily
    digest") should not run five times in a row after an outage.
    """
    it = croniter(expr, after)
    nxt: datetime = it.get_next(datetime)
    if nxt > now:
        return None
    latest = nxt
    while True:
        candidate: datetime = it.get_next(datetime)
        if candidate > now:
            return latest
        latest = candidate
