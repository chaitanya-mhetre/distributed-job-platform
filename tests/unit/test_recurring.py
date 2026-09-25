from datetime import UTC, datetime

import pytest

from relay.recurring import latest_fire, validate_cron

T0 = datetime(2026, 9, 25, 10, 0, tzinfo=UTC)


def test_nothing_due_before_next_period() -> None:
    assert latest_fire("*/5 * * * *", T0, T0.replace(minute=4)) is None


def test_due_exactly_on_period() -> None:
    assert latest_fire("*/5 * * * *", T0, T0.replace(minute=5)) == T0.replace(minute=5)


def test_no_backfill_only_latest_missed_period() -> None:
    # scheduler was down 10:00 -> 10:23: fire once, for 10:20
    assert latest_fire("*/5 * * * *", T0, T0.replace(minute=23)) == T0.replace(minute=20)


def test_seconds_field() -> None:
    assert latest_fire("* * * * * */10", T0, T0.replace(second=25)) == T0.replace(second=20)


def test_validate() -> None:
    validate_cron("0 3 * * *")
    with pytest.raises(ValueError):
        validate_cron("every tuesday")
