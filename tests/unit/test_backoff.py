import random

import pytest

from relay.backoff import backoff_seconds


@pytest.mark.parametrize(("attempt", "raw"), [(1, 1.0), (2, 2.0), (3, 4.0), (6, 32.0)])
def test_exponential_with_jitter_bounds(attempt: int, raw: float) -> None:
    rng = random.Random(42)
    for _ in range(200):
        assert 0.5 * raw <= backoff_seconds(attempt, rng=rng) <= 1.5 * raw


def test_capped() -> None:
    rng = random.Random(1)
    assert all(backoff_seconds(30, cap_s=10, rng=rng) <= 15 for _ in range(100))


def test_jitter_spreads_values() -> None:
    rng = random.Random(7)
    values = {round(backoff_seconds(3, rng=rng), 3) for _ in range(50)}
    assert len(values) > 40  # not all retries at the same instant


def test_attempt_is_one_based() -> None:
    with pytest.raises(ValueError):
        backoff_seconds(0)
