from collections import Counter

import pytest

from relay.broker import DEFAULT_WEIGHTS, WeightedOrder
from relay.models import Priority


def test_first_choice_follows_weights_exactly_per_cycle() -> None:
    order = WeightedOrder(DEFAULT_WEIGHTS)
    firsts = Counter(order.next_order()[0] for _ in range(100))
    assert firsts == {Priority.HIGH: 60, Priority.DEFAULT: 30, Priority.LOW: 10}


def test_every_order_contains_all_priorities_once() -> None:
    order = WeightedOrder(DEFAULT_WEIGHTS)
    for _ in range(20):
        assert sorted(order.next_order()) == sorted(Priority)


def test_picks_are_spread_not_bunched() -> None:
    order = WeightedOrder(DEFAULT_WEIGHTS)
    firsts = [order.next_order()[0] for _ in range(10)]
    # smooth WRR never picks `high` 6 times in a row
    runs = max(len(list(g)) for _, g in __import__("itertools").groupby(firsts))
    assert runs < 6
    assert Priority.LOW in firsts


def test_rejects_non_positive_weights() -> None:
    with pytest.raises(ValueError):
        WeightedOrder({Priority.HIGH: 0})
