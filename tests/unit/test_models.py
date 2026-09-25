import pytest

from relay.models import TERMINAL, TRANSITIONS, JobStatus, allowed_from, can_transition


def test_terminal_states_have_no_exits_except_manual_retry() -> None:
    for status in TERMINAL:
        assert status.is_terminal
        assert TRANSITIONS[status] <= {JobStatus.QUEUED}


@pytest.mark.parametrize(
    ("current", "target", "ok"),
    [
        (JobStatus.QUEUED, JobStatus.RUNNING, True),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED, True),
        (JobStatus.RUNNING, JobStatus.SCHEDULED, True),
        (JobStatus.SCHEDULED, JobStatus.RUNNING, False),  # must be promoted to queued first
        (JobStatus.SUCCEEDED, JobStatus.RUNNING, False),
        (JobStatus.CANCELLED, JobStatus.QUEUED, False),
        (JobStatus.DEAD, JobStatus.QUEUED, True),
    ],
)
def test_can_transition(current: JobStatus, target: JobStatus, ok: bool) -> None:
    assert can_transition(current, target) is ok


def test_allowed_from_is_the_inverse_of_transitions() -> None:
    assert allowed_from(JobStatus.RUNNING) == ["queued"]
    assert allowed_from(JobStatus.CANCELLED) == ["queued", "running", "scheduled"]
    for target in JobStatus:
        for source in allowed_from(target):
            assert target in TRANSITIONS[JobStatus(source)]
