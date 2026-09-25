"""Property-based test of the job state machine (Hypothesis generates the event sequences)."""

from hypothesis import given
from hypothesis import strategies as st

from relay.models import JobStatus, can_transition

# events a job can experience; each maps to the status it would move to
EVENTS = list(JobStatus)
AUTOMATIC = [s for s in JobStatus if s is not JobStatus.QUEUED]  # everything but manual retry


@given(st.lists(st.sampled_from(EVENTS), max_size=40))
def test_succeeded_and_cancelled_are_absorbing(events: list[JobStatus]) -> None:
    status = JobStatus.QUEUED
    for target in events:
        if can_transition(status, target):
            status = target
        if status in (JobStatus.SUCCEEDED, JobStatus.CANCELLED):
            # once here, no event sequence can ever move the job again
            assert all(not can_transition(status, t) for t in JobStatus)


@given(st.lists(st.sampled_from(AUTOMATIC), max_size=40))
def test_without_manual_retry_a_job_reaches_at_most_one_terminal_state(
    events: list[JobStatus],
) -> None:
    status = JobStatus.QUEUED
    terminals: list[JobStatus] = []
    for target in events:
        if can_transition(status, target):
            status = target
            if status.is_terminal:
                terminals.append(status)
    assert len(terminals) <= 1


@given(st.sampled_from(EVENTS))
def test_nothing_runs_without_being_queued_first(start: JobStatus) -> None:
    if can_transition(start, JobStatus.RUNNING):
        assert start is JobStatus.QUEUED
