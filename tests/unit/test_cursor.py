from datetime import UTC, datetime
from uuid import uuid4

from relay.store import decode_cursor, encode_cursor


def test_cursor_roundtrip() -> None:
    ts, job_id = datetime(2026, 9, 25, 10, 0, tzinfo=UTC), uuid4()
    assert decode_cursor(encode_cursor(ts, job_id)) == (ts, job_id)
