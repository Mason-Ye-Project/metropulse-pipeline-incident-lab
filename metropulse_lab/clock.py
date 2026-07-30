"""Deterministic logical clock.

The lab never reads wall-clock time for anything that becomes evidence or an
oracle. A run starts at a fixture-defined instant and advances by declared
logical offsets. The real current time may be recorded only in an optional,
non-oracle debug field and must not appear in captured book output.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def parse_utc(iso: str) -> datetime:
    """Parse an ISO 8601 UTC string ending in ``Z`` into an aware datetime."""
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_utc(dt: datetime) -> str:
    """Format an aware datetime as ISO 8601 UTC with a trailing ``Z``."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class LogicalClock:
    """A monotonic logical clock seeded at a fixture instant.

    ``tick`` advances by a fixed number of logical seconds and returns the new
    logical timestamp. It is used for framework events (state transitions) whose
    exact instant is not itself data under test.
    """

    def __init__(self, start_utc: str, step_seconds: int = 60) -> None:
        self._base = parse_utc(start_utc)
        self._offset = 0
        self._step = step_seconds

    def now(self) -> str:
        return format_utc(self._base + timedelta(seconds=self._offset))

    def tick(self, seconds: int | None = None) -> str:
        self._offset += self._step if seconds is None else seconds
        return self.now()

    def at_offset(self, seconds: int) -> str:
        return format_utc(self._base + timedelta(seconds=seconds))
