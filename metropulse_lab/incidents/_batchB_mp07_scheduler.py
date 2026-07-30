"""MP-07 deterministic mini-scheduler (no real orchestrator).

Models a partitioned "route-day service report" job. A run resolves a
source/target window from either an explicit validated request (correct) or from
a frozen wall-clock instant mixed with a mismatched default window (faulty). The
resolved window renders the read and write paths; the write path's date is the
partition the run actually refreshes.

The clock is frozen to a fixed instant defined here (a checked-in constant, never
the host clock), so both paths are byte-deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

# Frozen "wall-clock" instant the faulty data logic reaches for. It is the trigger
# day, one day after the service window — a checked-in constant, not host time.
FROZEN_TRIGGER_WALL_CLOCK = "2026-04-16T09:30:00Z"

# The three service-day partitions that exist in this scenario.
PARTITIONS = ("2026-04-13", "2026-04-14", "2026-04-15")
# The engineer manually re-triggers a fix for this service day.
REQUESTED_SERVICE_DATE = "2026-04-14"


def scheduled_content(service_date: str) -> str:
    """Correct content token a scheduled run writes for a partition."""
    return f"SCHED-{service_date}"


def fix_content(service_date: str) -> str:
    """Correct content token the manual fix writes for its target partition."""
    return f"FIX-{service_date}"


def _prev_day(service_date: str) -> str:
    y, m, d = (int(x) for x in service_date.split("-"))
    return (date(y, m, d) - timedelta(days=1)).isoformat()


@dataclass(frozen=True)
class ResolvedRun:
    requested_service_date: str
    window_start: str          # source/target window start (date)
    window_end: str            # exclusive end (date)
    read_path: str
    write_path: str
    write_partition: str       # the partition actually refreshed
    validated: bool


def _render(service_date: str, window_start: str, window_end: str,
            write_partition: str, validated: bool) -> ResolvedRun:
    read_path = f"landing/route_day/service_date={window_start}"
    write_path = f"warehouse/mart_route_day/service_date={write_partition}"
    return ResolvedRun(
        requested_service_date=service_date,
        window_start=window_start,
        window_end=window_end,
        read_path=read_path,
        write_path=write_path,
        write_partition=write_partition,
        validated=validated,
    )


def resolve_manual_run(requested_service_date: str, faulty: bool) -> ResolvedRun:
    """Resolve a manual re-trigger to a concrete window and write partition.

    Correct: use the explicit validated window ``[D, D+1)`` and write partition D.
    Faulty: the data logic derives the window from the frozen wall clock and a
    mismatched default, landing on the day before D.
    """
    y, m, d = (int(x) for x in requested_service_date.split("-"))
    next_day = (date(y, m, d) + timedelta(days=1)).isoformat()
    if not faulty:
        return _render(requested_service_date, requested_service_date, next_day,
                       requested_service_date, validated=True)
    # Faulty: default window trails one day behind the request; write partition is
    # the day before the requested date.
    wrong = _prev_day(requested_service_date)
    return _render(requested_service_date, wrong, requested_service_date,
                   wrong, validated=False)


def correct_final_partitions() -> dict[str, str]:
    """The partition contents after a correctly targeted manual fix for D."""
    out = {p: scheduled_content(p) for p in PARTITIONS}
    out[REQUESTED_SERVICE_DATE] = fix_content(REQUESTED_SERVICE_DATE)
    return out
