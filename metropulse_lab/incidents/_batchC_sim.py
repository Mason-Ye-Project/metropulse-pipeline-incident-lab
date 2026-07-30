"""Deterministic simulators for the batch-C incidents (MP-11 .. MP-15).

Every function here is pure: given the same inputs it returns byte-identical
Python data structures, with no wall clock, no host time zone, no real threads,
sleeps, sockets, processes, or external packages. The incident modules persist
these results into the control plane and incident-private tables and turn them
into evidence.

The simulators model *logical* offsets (integer seconds from a fixture instant),
never measured elapsed time. Concurrency is modelled by a declared event queue
with a fixed interleaving (lab_architecture.md 10.1), not by OS scheduling.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable

# ---------------------------------------------------------------------------
# MP-11 — station-status collection under a briefly-overloaded upstream.
# ---------------------------------------------------------------------------


def _seeded_offset_jitter(station: str, n: int) -> int:
    """Deterministic 0/1 logical-second jitter (hashlib, never builtin hash)."""
    return int(hashlib.sha256(f"{station}|{n}".encode("utf-8")).hexdigest(), 16) % 2


def upstream_response(offset_s: int, recovery_offset_s: int) -> str:
    """The fake station-status upstream: OK once it has recovered, else BUSY."""
    return "OK" if offset_s >= recovery_offset_s else "BUSY"


def single_owner_collect(
    station: str,
    recovery_offset_s: int,
    server_delay_s: int,
    backoff_base_s: int,
    max_attempts: int,
    use_backoff: bool,
    use_jitter: bool,
) -> list[dict[str, Any]]:
    """One owner, bounded attempts, honours the upstream's requested delay and
    spaces further attempts out. Returns the ordered attempt records."""
    attempts: list[dict[str, Any]] = []
    offset = 0
    for n in range(1, max_attempts + 1):
        cls = upstream_response(offset, recovery_offset_s)
        attempts.append({
            "layer": "owner", "n": n, "offset_s": offset,
            "response_class": cls, "succeeded": 1 if cls == "OK" else 0,
        })
        if cls == "OK":
            break
        step = server_delay_s
        if use_backoff:
            step += backoff_base_s * (2 ** (n - 1))
        if use_jitter:
            step += _seeded_offset_jitter(station, n)
        offset += step
    return attempts


def layered_immediate_collect(
    station: str,
    recovery_offset_s: int,
    layer_fanout: tuple[int, int, int],
) -> list[dict[str, Any]]:
    """Three independent owners each re-issue immediately, so the logical clock
    never advances past the upstream's overloaded phase. Returns every attempt
    the layered owners produced (all before the upstream recovered)."""
    attempts: list[dict[str, Any]] = []
    o, c, w = layer_fanout
    seq = 0
    for a in range(1, o + 1):
        for b in range(1, c + 1):
            for d in range(1, w + 1):
                seq += 1
                offset = 0  # immediate re-issue: no delay, clock stays put
                cls = upstream_response(offset, recovery_offset_s)
                attempts.append({
                    "layer": f"L{a}.{b}.{d}", "n": seq, "offset_s": offset,
                    "response_class": cls, "succeeded": 1 if cls == "OK" else 0,
                })
    return attempts


# ---------------------------------------------------------------------------
# MP-12 — long station run behind a simulated access grant (no real secret).
# ---------------------------------------------------------------------------


class AccessGrant:
    """A simulated, non-secret access grant.

    ``kind='refreshable'`` is always valid because it renews itself before it
    would lapse. ``kind='static'`` is issued once for a fixed logical lifetime
    and stops being valid once the run runs past it. There is no real
    credential, token string, or secret anywhere — only logical offsets.
    """

    def __init__(self, kind: str, issued_offset_s: int, lifetime_s: int) -> None:
        self.kind = kind
        self.issued_offset_s = issued_offset_s
        self.lifetime_s = lifetime_s

    def valid_at(self, offset_s: int) -> bool:
        if self.kind == "refreshable":
            return True
        return offset_s < self.issued_offset_s + self.lifetime_s


def run_station_pass(
    stations: list[tuple[int, str]],
    grant: AccessGrant,
    seconds_per_station: int,
    fail_fast: bool,
    generic_retries_on_denied: int,
) -> list[dict[str, Any]]:
    """Process stations in order. Each consumes ``seconds_per_station`` logical
    seconds. A station reads only while the grant is valid; otherwise it is
    denied. A generic retry (when ``fail_fast`` is false) pointlessly re-issues
    a deterministically-denied read a fixed number of times."""
    out: list[dict[str, Any]] = []
    for index, station_id in stations:
        offset = (index - 1) * seconds_per_station
        granted = grant.valid_at(offset)
        if granted:
            out.append({
                "station_index": index, "station_id": station_id,
                "offset_s": offset, "access_state": "GRANTED", "staged": 1,
                "extra_attempts": 0,
            })
        else:
            extra = 0 if fail_fast else generic_retries_on_denied
            out.append({
                "station_index": index, "station_id": station_id,
                "offset_s": offset, "access_state": "DENIED", "staged": 0,
                "extra_attempts": extra,
            })
    return out


# ---------------------------------------------------------------------------
# MP-13 — two checked-in environments and two parser builds for one input.
# ---------------------------------------------------------------------------

# Two local environment fingerprints. The reference environment is what the
# approved build resolved to; the fresh-runner environment is what a loosely
# constrained resolve produced later. The ``stampkit`` component (a date
# reader) differs between them and changes how an ambiguous date is read.
ENVIRONMENTS: dict[str, dict[str, tuple[str, str]]] = {
    "env_reference": {
        "metrocore": ("2.3.1", "mc231-4f1a9e"),
        "stampkit": ("1.4.2", "sk142-7b0c2d"),
        "gridcsv": ("0.9.0", "gc090-1122aa"),
    },
    "env_fresh_runner": {
        "metrocore": ("2.3.1", "mc231-4f1a9e"),
        "stampkit": ("1.5.0", "sk150-9d3e11"),
        "gridcsv": ("0.9.0", "gc090-1122aa"),
    },
}

# Input rows with day/month-ambiguous dates (both components <= 12), so each
# reader build produces a valid but different instant for the identical input.
STAMP_INPUT: tuple[tuple[int, str, str], ...] = (
    (1, "05/04/2026", "08:30"),
    (2, "03/07/2026", "16:15"),
    (3, "11/06/2026", "06:45"),
    (4, "02/12/2026", "17:05"),
)


def _stampkit_build(environment_id: str) -> str:
    return ENVIRONMENTS[environment_id]["stampkit"][0]


def parse_stamp(environment_id: str, day_field: str, time_field: str) -> str:
    """Parse an ambiguous ``dd/mm`` or ``mm/dd`` date under a given environment.

    The reference ``stampkit`` build reads the date as day-first; the newer
    build reads it as month-first. Same input string, different instant.
    """
    a, b, year = day_field.split("/")
    build = _stampkit_build(environment_id)
    if build == "1.4.2":
        day, month = int(a), int(b)      # day-first
    else:
        month, day = int(a), int(b)      # month-first
    hour, minute = time_field.split(":")
    return f"{int(year):04d}-{month:02d}-{day:02d}T{int(hour):02d}:{int(minute):02d}:00Z"


def parse_all(environment_id: str) -> list[dict[str, Any]]:
    out = []
    for row_id, day_field, time_field in STAMP_INPUT:
        out.append({
            "input_row_id": row_id,
            "raw_value": f"{day_field} {time_field}",
            "parsed_utc": parse_stamp(environment_id, day_field, time_field),
        })
    return out


# ---------------------------------------------------------------------------
# MP-14 — declared event queue for a scheduler that starts a second attempt.
# ---------------------------------------------------------------------------

# A step is (offset_s, actor, action, payload). Actions:
#   START        actor begins producing into the target
#   WRITE        actor appends (service_date, value)
#   LOSE_CONTACT the scheduler stops receiving actor's liveness signal
#   GIVEUP       the scheduler declares payload (an actor) timed out
#   COMMIT       actor commits
DAY1 = "2026-04-14"
DAY2 = "2026-04-15"

# The bug reproduction: attempt A loses contact, the scheduler gives up and
# starts B, but A is still alive and keeps producing into the same target.
FAULTY_INTERLEAVING: tuple[tuple[int, str, str, Any], ...] = (
    (0, "A", "START", None),
    (1, "A", "WRITE", (DAY1, 10)),
    (2, "A", "WRITE", (DAY1, 20)),
    (3, "A", "LOSE_CONTACT", None),
    (4, "SCHED", "GIVEUP", "A"),
    (5, "B", "START", None),
    (6, "B", "WRITE", (DAY2, 11)),
    (7, "A", "WRITE", (DAY1, 30)),
    (8, "B", "WRITE", (DAY2, 21)),
    (9, "A", "COMMIT", None),
    (10, "B", "COMMIT", None),
)

# The corrected run: one guarded producer P writes the full intended dataset; a
# late duplicate attempt X is refused before it can write anything.
INTENDED_VALUES: tuple[int, ...] = (10, 20, 30, 40, 50)
CLEAN_INTERLEAVING: tuple[tuple[int, str, str, Any], ...] = (
    (0, "P", "START", None),
    (1, "P", "WRITE", (DAY1, 10)),
    (2, "P", "WRITE", (DAY1, 20)),
    (3, "X", "START", None),
    (4, "P", "WRITE", (DAY1, 30)),
    (5, "X", "WRITE", (DAY2, 99)),
    (6, "P", "WRITE", (DAY1, 40)),
    (7, "P", "WRITE", (DAY1, 50)),
    (8, "P", "COMMIT", None),
)


def run_writer_queue(
    steps: tuple[tuple[int, str, str, Any], ...],
    enforce_guard: bool,
) -> dict[str, Any]:
    """Replay a declared interleaving deterministically.

    When ``enforce_guard`` is true, only the current guard holder may write and
    a second attempt is refused before writing — leaving at most one committing
    producer. When false, any active attempt writes into the shared target.
    """
    rows: list[dict[str, Any]] = []
    jobs: dict[str, dict[str, Any]] = {}
    timeline: list[dict[str, Any]] = []
    guard_holder: str | None = None
    guard_fence = 0
    row_id = 0

    def job(actor: str) -> dict[str, Any]:
        return jobs.setdefault(
            actor, {"attempt_id": actor, "state": "IDLE",
                    "fence": 0, "in_contact": 1, "rows_written": 0})

    for offset, actor, action, payload in steps:
        if action == "START":
            j = job(actor)
            if enforce_guard and guard_holder is not None and guard_holder != actor:
                j["state"] = "REFUSED"
                timeline.append({"offset_s": offset, "attempt_id": actor,
                                 "event": "REFUSED_SECOND_ATTEMPT"})
            else:
                guard_holder = actor
                guard_fence += 1
                j["state"] = "ACTIVE"
                j["fence"] = guard_fence
                timeline.append({"offset_s": offset, "attempt_id": actor,
                                 "event": "STARTED"})
        elif action == "WRITE":
            j = job(actor)
            service_date, value = payload
            allowed = (not enforce_guard) or (actor == guard_holder)
            if allowed and j["state"] in ("ACTIVE",):
                row_id += 1
                rows.append({"row_id": row_id, "producer_attempt": actor,
                             "service_date": service_date, "value": value})
                j["rows_written"] += 1
                timeline.append({"offset_s": offset, "attempt_id": actor,
                                 "event": "WROTE_ROW"})
            else:
                timeline.append({"offset_s": offset, "attempt_id": actor,
                                 "event": "WRITE_REFUSED"})
        elif action == "LOSE_CONTACT":
            j = job(actor)
            j["in_contact"] = 0
            timeline.append({"offset_s": offset, "attempt_id": actor,
                             "event": "SCHEDULER_LOST_CONTACT"})
        elif action == "GIVEUP":
            target_actor = payload
            timeline.append({"offset_s": offset, "attempt_id": target_actor,
                             "event": "SCHEDULER_GAVE_UP"})
            # In the faulty run the given-up attempt is NOT actually stopped.
        elif action == "COMMIT":
            j = job(actor)
            if j["state"] == "ACTIVE":
                j["state"] = "COMMITTED"
                timeline.append({"offset_s": offset, "attempt_id": actor,
                                 "event": "COMMITTED"})

    # Any attempt that started, never committed, and was never refused is still
    # holding on — the second-writer hazard the scheduler could not clear.
    for j in jobs.values():
        if j["state"] == "ACTIVE":
            j["state"] = "STILL_ACTIVE"
    return {"rows": rows, "jobs": jobs, "timeline": timeline, "fence": guard_fence}


# ---------------------------------------------------------------------------
# MP-15 — deterministic fake database-session pool.
# ---------------------------------------------------------------------------


class FakePool:
    """A deterministic stand-in for a fixed-capacity database-session pool.

    It records acquire/release/rollback and the number of in-use sessions over
    time. No SQLAlchemy, no PostgreSQL, no real sockets — the numbers come only
    from the declared task sequence.
    """

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.in_use = 0
        self.max_in_use = 0
        self.seq = 0
        self.sessions: dict[str, dict[str, Any]] = {}

    def acquire(self, task_id: str) -> str | None:
        if self.in_use >= self.capacity:
            return None
        self.seq += 1
        self.in_use += 1
        self.max_in_use = max(self.max_in_use, self.in_use)
        session_id = f"SESS-{task_id}"
        self.sessions[session_id] = {
            "session_id": session_id, "acquired_seq": self.seq,
            "released_seq": None, "state": "IN_USE",
        }
        return session_id

    def release(self, session_id: str) -> None:
        self.seq += 1
        s = self.sessions[session_id]
        s["released_seq"] = self.seq
        s["state"] = "RELEASED"
        self.in_use -= 1

    def rollback_and_release(self, session_id: str) -> None:
        self.seq += 1
        s = self.sessions[session_id]
        s["released_seq"] = self.seq
        s["state"] = "ROLLED_BACK"
        self.in_use -= 1

    def end_held_session(self, session_id: str) -> None:
        """Recovery: forcibly end a session the faulty run left held open."""
        s = self.sessions[session_id]
        if s["released_seq"] is None:
            self.seq += 1
            s["released_seq"] = self.seq
            s["state"] = "ENDED_BY_RECOVERY"
            self.in_use -= 1


def run_pool_tasks(
    pool: FakePool,
    task_ids: list[str],
    raises: Callable[[str], bool],
    release_on_error: bool,
) -> list[dict[str, Any]]:
    """Run each task against the pool. A task acquires a session, does work, and
    should return the session on every path. When ``release_on_error`` is false
    the error path skips the return, so the session stays held."""
    results: list[dict[str, Any]] = []
    for task_id in task_ids:
        session_id = pool.acquire(task_id)
        if session_id is None:
            results.append({"task_id": task_id, "outcome": "TIMED_OUT",
                            "session_id": None, "acquire_seq": None,
                            "session_state": "NONE"})
            continue
        acquire_seq = pool.sessions[session_id]["acquired_seq"]
        if raises(task_id):
            if release_on_error:
                pool.rollback_and_release(session_id)
                outcome, sstate = "FAILED_THEN_RETURNED", "ROLLED_BACK"
            else:
                outcome, sstate = "FAILED_HELD_OPEN", "HELD_OPEN"
        else:
            pool.release(session_id)
            outcome, sstate = "COMPLETED", "RELEASED"
        results.append({"task_id": task_id, "outcome": outcome,
                        "session_id": session_id, "acquire_seq": acquire_seq,
                        "session_state": sstate})
    return results
