"""Lifecycle state machine.

State is tracked explicitly in ``control.sqlite3`` rather than inferred from
which files happen to exist (lab_architecture.md section 8). Each command
validates its allowed predecessor states before acting.
"""

from __future__ import annotations

import sqlite3

# Canonical ordered lifecycle states.
CREATED = "CREATED"
BASELINED = "BASELINED"
INJECTED = "INJECTED"
EXECUTED = "EXECUTED"
DETECTED = "DETECTED"
EVIDENCE_COLLECTED = "EVIDENCE_COLLECTED"
CONTAINED = "CONTAINED"
REPAIRED = "REPAIRED"
RECOVERED = "RECOVERED"
VERIFIED = "VERIFIED"
RESET = "RESET"

ORDER = [
    CREATED,
    BASELINED,
    INJECTED,
    EXECUTED,
    DETECTED,
    EVIDENCE_COLLECTED,
    CONTAINED,
    REPAIRED,
    RECOVERED,
    VERIFIED,
    RESET,
]

# Allowed predecessor states for each command. Idempotent steps (evidence,
# recover, verify) accept their own resulting state so a second call is legal.
ALLOWED_PREDECESSORS = {
    "baseline": {CREATED},
    "inject": {BASELINED},
    "run": {INJECTED},
    "detect": {EXECUTED},
    "evidence": {DETECTED, EVIDENCE_COLLECTED},
    "contain": {EVIDENCE_COLLECTED},
    "repair": {CONTAINED},
    "recover": {REPAIRED, RECOVERED, VERIFIED},
    "verify": {RECOVERED, VERIFIED},
}

RESULTING_STATE = {
    "baseline": BASELINED,
    "inject": INJECTED,
    "run": EXECUTED,
    "detect": DETECTED,
    "evidence": EVIDENCE_COLLECTED,
    "contain": CONTAINED,
    "repair": REPAIRED,
    "recover": RECOVERED,
    "verify": VERIFIED,
}


class LifecycleError(Exception):
    """Raised when a command runs from a disallowed lifecycle state."""


def get_state(conn: sqlite3.Connection, run_id: str) -> str | None:
    row = conn.execute(
        "SELECT lifecycle_state FROM pipeline_run WHERE run_id = ?", (run_id,)
    ).fetchone()
    return None if row is None else row[0]


def require_state(conn: sqlite3.Connection, run_id: str, command: str) -> str:
    current = get_state(conn, run_id)
    allowed = ALLOWED_PREDECESSORS.get(command, set())
    if current not in allowed:
        raise LifecycleError(
            f"command {command!r} requires state in {sorted(allowed)}, "
            f"but run {run_id} is {current!r}"
        )
    return current


def set_state(conn: sqlite3.Connection, run_id: str, new_state: str, logical_time: str) -> None:
    conn.execute(
        "UPDATE pipeline_run SET lifecycle_state = ?, updated_logical_time = ? WHERE run_id = ?",
        (new_state, logical_time, run_id),
    )
    conn.commit()


def advance(conn: sqlite3.Connection, run_id: str, command: str, logical_time: str) -> str:
    """Validate the predecessor, then move to the command's resulting state."""
    require_state(conn, run_id, command)
    new_state = RESULTING_STATE[command]
    set_state(conn, run_id, new_state, logical_time)
    return new_state
