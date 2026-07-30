"""Data-plane invariants used as decisive evidence.

The MP-01 conservation invariant: the boardings attributed to a single
(vehicle, boot session) can never exceed the maximum cumulative counter observed
in that session, and can never be negative. Summing cumulative readings as if
they were per-event deltas violates this by a wide margin.
"""

from __future__ import annotations

import sqlite3
from typing import Any


def counter_session_conservation(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Per (vehicle, boot) attributed boardings vs the max cumulative counter."""
    rows = conn.execute(
        """
        SELECT vehicle_test_id, boot_id,
               SUM(interpreted_delta) AS attributed,
               MAX(current_counter)   AS max_counter,
               MIN(interpreted_delta) AS min_delta,
               COUNT(*)               AS readings
        FROM fct_vehicle_counter_delta
        GROUP BY vehicle_test_id, boot_id
        ORDER BY vehicle_test_id, boot_id
        """
    ).fetchall()
    out = []
    for r in rows:
        attributed = r["attributed"] or 0
        max_counter = r["max_counter"] or 0
        ok = 0 <= attributed <= max_counter
        out.append(
            {
                "vehicle_test_id": r["vehicle_test_id"],
                "boot_id": r["boot_id"],
                "attributed_boardings": attributed,
                "max_cumulative_counter": max_counter,
                "min_delta": r["min_delta"],
                "readings": r["readings"],
                "ok": ok,
            }
        )
    return out


def conservation_violations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in rows if not r["ok"]]
