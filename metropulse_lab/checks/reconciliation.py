"""Reconciliation between the two independent ridership measurement paths.

Published route-hour boardings (onboard counter deltas) are compared against the
independent station gate-count control for the same route-hour. In a healthy
baseline the residual is zero.
"""

from __future__ import annotations

import sqlite3
from typing import Any


def ridership_vs_gate(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return per (service_date, route, hour) published boardings vs gate control."""
    gate = {
        (r["service_date"], r["route_id"], r["service_hour_utc"]): r["gate_total"]
        for r in conn.execute(
            """
            SELECT service_date, route_id, service_hour_utc,
                   SUM(passenger_count) AS gate_total
            FROM fct_station_gate_count
            WHERE route_id IS NOT NULL
            GROUP BY service_date, route_id, service_hour_utc
            """
        ).fetchall()
    }
    published = conn.execute(
        "SELECT service_date, route_id, service_hour_utc, boardings "
        "FROM mart_route_hour_ridership "
        "ORDER BY service_date, route_id, service_hour_utc"
    ).fetchall()
    out = []
    for r in published:
        key = (r["service_date"], r["route_id"], r["service_hour_utc"])
        gate_total = gate.get(key, 0)
        residual = r["boardings"] - gate_total
        ratio = (r["boardings"] / gate_total) if gate_total else None
        out.append(
            {
                "service_date": r["service_date"],
                "route_id": r["route_id"],
                "service_hour_utc": r["service_hour_utc"],
                "boardings": r["boardings"],
                "gate_control": gate_total,
                "residual": residual,
                "ratio": ratio,
            }
        )
    return out


def violations(recon_rows: list[dict[str, Any]], tolerance: float) -> list[dict[str, Any]]:
    bad = []
    for row in recon_rows:
        base = max(row["gate_control"], 1)
        if abs(row["residual"]) / base > tolerance:
            bad.append(row)
    return bad
