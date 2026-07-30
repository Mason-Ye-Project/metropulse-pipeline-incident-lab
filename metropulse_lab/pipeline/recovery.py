"""Bounded recovery helpers.

Recovery rebuilds only the affected datasets so that unrelated tables are proven
unchanged. For MP-01 this is a REPLAY: recompute the vehicle counter-delta fact
with the corrected session-aware rule and republish the route-hour ridership
mart. All other warehouse tables are left exactly as the faulty run wrote them.
"""

from __future__ import annotations

from typing import Any

from .. import config, db
from ..context import RunContext
from .build import publication_id_for
from .model import _build_fct_counter_delta
from .publish import _build_mart_ridership


def replay_counter_ridership(ctx: RunContext) -> dict[str, Any]:
    conn = ctx.warehouse
    pub = publication_id_for(ctx, "recover")
    with db.transaction(conn):
        counter_info = _build_fct_counter_delta(conn, pub, faulty=False)
        ridership_rows = _build_mart_ridership(conn, pub)

    now = ctx.clock.now()
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO dataset_version "
            "(dataset_version_id, dataset_name, target_scope, state, referenced_object, "
            " catalog_version, created_logical_time) "
            "VALUES (?, 'mart_route_hour_ridership', 'all-baseline', 'PUBLISHED', ?, ?, ?)",
            (f"DV-ridership-{pub}", pub, config.CATALOG_VERSION, now),
        )

    manifest = {
        "mode": "REPLAY",
        "publication_id": pub,
        "scope": {"service_dates": "all-baseline", "routes": "all", "datasets": [
            "fct_vehicle_counter_delta", "mart_route_hour_ridership"]},
        "write_mode": "bounded_replace",
        "duplicate_protection": "primary key at declared grain; deterministic recompute",
        "counter_delta_rows": counter_info["rows"],
        "ridership_rows": ridership_rows,
        "corrected_method": "session_delta",
    }
    ctx.log.emit(
        logical_time=ctx.clock.tick(),
        component="recovery",
        event_type="replay_completed",
        fields={"publication_id": pub, "ridership_rows": ridership_rows},
    )
    return manifest
