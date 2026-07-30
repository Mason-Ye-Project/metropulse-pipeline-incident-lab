"""Publish stage: warehouse -> marts, serving views, dataset versions/lineage.

Publishing records dataset versions and lineage in the control plane so that a
green run is never, on its own, proof that published data is correct.
"""

from __future__ import annotations

from typing import Any

from .. import config, db
from ..context import RunContext
from ..baseline import NOMINAL_TRIP_DISTANCE_M, ON_TIME_THRESHOLD_SECONDS

RIDERSHIP_DATASET = "mart_route_hour_ridership"
ROUTE_DAY_DATASET = "mart_route_day"
COUNTER_DELTA_DATASET = "fct_vehicle_counter_delta"


def _build_mart_route_day(conn, publication_id: str) -> int:
    conn.execute("DELETE FROM mart_route_day")
    trips = db.query(
        conn,
        "SELECT trip_id, service_date, route_id, direction_id FROM stg_scheduled_trip",
    )
    # trip -> max delay and completed flag
    delay_by_trip: dict[str, int] = {}
    completed: set[str] = set()
    for r in db.query(conn, "SELECT trip_id, delay_seconds FROM fct_trip_stop_event"):
        completed.add(r["trip_id"])
        d = r["delay_seconds"] if r["delay_seconds"] is not None else 0
        delay_by_trip[r["trip_id"]] = max(delay_by_trip.get(r["trip_id"], 0), d)

    groups: dict[tuple[str, str, str], dict[str, int]] = {}
    for t in trips:
        key = (t["service_date"], t["route_id"], t["direction_id"])
        g = groups.setdefault(
            key, {"scheduled": 0, "completed": 0, "on_time": 0}
        )
        g["scheduled"] += 1
        if t["trip_id"] in completed:
            g["completed"] += 1
            if delay_by_trip.get(t["trip_id"], 0) <= ON_TIME_THRESHOLD_SECONDS:
                g["on_time"] += 1

    out = []
    for (service_date, route, direction), g in sorted(groups.items()):
        canceled = g["scheduled"] - g["completed"]
        out.append(
            (service_date, route, direction, g["scheduled"], g["completed"],
             canceled, 0, g["on_time"], g["completed"] * NOMINAL_TRIP_DISTANCE_M,
             publication_id)
        )
    db.insert_rows(
        conn, "mart_route_day",
        ("service_date", "route_id", "direction_id", "scheduled_trips",
         "completed_trips", "canceled_trips", "unknown_trips", "on_time_trips",
         "total_distance_m", "publication_id"),
        out,
    )
    return len(out)


def _build_mart_ridership(conn, publication_id: str) -> int:
    conn.execute("DELETE FROM mart_route_hour_ridership")
    conn.execute(
        """
        INSERT INTO mart_route_hour_ridership
          (service_date, route_id, service_hour_utc, boardings, publication_id)
        SELECT service_date, route_id, service_hour_utc,
               SUM(interpreted_delta), ?
        FROM fct_vehicle_counter_delta
        WHERE quarantined = 0 AND route_id IS NOT NULL
        GROUP BY service_date, route_id, service_hour_utc
        ORDER BY service_date, route_id, service_hour_utc
        """,
        (publication_id,),
    )
    return db.scalar(conn, "SELECT COUNT(*) FROM mart_route_hour_ridership")


def _build_serving(conn) -> None:
    conn.execute("DELETE FROM serving_route_status")
    conn.execute(
        """
        INSERT INTO serving_route_status
          (route_id, service_status, source_publication_ids, serving_version)
        SELECT route_id, 'IN_SERVICE', 'baseline', 'v1'
        FROM dim_route_version WHERE is_current = 1
        """
    )


def _record_dataset_versions(ctx: RunContext, publication_id: str) -> None:
    control = ctx.control
    now = ctx.clock.now()
    counter_dv = f"DV-counterdelta-{publication_id}"
    ridership_dv = f"DV-ridership-{publication_id}"
    route_day_dv = f"DV-routeday-{publication_id}"
    with db.transaction(control):
        for dv, name, scope in (
            (counter_dv, COUNTER_DELTA_DATASET, "all-baseline"),
            (ridership_dv, RIDERSHIP_DATASET, "all-baseline"),
            (route_day_dv, ROUTE_DAY_DATASET, "all-baseline"),
        ):
            control.execute(
                "INSERT OR REPLACE INTO dataset_version "
                "(dataset_version_id, dataset_name, target_scope, state, "
                " referenced_object, catalog_version, created_logical_time) "
                "VALUES (?, ?, ?, 'PUBLISHED', NULL, ?, ?)",
                (dv, name, scope, config.CATALOG_VERSION, now),
            )
        # ridership and route_day both depend on the counter-delta publication.
        control.execute(
            "INSERT OR REPLACE INTO dataset_lineage VALUES (?, ?)",
            (ridership_dv, counter_dv),
        )


def publish(ctx: RunContext, publication_id: str) -> dict[str, Any]:
    conn = ctx.warehouse
    with db.transaction(conn):
        route_day_rows = _build_mart_route_day(conn, publication_id)
        ridership_rows = _build_mart_ridership(conn, publication_id)
        _build_serving(conn)
    _record_dataset_versions(ctx, publication_id)

    summary = {
        "mart_route_day": route_day_rows,
        "mart_route_hour_ridership": ridership_rows,
        "publication_id": publication_id,
    }
    ctx.log.emit(
        logical_time=ctx.clock.tick(),
        component="publish",
        event_type="publication_committed",
        fields=summary,
    )
    return summary
