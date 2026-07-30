"""Model stage: staging -> warehouse dimensions and facts.

This stage contains the MP-01 transform branch. The onboard telemetry counter is
cumulative since boot; the correct interpretation is a non-negative per-session
delta. The faulty branch ``counter_as_delta`` misreads each cumulative reading
as this interval's boardings.
"""

from __future__ import annotations

from typing import Any

from .. import config, db
from ..clock import parse_utc
from ..context import RunContext
from ..baseline import ON_TIME_THRESHOLD_SECONDS, NOMINAL_TRIP_DISTANCE_M

COUNTER_AS_DELTA = "counter_as_delta"


def _station_route_map(conn) -> dict[str, str]:
    rows = db.query(
        conn,
        """
        SELECT DISTINCT sp.station_id AS station_id, t.route_id AS route_id
        FROM raw_stop_plan sp
        JOIN stg_scheduled_trip t ON sp.trip_id = t.trip_id
        ORDER BY sp.station_id
        """,
    )
    return {r["station_id"]: r["route_id"] for r in rows}


def _build_dim_route_version(conn) -> None:
    conn.execute("DELETE FROM dim_route_version")
    conn.execute(
        """
        INSERT INTO dim_route_version
          (route_id, valid_from_position, valid_to_position,
           display_code, mode, active_flag, is_current)
        SELECT route_id, registry_version, NULL, display_code, mode, active_flag, 1
        FROM raw_route_registry
        GROUP BY route_id, registry_version
        """
    )


def _build_fct_gate_count(conn, publication_id: str) -> None:
    conn.execute("DELETE FROM fct_station_gate_count")
    station_route = _station_route_map(conn)
    rows = db.query(
        conn,
        "SELECT gate_event_id, device_id, station_id, service_date, observed_at_utc, "
        "trip_stop_window, passenger_count FROM stg_gate_count_event "
        "ORDER BY gate_event_id",
    )
    out = []
    for r in rows:
        hour = parse_utc(r["observed_at_utc"]).hour
        out.append(
            (r["gate_event_id"], r["station_id"], r["device_id"], r["service_date"],
             r["observed_at_utc"], hour, r["trip_stop_window"], r["passenger_count"],
             station_route.get(r["station_id"]), publication_id)
        )
    db.insert_rows(
        conn, "fct_station_gate_count",
        ("gate_event_id", "station_id", "device_id", "service_date", "observed_at_utc",
         "service_hour_utc", "trip_stop_window", "passenger_count", "route_id",
         "source_publication"),
        out,
    )


def _build_fct_trip_stop_event(conn, publication_id: str) -> None:
    conn.execute("DELETE FROM fct_trip_stop_event")
    plan = {
        (r["trip_id"], r["station_id"]): r["scheduled_arrival_utc"]
        for r in db.query(conn, "SELECT trip_id, station_id, scheduled_arrival_utc "
                                "FROM raw_stop_plan")
    }
    trip_route = {
        r["trip_id"]: r["route_id"]
        for r in db.query(conn, "SELECT trip_id, route_id FROM stg_scheduled_trip")
    }
    rows = db.query(
        conn,
        "SELECT stop_event_id, trip_id, station_id, event_type, occurred_at_utc "
        "FROM stg_stop_event ORDER BY stop_event_id",
    )
    out = []
    for r in rows:
        scheduled = plan.get((r["trip_id"], r["station_id"]))
        delay = None
        if scheduled is not None:
            delay = int((parse_utc(r["occurred_at_utc"]) - parse_utc(scheduled)).total_seconds())
        out.append(
            (r["trip_id"], r["station_id"], r["event_type"], r["stop_event_id"],
             scheduled, r["occurred_at_utc"], delay, trip_route.get(r["trip_id"]),
             publication_id)
        )
    db.insert_rows(
        conn, "fct_trip_stop_event",
        ("trip_id", "station_id", "event_type", "stop_event_id", "scheduled_time_utc",
         "actual_time_utc", "delay_seconds", "route_id", "source_publication"),
        out,
    )


def _build_fct_counter_delta(conn, publication_id: str, faulty: bool) -> dict[str, Any]:
    """Build fct_vehicle_counter_delta using the correct or faulty branch."""
    conn.execute("DELETE FROM fct_vehicle_counter_delta")
    rows = db.query(
        conn,
        "SELECT telemetry_event_id, vehicle_test_id, boot_id, passenger_counter, "
        "observed_at_utc FROM stg_vehicle_telemetry "
        "ORDER BY vehicle_test_id, boot_id, observed_at_utc, telemetry_event_id",
    )
    method = COUNTER_AS_DELTA if faulty else "session_delta"
    out = []
    prev_counter: dict[tuple[str, str], int] = {}
    quarantined_count = 0
    for r in rows:
        vehicle = r["vehicle_test_id"]
        boot = r["boot_id"]
        counter = r["passenger_counter"]
        route = config.VEHICLE_ROUTE.get(vehicle)
        dt = parse_utc(r["observed_at_utc"])
        service_date = dt.strftime("%Y-%m-%d")
        hour = dt.hour
        previous = prev_counter.get((vehicle, boot))
        quarantined = 0
        if faulty:
            # Faulty: each cumulative reading is misread as this interval's boardings.
            interpreted = counter
        else:
            # Correct: non-negative per-session delta; first-of-session is boardings
            # since boot (delta from zero).
            if previous is None:
                interpreted = counter
            else:
                interpreted = counter - previous
            if interpreted < 0:
                # An unexplained decrease inside a session is quarantined, not summed.
                interpreted = 0
                quarantined = 1
                quarantined_count += 1
        out.append(
            (r["telemetry_event_id"], vehicle, boot, route, service_date, hour,
             r["observed_at_utc"], previous, counter, interpreted, method,
             quarantined, publication_id)
        )
        prev_counter[(vehicle, boot)] = counter

    db.insert_rows(
        conn, "fct_vehicle_counter_delta",
        ("telemetry_event_id", "vehicle_test_id", "boot_id", "route_id", "service_date",
         "service_hour_utc", "observed_at_utc", "previous_counter", "current_counter",
         "interpreted_delta", "delta_method", "quarantined", "source_publication"),
        out,
    )
    return {"rows": len(out), "method": method, "quarantined": quarantined_count}


def model(ctx: RunContext, publication_id: str) -> dict[str, Any]:
    conn = ctx.warehouse
    faulty = ctx.fault_active(COUNTER_AS_DELTA)
    with db.transaction(conn):
        _build_dim_route_version(conn)
        _build_fct_gate_count(conn, publication_id)
        _build_fct_trip_stop_event(conn, publication_id)
        delta_info = _build_fct_counter_delta(conn, publication_id, faulty)

    summary = {
        "dim_route_version": db.scalar(conn, "SELECT COUNT(*) FROM dim_route_version"),
        "fct_station_gate_count": db.scalar(conn, "SELECT COUNT(*) FROM fct_station_gate_count"),
        "fct_trip_stop_event": db.scalar(conn, "SELECT COUNT(*) FROM fct_trip_stop_event"),
        "fct_vehicle_counter_delta": delta_info,
    }
    ctx.log.emit(
        logical_time=ctx.clock.tick(),
        component="model",
        event_type="warehouse_built",
        fields={"counter_delta_rows": delta_info["rows"]},
    )
    return summary


# Expose constants for mart derivation / verification.
__all__ = ["model", "COUNTER_AS_DELTA", "ON_TIME_THRESHOLD_SECONDS", "NOMINAL_TRIP_DISTANCE_M"]
