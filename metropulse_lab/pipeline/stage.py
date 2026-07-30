"""Staging stage: raw tables -> normalized staging tables (one row per source
event identity). Timestamps are normalized to ISO 8601 UTC strings.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .. import db
from ..clock import format_utc
from ..context import RunContext


def _iso_from_epoch_seconds(epoch: int) -> str:
    return format_utc(datetime.fromtimestamp(int(epoch), tz=timezone.utc))


def stage(ctx: RunContext) -> dict[str, Any]:
    conn = ctx.warehouse
    summary: dict[str, int] = {}

    with db.transaction(conn):
        for table in ("stg_scheduled_trip", "stg_stop_event", "stg_gate_count_event",
                      "stg_vehicle_telemetry", "stg_vehicle_position", "stg_route_change"):
            conn.execute(f"DELETE FROM {table}")

        # Scheduled trips: one row per trip_id.
        conn.execute(
            """
            INSERT INTO stg_scheduled_trip
              (trip_id, service_date, route_id, direction_id,
               scheduled_start_utc, scheduled_end_utc, source_delivery_id)
            SELECT trip_id, service_date, route_id, direction_id,
                   scheduled_start_utc, scheduled_end_utc, delivery_id
            FROM raw_scheduled_trip
            GROUP BY trip_id
            """
        )

        # Stop events: one row per stop_event_id.
        conn.execute(
            """
            INSERT INTO stg_stop_event
              (stop_event_id, trip_id, station_id, event_type,
               occurred_at_utc, received_at_utc, source_sequence, source_delivery_id)
            SELECT stop_event_id, trip_id, station_id, event_type,
                   occurred_at_utc, received_at_utc, source_sequence, delivery_id
            FROM raw_stop_event
            GROUP BY stop_event_id
            """
        )

        # Gate counts: add service_date derived from the observed timestamp.
        conn.execute(
            """
            INSERT INTO stg_gate_count_event
              (gate_event_id, device_id, station_id, service_date,
               observed_at_utc, trip_stop_window, passenger_count, source_delivery_id)
            SELECT gate_event_id, device_id, station_id,
                   substr(observed_at_utc, 1, 10),
                   observed_at_utc, trip_stop_window, passenger_count, delivery_id
            FROM raw_gate_count_event
            GROUP BY gate_event_id
            """
        )

        # Vehicle telemetry: normalize the raw epoch (seconds) to ISO UTC.
        rows = db.query(
            conn,
            "SELECT telemetry_event_id, vehicle_test_id, boot_id, passenger_counter, "
            "distance_m, observed_at_raw, delivery_id FROM raw_vehicle_telemetry "
            "GROUP BY telemetry_event_id ORDER BY telemetry_event_id",
        )
        staged = []
        for r in rows:
            staged.append(
                (
                    r["telemetry_event_id"], r["vehicle_test_id"], r["boot_id"],
                    r["passenger_counter"], r["distance_m"],
                    _iso_from_epoch_seconds(r["observed_at_raw"]), r["delivery_id"],
                )
            )
        db.insert_rows(
            conn, "stg_vehicle_telemetry",
            ("telemetry_event_id", "vehicle_test_id", "boot_id", "passenger_counter",
             "distance_m", "observed_at_utc", "source_delivery_id"),
            staged,
        )

    for table in ("stg_scheduled_trip", "stg_stop_event", "stg_gate_count_event",
                  "stg_vehicle_telemetry"):
        summary[table] = db.scalar(conn, f"SELECT COUNT(*) FROM {table}")

    ctx.log.emit(
        logical_time=ctx.clock.tick(),
        component="stage",
        event_type="staging_built",
        fields={"tables": summary},
    )
    return summary
