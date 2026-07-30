"""Receive stage: landing files -> raw tables + ingestion manifest.

Raw tables intentionally permit domain duplicates so incidents can be observed.
Each raw record carries lineage columns (delivery_id, record_number,
content_sha256, ingested_at_utc, pipeline_run_id).
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .. import config, db
from ..context import RunContext


@dataclass(frozen=True)
class SourceSpec:
    filename: str
    fmt: str  # "csv" | "jsonl" | "gzip_jsonl"
    raw_table: str
    domain_columns: tuple[str, ...]
    int_columns: tuple[str, ...] = ()


SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec("route_registry.csv", "csv", "raw_route_registry",
               ("route_id", "registry_version", "display_code", "mode", "active_flag"),
               ("registry_version", "active_flag")),
    SourceSpec("station_registry.csv", "csv", "raw_station_registry",
               ("station_id", "registry_version", "station_name", "zone_code", "active_flag"),
               ("registry_version", "active_flag")),
    SourceSpec("scheduled_trip.csv", "csv", "raw_scheduled_trip",
               ("service_date", "trip_id", "route_id", "direction_id",
                "scheduled_start_utc", "scheduled_end_utc", "updated_at_utc")),
    SourceSpec("stop_plan.csv", "csv", "raw_stop_plan",
               ("trip_id", "stop_sequence", "station_id",
                "scheduled_arrival_utc", "scheduled_departure_utc"),
               ("stop_sequence",)),
    SourceSpec("stop_event.jsonl", "jsonl", "raw_stop_event",
               ("stop_event_id", "trip_id", "station_id", "event_type",
                "occurred_at_utc", "received_at_utc", "source_sequence"),
               ("source_sequence",)),
    SourceSpec("gate_count_event.csv", "csv", "raw_gate_count_event",
               ("gate_event_id", "device_id", "station_id", "observed_at_utc",
                "trip_stop_window", "passenger_count"),
               ("passenger_count",)),
    SourceSpec("vehicle_telemetry.jsonl", "jsonl", "raw_vehicle_telemetry",
               ("telemetry_event_id", "vehicle_test_id", "boot_id",
                "passenger_counter", "distance_m", "observed_at_raw"),
               ("passenger_counter", "distance_m", "observed_at_raw")),
    SourceSpec("vehicle_position.jsonl", "jsonl", "raw_vehicle_position",
               ("position_event_id", "snapshot_id", "vehicle_test_id", "trip_id",
                "route_id", "observed_at_raw", "latitude_e6", "longitude_e6", "distance_m"),
               ("observed_at_raw", "latitude_e6", "longitude_e6", "distance_m")),
    SourceSpec("route_change.jsonl", "jsonl", "raw_route_change",
               ("change_id", "route_id", "source_position", "updated_at_utc", "payload_json"),
               ("source_position",)),
    SourceSpec("service_alert.jsonl", "jsonl", "raw_service_alert",
               ("alert_event_id", "alert_id", "route_id", "status",
                "occurred_at_utc", "received_at_utc", "source_sequence"),
               ("source_sequence",)),
)

LINEAGE_COLUMNS = ("delivery_id", "record_number", "content_sha256",
                   "ingested_at_utc", "pipeline_run_id")


def _read_records(path: Path, spec: SourceSpec) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if spec.fmt == "csv":
        with path.open("r", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    if spec.fmt == "jsonl":
        rows = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows
    if spec.fmt == "gzip_jsonl":
        rows = []
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows
    raise ValueError(f"unknown format: {spec.fmt}")


def _coerce(value: Any, is_int: bool) -> Any:
    if value is None or value == "":
        return None
    if is_int:
        return int(value)
    return value


def receive(ctx: RunContext) -> dict[str, Any]:
    conn = ctx.warehouse
    control = ctx.control
    landing = ctx.paths.landing_dir
    ingested_at = ctx.clock.now()

    summary: dict[str, int] = {}
    with db.transaction(conn):
        for spec in SOURCE_SPECS:
            conn.execute(f"DELETE FROM {spec.raw_table}")
    with db.transaction(control):
        control.execute("DELETE FROM ingestion_manifest")

    for spec in SOURCE_SPECS:
        path = landing / spec.filename
        records = _read_records(path, spec)
        delivery_id = f"DLV-{Path(spec.filename).stem}"
        rows_to_insert = []
        for i, rec in enumerate(records, start=1):
            domain_values = [
                _coerce(rec.get(col), col in spec.int_columns) for col in spec.domain_columns
            ]
            canonical = json.dumps(
                {col: rec.get(col) for col in spec.domain_columns}, sort_keys=True
            )
            content_sha = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            rows_to_insert.append(
                list(domain_values)
                + [delivery_id, i, content_sha, ingested_at, ctx.run_id]
            )
        columns = list(spec.domain_columns) + list(LINEAGE_COLUMNS)
        with db.transaction(conn):
            db.insert_rows(conn, spec.raw_table, columns, rows_to_insert)

        file_bytes = path.stat().st_size if path.exists() else 0
        file_sha = (
            hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else ""
        )
        with db.transaction(control):
            control.execute(
                "INSERT INTO ingestion_manifest "
                "(delivery_id, source_name, object_path, checksum, record_count, "
                " byte_count, completeness_token, commit_state) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (delivery_id, spec.raw_table, spec.filename, file_sha,
                 len(records), file_bytes, "COMPLETE", "COMMITTED"),
            )
        summary[spec.raw_table] = len(records)

    ctx.log.emit(
        logical_time=ctx.clock.tick(),
        component="receive",
        event_type="raw_loaded",
        fields={"tables": summary},
    )
    return summary
