"""Deterministic MetroPulse baseline fixture generation.

All source data is synthetic and generated from fixed code plus a fixed seed
(``config.BASELINE_SEED``). Nothing here reads the wall clock, the host time
zone, or ``os.urandom``. The same inputs are produced on every OS.

Two independent measurement paths are modelled so incidents in family A can be
reconciled:

* ``vehicle_telemetry`` — an onboard automatic passenger counter (APC) that
  reports a **cumulative** count since the device last booted. It resets to zero
  on a restart (a new ``boot_id``).
* ``gate_count_event`` — independent station gate observations. In a healthy
  baseline the gate totals for a route-hour equal the true onboard boardings for
  that route-hour.

These are fictional lab volumes, not production benchmarks.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

from . import config
from .clock import format_utc, parse_utc

# Nominal geometry/among-derived constants (lab values, not real measurements).
NOMINAL_TRIP_DISTANCE_M = 4200
ON_TIME_THRESHOLD_SECONDS = 300

# Onboard counter increment per telemetry reading, by route. Small integers keep
# the arithmetic transparent. Three readings are emitted per service hour.
COUNTER_STEP_BY_ROUTE = {"R1": 4, "R2": 5, "R3": 5, "R4": 4}
READING_MINUTES = (0, 20, 40)

# The one baseline device restart: this vehicle splits 2026-04-14 into a morning
# boot session and an afternoon boot session. Restarts are normal; the MP-01
# fault is a transform, not the restart.
RESTART_VEHICLE = "VEH_TEST_02"
RESTART_DATE = "2026-04-14"
MORNING_HOURS = (6, 7, 8)
AFTERNOON_HOURS = (16, 17, 18)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _stable_int(*parts: object) -> int:
    """Deterministic non-negative int from parts (builtin hash() is randomized)."""
    joined = "|".join(str(p) for p in parts)
    return int(hashlib.sha256(joined.encode("utf-8")).hexdigest(), 16)


def _epoch_seconds(iso_utc: str) -> int:
    return int(parse_utc(iso_utc).timestamp())


# ---------------------------------------------------------------------------
# Row builders. Each returns a list of ordered dict rows plus the column order.
# ---------------------------------------------------------------------------

def _route_registry() -> list[dict[str, Any]]:
    rows = []
    for n, route in enumerate(config.ROUTES, start=1):
        rows.append(
            {
                "route_id": route,
                "registry_version": 1,
                "display_code": f"{config.ROUTE_MODE[route][:1].upper()}{n:02d}",
                "mode": config.ROUTE_MODE[route],
                "active_flag": 1,
            }
        )
    return rows


def _station_registry() -> list[dict[str, Any]]:
    rows = []
    for route, stations in config.ROUTE_STATIONS.items():
        for idx, station in enumerate(stations, start=1):
            rows.append(
                {
                    "station_id": station,
                    "registry_version": 1,
                    "station_name": f"{route} Stop {idx}",
                    "zone_code": f"Z{route[-1]}",
                    "active_flag": 1,
                }
            )
    return rows


def _scheduled_trips() -> list[dict[str, Any]]:
    rows = []
    for service_date in config.BASELINE_SERVICE_DATES:
        for route in config.ROUTES:
            for direction in ("0", "1"):
                trip_id = f"TRIP_{route}_{service_date.replace('-', '')}_{direction}"
                start_hour = 6 if direction == "0" else 16
                start = f"{service_date}T{start_hour:02d}:05:00Z"
                end = f"{service_date}T{start_hour + 1:02d}:05:00Z"
                rows.append(
                    {
                        "service_date": service_date,
                        "trip_id": trip_id,
                        "route_id": route,
                        "direction_id": direction,
                        "scheduled_start_utc": start,
                        "scheduled_end_utc": end,
                        "updated_at_utc": f"{service_date}T00:00:00Z",
                    }
                )
    return rows


def _stop_plan() -> list[dict[str, Any]]:
    rows = []
    for trip in _scheduled_trips():
        route = trip["route_id"]
        base = parse_utc(trip["scheduled_start_utc"])
        for seq, station in enumerate(config.ROUTE_STATIONS[route], start=1):
            arrival = format_utc(base + timedelta(minutes=10 * seq))
            departure = format_utc(base + timedelta(minutes=10 * seq + 1))
            rows.append(
                {
                    "trip_id": trip["trip_id"],
                    "stop_sequence": seq,
                    "station_id": station,
                    "scheduled_arrival_utc": arrival,
                    "scheduled_departure_utc": departure,
                }
            )
    return rows


def _stop_events() -> list[dict[str, Any]]:
    """Actual arrivals, all on-time in the baseline (delay 0-120s)."""
    rows = []
    seq_counter = 0
    for plan in _stop_plan():
        seq_counter += 1
        scheduled = parse_utc(plan["scheduled_arrival_utc"])
        # Deterministic small delay based on station index and trip.
        delay = (_stable_int(plan["trip_id"], plan["station_id"]) % 3) * 40
        actual = format_utc(scheduled + timedelta(seconds=delay))
        rows.append(
            {
                "stop_event_id": f"EVT_{seq_counter:06d}",
                "trip_id": plan["trip_id"],
                "station_id": plan["station_id"],
                "event_type": "ARRIVAL",
                "occurred_at_utc": actual,
                "received_at_utc": actual,
                "source_sequence": seq_counter,
            }
        )
    return rows


def _telemetry_readings() -> list[dict[str, Any]]:
    """Cumulative onboard counter readings.

    Returns raw telemetry rows. ``passenger_counter`` is cumulative since the
    session's boot; it resets to a fresh session for the one restart case.
    """
    rows: list[dict[str, Any]] = []
    seq = 0
    for service_date in config.BASELINE_SERVICE_DATES:
        for route in config.ROUTES:
            vehicle = config.ROUTE_VEHICLE[route]
            step = COUNTER_STEP_BY_ROUTE[route]
            is_restart = vehicle == RESTART_VEHICLE and service_date == RESTART_DATE
            if is_restart:
                sessions = [
                    (f"BOOT_{vehicle}_{service_date.replace('-', '')}_AM", MORNING_HOURS),
                    (f"BOOT_{vehicle}_{service_date.replace('-', '')}_PM", AFTERNOON_HOURS),
                ]
            else:
                sessions = [
                    (f"BOOT_{vehicle}_{service_date.replace('-', '')}", config.SERVICE_HOURS_UTC)
                ]
            for boot_id, hours in sessions:
                cumulative = 0
                distance = 0
                for hour in hours:
                    for minute in READING_MINUTES:
                        cumulative += step
                        distance += 300
                        seq += 1
                        observed = f"{service_date}T{hour:02d}:{minute:02d}:00Z"
                        rows.append(
                            {
                                "telemetry_event_id": f"TLM_{seq:06d}",
                                "vehicle_test_id": vehicle,
                                "boot_id": boot_id,
                                "passenger_counter": cumulative,
                                "distance_m": distance,
                                "observed_at_raw": _epoch_seconds(observed),
                            }
                        )
    return rows


def _true_route_hour_boardings() -> dict[tuple[str, str, int], int]:
    """The true boardings per (service_date, route, hour) implied by telemetry.

    Truth = non-negative session deltas summed per hour. For an hour, the true
    boardings equal (last cumulative in hour) - (last cumulative in previous
    reading within the same session), i.e. the counter growth across that hour.
    This is what the gate-count control is set to match.
    """
    readings = _telemetry_readings()
    # Group by (vehicle, boot_id) in chronological order (already ordered).
    by_session: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in readings:
        by_session.setdefault((r["vehicle_test_id"], r["boot_id"]), []).append(r)

    boardings: dict[tuple[str, str, int], int] = {}
    for (vehicle, _boot), session in by_session.items():
        route = config.VEHICLE_ROUTE[vehicle]
        prev = 0
        for r in session:
            iso = _iso_from_epoch(r["observed_at_raw"])
            dt = parse_utc(iso)
            key = (dt.strftime("%Y-%m-%d"), route, dt.hour)
            delta = r["passenger_counter"] - prev
            if delta < 0:
                delta = r["passenger_counter"]  # reset: boardings since boot
            boardings[key] = boardings.get(key, 0) + delta
            prev = r["passenger_counter"]
    return boardings


def _iso_from_epoch(epoch_seconds: int) -> str:
    from datetime import datetime, timezone

    return format_utc(datetime.fromtimestamp(epoch_seconds, tz=timezone.utc))


def _gate_count_events() -> list[dict[str, Any]]:
    """Independent gate counts that reconcile to true route-hour boardings.

    The route-hour true boardings are split as evenly as possible across the
    route's three stations, so the healthy-baseline reconciliation residual is
    zero.
    """
    true_boardings = _true_route_hour_boardings()
    rows: list[dict[str, Any]] = []
    seq = 0
    for (service_date, route, hour) in sorted(true_boardings):
        total = true_boardings[(service_date, route, hour)]
        stations = config.ROUTE_STATIONS[route]
        base = total // len(stations)
        remainder = total - base * len(stations)
        for idx, station in enumerate(stations):
            count = base + (1 if idx < remainder else 0)
            seq += 1
            observed = f"{service_date}T{hour:02d}:30:00Z"
            rows.append(
                {
                    "gate_event_id": f"GATE_{seq:06d}",
                    "device_id": f"GATE_DEV_{station}",
                    "station_id": station,
                    "observed_at_utc": observed,
                    "trip_stop_window": f"{route}:{service_date}:{hour:02d}",
                    "passenger_count": count,
                }
            )
    return rows


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row[c] for c in columns})


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n")


def _write_gzip_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(json.dumps(r, sort_keys=True, ensure_ascii=True) + "\n" for r in rows)
    # mtime=0 keeps the gzip bytes deterministic across runs and machines.
    with gzip.GzipFile(filename=str(path), mode="wb", mtime=0) as gz:
        gz.write(payload.encode("utf-8"))


def write_baseline_inputs(dest_dir: Path) -> dict[str, str]:
    """Write all baseline source files under ``dest_dir``. Returns file->sha256."""
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    _write_csv(
        dest / "route_registry.csv",
        _route_registry(),
        ["route_id", "registry_version", "display_code", "mode", "active_flag"],
    )
    _write_csv(
        dest / "station_registry.csv",
        _station_registry(),
        ["station_id", "registry_version", "station_name", "zone_code", "active_flag"],
    )
    _write_csv(
        dest / "scheduled_trip.csv",
        _scheduled_trips(),
        ["service_date", "trip_id", "route_id", "direction_id",
         "scheduled_start_utc", "scheduled_end_utc", "updated_at_utc"],
    )
    _write_csv(
        dest / "stop_plan.csv",
        _stop_plan(),
        ["trip_id", "stop_sequence", "station_id",
         "scheduled_arrival_utc", "scheduled_departure_utc"],
    )
    _write_csv(
        dest / "gate_count_event.csv",
        _gate_count_events(),
        ["gate_event_id", "device_id", "station_id",
         "observed_at_utc", "trip_stop_window", "passenger_count"],
    )
    _write_jsonl(dest / "stop_event.jsonl", _stop_events())
    _write_jsonl(dest / "vehicle_telemetry.jsonl", _telemetry_readings())
    # Sources reserved for later incident families; present but empty in v1 baseline.
    _write_jsonl(dest / "vehicle_position.jsonl", [])
    _write_jsonl(dest / "route_change.jsonl", [])
    _write_jsonl(dest / "service_alert.jsonl", [])

    checksums: dict[str, str] = {}
    for path in sorted(dest.iterdir()):
        if path.is_file():
            checksums[path.name] = _sha256_bytes(path.read_bytes())
    return checksums
