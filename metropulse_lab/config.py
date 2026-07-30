"""Frozen constants for the MetroPulse incident lab.

This module holds only stable configuration values: catalog identifiers, the
baseline window, exit codes, and the canonical table lists. It performs no I/O
and imports nothing outside the standard library so it is safe to import from
every other module.

MetroPulse is a wholly fictional, synthetic city-transit data platform. None of
the identifiers, dates, coordinates, or counts here describe a real transit
agency, a real incident, or the author's work history.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Catalog identity
# ---------------------------------------------------------------------------

CATALOG_VERSION = "1.0"
BASELINE_ID = "metro-v1"
RUNTIME_PROFILE = "stdlib-sqlite"
SCHEMA_VERSION = 1

# The fictional reader-facing system name. Any appearance of a retail brand,
# "BrokenMart", or "HarborFlow" is a naming regression and must block a freeze.
SYSTEM_NAME = "MetroPulse"

# ---------------------------------------------------------------------------
# Deterministic baseline window
# ---------------------------------------------------------------------------
# Three synthetic service days. These are lab fixtures, not a real calendar of
# events. Timestamps in fixtures are ISO 8601 UTC strings; any local offset used
# by an incident comes from checked-in configuration, never the host tz database.

BASELINE_SERVICE_DATES = ("2026-04-13", "2026-04-14", "2026-04-15")
BASELINE_CLOCK_START_UTC = "2026-04-13T00:00:00Z"

# Fixed generation seed. Deterministic pseudo-random choices in the fixture
# generator derive from this value only; never from wall clock or os.urandom.
BASELINE_SEED = 20260413

# ---------------------------------------------------------------------------
# Reference geography and fleet (invented test identifiers only)
# ---------------------------------------------------------------------------

ROUTES = ("R1", "R2", "R3", "R4")
ROUTE_MODE = {"R1": "bus", "R2": "bus", "R3": "rail", "R4": "rail"}

# Twelve stations, three per route, assigned deterministically.
STATIONS = tuple(f"STN_{n:03d}" for n in range(1, 13))
# STN_001..003 -> R1, STN_004..006 -> R2, STN_007..009 -> R3, STN_010..012 -> R4
ROUTE_STATIONS = {
    "R1": ("STN_001", "STN_002", "STN_003"),
    "R2": ("STN_004", "STN_005", "STN_006"),
    "R3": ("STN_007", "STN_008", "STN_009"),
    "R4": ("STN_010", "STN_011", "STN_012"),
}

# One test vehicle per route (1:1 for the first edition baseline).
ROUTE_VEHICLE = {"R1": "VEH_TEST_01", "R2": "VEH_TEST_02",
                 "R3": "VEH_TEST_03", "R4": "VEH_TEST_04"}
VEHICLE_ROUTE = {v: r for r, v in ROUTE_VEHICLE.items()}

# Service hours produced in the baseline (UTC hour-of-day). Small but believable.
SERVICE_HOURS_UTC = (6, 7, 8, 16, 17, 18)

# Reconciliation tolerance between onboard counter boardings and independent
# gate-count control totals, expressed as a fraction. A lab value, not an
# industry SLA.
RIDERSHIP_RECONCILE_TOLERANCE = 0.10

# ---------------------------------------------------------------------------
# Exit codes (lab_architecture.md section 9)
# ---------------------------------------------------------------------------

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_BAD_STATE = 3
EXIT_UNSAFE = 4
EXIT_DETECTED = 10
EXIT_VERIFY_FAILED = 11
EXIT_INTEGRITY_FAILED = 12
EXIT_INTERNAL = 20

# ---------------------------------------------------------------------------
# Canonical table names
# ---------------------------------------------------------------------------

RAW_TABLES = (
    "raw_route_registry",
    "raw_station_registry",
    "raw_scheduled_trip",
    "raw_stop_plan",
    "raw_stop_event",
    "raw_gate_count_event",
    "raw_vehicle_telemetry",
    "raw_vehicle_position",
    "raw_route_change",
    "raw_service_alert",
)

STAGING_TABLES = (
    "stg_scheduled_trip",
    "stg_stop_event",
    "stg_gate_count_event",
    "stg_vehicle_telemetry",
    "stg_vehicle_position",
    "stg_route_change",
)

WAREHOUSE_TABLES = (
    "dim_route_version",
    "fct_trip_stop_event",
    "fct_station_gate_count",
    "fct_vehicle_counter_delta",
    "fct_vehicle_position",
    "fct_service_alert_version",
)

MART_TABLES = (
    "mart_route_day",
    "mart_route_hour_ridership",
    "serving_vehicle_status",
    "serving_route_status",
)

CONTROL_TABLES = (
    "pipeline_run",
    "task_run",
    "ingestion_manifest",
    "api_request_log",
    "source_cursor",
    "consumer_checkpoint",
    "quality_result",
    "dataset_version",
    "dataset_lineage",
    "scheduler_run",
    "external_job",
    "writer_lease",
    "fake_pool_session",
    "simulated_state_entry",
    "snapshot_commit",
    "object_inventory",
    "dependency_manifest",
    "simulated_policy_decision",
    "simulated_outbox",
)

# Forbidden tokens used by privacy/overlap contract tests. Presence of any of
# these in shipped fixtures, source, or captured output is a hard failure.
FORBIDDEN_DOMAIN_TOKENS = (
    "TinyShop",
    "Northstar",
    "BrokenMart",
    "HarborFlow",
    "order_items",
    "Reliable Retail",
)
