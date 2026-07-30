-- MetroPulse warehouse schema (lab_architecture.md section 4.3).
-- SQLite types are deliberately limited to TEXT and INTEGER; booleans are
-- INTEGER CHECK (... IN (0,1)). No floating point is used as a key or oracle.
-- Coordinates are integer microdegrees; distances and durations are integers;
-- fictional fare money, if ever used, is integer cents.
--
-- This file is applied to the per-run warehouse database. The control-plane
-- tables live in a separate control database (see control_schema.sql).

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- Raw layer: one parsed source record per row. Domain duplicates are permitted
-- here so incidents can be observed. Every raw table carries lineage columns.
-- ---------------------------------------------------------------------------

CREATE TABLE raw_route_registry (
    delivery_id       TEXT    NOT NULL,
    record_number     INTEGER NOT NULL,
    route_id          TEXT    NOT NULL,
    registry_version  INTEGER NOT NULL,
    display_code      TEXT    NOT NULL,
    mode              TEXT    NOT NULL,
    active_flag       INTEGER NOT NULL CHECK (active_flag IN (0, 1)),
    content_sha256    TEXT    NOT NULL,
    ingested_at_utc   TEXT    NOT NULL,
    pipeline_run_id   TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_station_registry (
    delivery_id       TEXT    NOT NULL,
    record_number     INTEGER NOT NULL,
    station_id        TEXT    NOT NULL,
    registry_version  INTEGER NOT NULL,
    station_name      TEXT    NOT NULL,
    zone_code         TEXT    NOT NULL,
    active_flag       INTEGER NOT NULL CHECK (active_flag IN (0, 1)),
    content_sha256    TEXT    NOT NULL,
    ingested_at_utc   TEXT    NOT NULL,
    pipeline_run_id   TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_scheduled_trip (
    delivery_id          TEXT    NOT NULL,
    record_number        INTEGER NOT NULL,
    service_date         TEXT    NOT NULL,
    trip_id              TEXT    NOT NULL,
    route_id             TEXT    NOT NULL,
    direction_id         TEXT    NOT NULL,
    scheduled_start_utc  TEXT    NOT NULL,
    scheduled_end_utc    TEXT    NOT NULL,
    updated_at_utc       TEXT    NOT NULL,
    content_sha256       TEXT    NOT NULL,
    ingested_at_utc      TEXT    NOT NULL,
    pipeline_run_id      TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_stop_plan (
    delivery_id              TEXT    NOT NULL,
    record_number            INTEGER NOT NULL,
    trip_id                  TEXT    NOT NULL,
    stop_sequence            INTEGER NOT NULL,
    station_id               TEXT    NOT NULL,
    scheduled_arrival_utc    TEXT    NOT NULL,
    scheduled_departure_utc  TEXT    NOT NULL,
    content_sha256           TEXT    NOT NULL,
    ingested_at_utc          TEXT    NOT NULL,
    pipeline_run_id          TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_stop_event (
    delivery_id       TEXT    NOT NULL,
    record_number     INTEGER NOT NULL,
    stop_event_id     TEXT    NOT NULL,
    trip_id           TEXT    NOT NULL,
    station_id        TEXT    NOT NULL,
    event_type        TEXT    NOT NULL,
    occurred_at_utc   TEXT    NOT NULL,
    received_at_utc   TEXT    NOT NULL,
    source_sequence   INTEGER NOT NULL,
    content_sha256    TEXT    NOT NULL,
    ingested_at_utc   TEXT    NOT NULL,
    pipeline_run_id   TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_gate_count_event (
    delivery_id       TEXT    NOT NULL,
    record_number     INTEGER NOT NULL,
    gate_event_id     TEXT    NOT NULL,
    device_id         TEXT,
    station_id        TEXT    NOT NULL,
    observed_at_utc   TEXT    NOT NULL,
    trip_stop_window  TEXT    NOT NULL,
    passenger_count   INTEGER NOT NULL,
    content_sha256    TEXT    NOT NULL,
    ingested_at_utc   TEXT    NOT NULL,
    pipeline_run_id   TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_vehicle_telemetry (
    delivery_id       TEXT    NOT NULL,
    record_number     INTEGER NOT NULL,
    telemetry_event_id TEXT   NOT NULL,
    vehicle_test_id   TEXT    NOT NULL,
    boot_id           TEXT    NOT NULL,
    passenger_counter INTEGER NOT NULL,
    distance_m        INTEGER NOT NULL,
    observed_at_raw   INTEGER NOT NULL,
    content_sha256    TEXT    NOT NULL,
    ingested_at_utc   TEXT    NOT NULL,
    pipeline_run_id   TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_vehicle_position (
    delivery_id       TEXT    NOT NULL,
    record_number     INTEGER NOT NULL,
    position_event_id TEXT    NOT NULL,
    snapshot_id       TEXT    NOT NULL,
    vehicle_test_id   TEXT    NOT NULL,
    trip_id           TEXT,
    route_id          TEXT,
    observed_at_raw   INTEGER NOT NULL,
    latitude_e6       INTEGER NOT NULL,
    longitude_e6      INTEGER NOT NULL,
    distance_m        INTEGER NOT NULL,
    content_sha256    TEXT    NOT NULL,
    ingested_at_utc   TEXT    NOT NULL,
    pipeline_run_id   TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_route_change (
    delivery_id       TEXT    NOT NULL,
    record_number     INTEGER NOT NULL,
    change_id         TEXT    NOT NULL,
    route_id          TEXT    NOT NULL,
    source_position   INTEGER NOT NULL,
    updated_at_utc    TEXT    NOT NULL,
    payload_json      TEXT    NOT NULL,
    content_sha256    TEXT    NOT NULL,
    ingested_at_utc   TEXT    NOT NULL,
    pipeline_run_id   TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

CREATE TABLE raw_service_alert (
    delivery_id       TEXT    NOT NULL,
    record_number     INTEGER NOT NULL,
    alert_event_id    TEXT    NOT NULL,
    alert_id          TEXT    NOT NULL,
    route_id          TEXT    NOT NULL,
    status            TEXT    NOT NULL,
    occurred_at_utc   TEXT    NOT NULL,
    received_at_utc   TEXT    NOT NULL,
    source_sequence   INTEGER NOT NULL,
    content_sha256    TEXT    NOT NULL,
    ingested_at_utc   TEXT    NOT NULL,
    pipeline_run_id   TEXT    NOT NULL,
    PRIMARY KEY (delivery_id, record_number)
);

-- ---------------------------------------------------------------------------
-- Staging layer: one row per source event identity after normalization.
-- ---------------------------------------------------------------------------

CREATE TABLE stg_scheduled_trip (
    trip_id              TEXT NOT NULL PRIMARY KEY,
    service_date         TEXT NOT NULL,
    route_id             TEXT NOT NULL,
    direction_id         TEXT NOT NULL,
    scheduled_start_utc  TEXT NOT NULL,
    scheduled_end_utc    TEXT NOT NULL,
    source_delivery_id   TEXT NOT NULL
);

CREATE TABLE stg_stop_event (
    stop_event_id     TEXT NOT NULL PRIMARY KEY,
    trip_id           TEXT NOT NULL,
    station_id        TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    occurred_at_utc   TEXT NOT NULL,
    received_at_utc   TEXT NOT NULL,
    source_sequence   INTEGER NOT NULL,
    source_delivery_id TEXT NOT NULL
);

CREATE TABLE stg_gate_count_event (
    gate_event_id     TEXT NOT NULL PRIMARY KEY,
    device_id         TEXT,
    station_id        TEXT NOT NULL,
    service_date      TEXT NOT NULL,
    observed_at_utc   TEXT NOT NULL,
    trip_stop_window  TEXT NOT NULL,
    passenger_count   INTEGER NOT NULL,
    source_delivery_id TEXT NOT NULL
);

CREATE TABLE stg_vehicle_telemetry (
    telemetry_event_id TEXT NOT NULL PRIMARY KEY,
    vehicle_test_id   TEXT NOT NULL,
    boot_id           TEXT NOT NULL,
    passenger_counter INTEGER NOT NULL,
    distance_m        INTEGER NOT NULL,
    observed_at_utc   TEXT NOT NULL,
    source_delivery_id TEXT NOT NULL
);

CREATE TABLE stg_vehicle_position (
    position_event_id TEXT NOT NULL PRIMARY KEY,
    vehicle_test_id   TEXT NOT NULL,
    trip_id           TEXT,
    route_id          TEXT,
    observed_at_utc   TEXT NOT NULL,
    latitude_e6       INTEGER NOT NULL,
    longitude_e6      INTEGER NOT NULL,
    distance_m        INTEGER NOT NULL,
    source_delivery_id TEXT NOT NULL
);

CREATE TABLE stg_route_change (
    route_id          TEXT NOT NULL,
    source_position   INTEGER NOT NULL,
    change_id         TEXT NOT NULL,
    updated_at_utc    TEXT NOT NULL,
    payload_json      TEXT NOT NULL,
    source_delivery_id TEXT NOT NULL,
    PRIMARY KEY (route_id, source_position)
);

-- ---------------------------------------------------------------------------
-- Warehouse layer: facts and dimensions.
-- ---------------------------------------------------------------------------

CREATE TABLE dim_route_version (
    route_id            TEXT NOT NULL,
    valid_from_position INTEGER NOT NULL,
    valid_to_position   INTEGER,          -- exclusive; NULL means open
    display_code        TEXT NOT NULL,
    mode                TEXT NOT NULL,
    active_flag         INTEGER NOT NULL CHECK (active_flag IN (0, 1)),
    is_current          INTEGER NOT NULL CHECK (is_current IN (0, 1)),
    PRIMARY KEY (route_id, valid_from_position)
);

CREATE TABLE fct_trip_stop_event (
    trip_id            TEXT NOT NULL,
    station_id         TEXT NOT NULL,
    event_type         TEXT NOT NULL,
    stop_event_id      TEXT NOT NULL,
    scheduled_time_utc TEXT,
    actual_time_utc    TEXT,
    delay_seconds      INTEGER,
    route_id           TEXT,
    source_publication TEXT NOT NULL,
    PRIMARY KEY (trip_id, station_id, event_type, stop_event_id)
);

CREATE TABLE fct_station_gate_count (
    gate_event_id     TEXT NOT NULL PRIMARY KEY,
    station_id        TEXT NOT NULL,
    device_id         TEXT,
    service_date      TEXT NOT NULL,
    observed_at_utc   TEXT NOT NULL,
    service_hour_utc  INTEGER NOT NULL,
    trip_stop_window  TEXT NOT NULL,
    passenger_count   INTEGER NOT NULL,
    route_id          TEXT,
    source_publication TEXT NOT NULL
);

CREATE TABLE fct_vehicle_counter_delta (
    telemetry_event_id TEXT NOT NULL PRIMARY KEY,
    vehicle_test_id    TEXT NOT NULL,
    boot_id            TEXT NOT NULL,
    route_id           TEXT,
    service_date       TEXT NOT NULL,
    service_hour_utc   INTEGER NOT NULL,
    observed_at_utc    TEXT NOT NULL,
    previous_counter   INTEGER,
    current_counter    INTEGER NOT NULL,
    interpreted_delta  INTEGER NOT NULL,
    delta_method       TEXT NOT NULL,     -- "session_delta" | "counter_as_delta" (faulty)
    quarantined        INTEGER NOT NULL CHECK (quarantined IN (0, 1)),
    source_publication TEXT NOT NULL
);

CREATE TABLE fct_vehicle_position (
    position_event_id TEXT NOT NULL PRIMARY KEY,
    vehicle_test_id   TEXT NOT NULL,
    trip_id           TEXT,
    route_id          TEXT,
    observed_at_utc   TEXT NOT NULL,
    latitude_e6       INTEGER NOT NULL,
    longitude_e6      INTEGER NOT NULL,
    distance_m        INTEGER NOT NULL,
    source_publication TEXT NOT NULL
);

CREATE TABLE fct_service_alert_version (
    alert_id          TEXT NOT NULL,
    source_sequence   INTEGER NOT NULL,
    alert_event_id    TEXT NOT NULL,
    route_id          TEXT NOT NULL,
    status            TEXT NOT NULL,
    occurred_at_utc   TEXT NOT NULL,
    received_at_utc   TEXT NOT NULL,
    source_publication TEXT NOT NULL,
    PRIMARY KEY (alert_id, source_sequence)
);

-- ---------------------------------------------------------------------------
-- Published marts and serving views.
-- ---------------------------------------------------------------------------

CREATE TABLE mart_route_day (
    service_date       TEXT NOT NULL,
    route_id           TEXT NOT NULL,
    direction_id       TEXT NOT NULL,
    scheduled_trips    INTEGER NOT NULL,
    completed_trips    INTEGER NOT NULL,
    canceled_trips     INTEGER NOT NULL,
    unknown_trips      INTEGER NOT NULL,
    on_time_trips      INTEGER NOT NULL,
    total_distance_m   INTEGER NOT NULL,
    publication_id     TEXT NOT NULL,
    PRIMARY KEY (service_date, route_id, direction_id)
);

CREATE TABLE mart_route_hour_ridership (
    service_date       TEXT NOT NULL,
    route_id           TEXT NOT NULL,
    service_hour_utc   INTEGER NOT NULL,
    boardings          INTEGER NOT NULL,
    publication_id     TEXT NOT NULL,
    PRIMARY KEY (service_date, route_id, service_hour_utc)
);

CREATE TABLE serving_vehicle_status (
    vehicle_test_id   TEXT NOT NULL PRIMARY KEY,
    route_id          TEXT,
    trip_id           TEXT,
    latest_position_event_id TEXT,
    observed_at_utc   TEXT,
    snapshot_version  TEXT
);

CREATE TABLE serving_route_status (
    route_id          TEXT NOT NULL PRIMARY KEY,
    service_status    TEXT NOT NULL,
    source_publication_ids TEXT NOT NULL,
    serving_version   TEXT NOT NULL
);
