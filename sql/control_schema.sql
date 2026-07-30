-- MetroPulse control-plane schema (lab_architecture.md section 4.3).
-- Applied to the per-run control database (control.sqlite3). These tables carry
-- run lifecycle, task attempts, manifests, cursors, quality results, dataset
-- versions/lineage, and the deterministic simulator ledgers.

PRAGMA foreign_keys = ON;

CREATE TABLE pipeline_run (
    run_id                TEXT NOT NULL PRIMARY KEY,
    incident_id           TEXT NOT NULL,
    catalog_version       TEXT NOT NULL,
    baseline_id           TEXT NOT NULL,
    logical_date          TEXT NOT NULL,
    lifecycle_state       TEXT NOT NULL,
    fault_profile_json    TEXT,
    created_logical_time  TEXT NOT NULL,
    updated_logical_time  TEXT NOT NULL
);

CREATE TABLE task_run (
    run_id       TEXT NOT NULL,
    task_name    TEXT NOT NULL,
    attempt      INTEGER NOT NULL,
    state        TEXT NOT NULL,
    logical_time TEXT NOT NULL,
    detail_json  TEXT,
    PRIMARY KEY (run_id, task_name, attempt)
);

CREATE TABLE ingestion_manifest (
    delivery_id        TEXT NOT NULL PRIMARY KEY,
    source_name        TEXT NOT NULL,
    object_path        TEXT NOT NULL,
    checksum           TEXT NOT NULL,
    record_count       INTEGER,
    byte_count         INTEGER,
    completeness_token TEXT,
    commit_state       TEXT NOT NULL
);

CREATE TABLE api_request_log (
    request_id     TEXT NOT NULL PRIMARY KEY,
    source_name    TEXT NOT NULL,
    snapshot_id    TEXT,
    page_token     TEXT,
    next_token     TEXT,
    response_code  INTEGER NOT NULL,
    attempt        INTEGER NOT NULL,
    worker         TEXT,
    logical_time   TEXT NOT NULL
);

CREATE TABLE source_cursor (
    source_name        TEXT NOT NULL PRIMARY KEY,
    cursor_value       TEXT NOT NULL,
    committed_position TEXT,
    updated_logical_time TEXT NOT NULL
);

CREATE TABLE consumer_checkpoint (
    consumer_name       TEXT NOT NULL,
    source_name         TEXT NOT NULL,
    last_source_position TEXT,
    last_publication     TEXT,
    PRIMARY KEY (consumer_name, source_name)
);

CREATE TABLE quality_result (
    run_id        TEXT NOT NULL,
    check_id      TEXT NOT NULL,
    scope_key     TEXT NOT NULL,
    dimension     TEXT NOT NULL,
    expected      TEXT,
    actual        TEXT,
    status        TEXT NOT NULL,
    evidence_ptr  TEXT,
    PRIMARY KEY (run_id, check_id, scope_key)
);

CREATE TABLE dataset_version (
    dataset_version_id TEXT NOT NULL PRIMARY KEY,
    dataset_name       TEXT NOT NULL,
    target_scope       TEXT NOT NULL,
    state              TEXT NOT NULL,
    referenced_object  TEXT,
    catalog_version    TEXT,
    created_logical_time TEXT NOT NULL
);

CREATE TABLE dataset_lineage (
    dataset_version_id  TEXT NOT NULL,
    upstream_version_id TEXT NOT NULL,
    PRIMARY KEY (dataset_version_id, upstream_version_id)
);

CREATE TABLE scheduler_run (
    scheduler_run_id  TEXT NOT NULL PRIMARY KEY,
    logical_run_date  TEXT NOT NULL,
    creation_reason   TEXT NOT NULL,
    state             TEXT NOT NULL
);

CREATE TABLE external_job (
    external_job_id  TEXT NOT NULL PRIMARY KEY,
    target_name      TEXT NOT NULL,
    state            TEXT NOT NULL,
    heartbeat_seq    INTEGER,
    writer_attempt   INTEGER,
    fencing_token    INTEGER
);

CREATE TABLE writer_lease (
    target_name    TEXT NOT NULL PRIMARY KEY,
    current_writer TEXT,
    fence          INTEGER NOT NULL
);

CREATE TABLE fake_pool_session (
    session_id        TEXT NOT NULL PRIMARY KEY,
    acquired_seq      INTEGER,
    released_seq      INTEGER,
    transaction_state TEXT
);

CREATE TABLE simulated_state_entry (
    operator_id  TEXT NOT NULL,
    state_key    TEXT NOT NULL,
    event_time   TEXT,
    expiry_time  TEXT,
    byte_estimate INTEGER,
    PRIMARY KEY (operator_id, state_key)
);

CREATE TABLE snapshot_commit (
    snapshot_id      TEXT NOT NULL PRIMARY KEY,
    parent_snapshot  TEXT,
    owner            TEXT,
    change_set_json  TEXT
);

CREATE TABLE object_inventory (
    object_id       TEXT NOT NULL PRIMARY KEY,
    run_relative_path TEXT NOT NULL,
    checksum        TEXT,
    lifecycle_state TEXT NOT NULL,
    active_writer   TEXT
);

CREATE TABLE dependency_manifest (
    environment_id TEXT NOT NULL,
    package_name   TEXT NOT NULL,
    resolved_version TEXT NOT NULL,
    resolved_hash  TEXT,
    PRIMARY KEY (environment_id, package_name)
);

CREATE TABLE simulated_policy_decision (
    decision_id TEXT NOT NULL PRIMARY KEY,
    identity    TEXT NOT NULL,
    resource    TEXT NOT NULL,
    action      TEXT NOT NULL,
    allow       INTEGER NOT NULL CHECK (allow IN (0, 1)),
    reason      TEXT
);

CREATE TABLE simulated_outbox (
    message_id   TEXT NOT NULL PRIMARY KEY,
    topic        TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    logical_time TEXT NOT NULL
);
