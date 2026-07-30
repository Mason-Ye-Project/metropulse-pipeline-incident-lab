"""Shared helpers for the performance-and-cost family (MP-16 .. MP-20).

These incidents never corrupt warehouse data. The published rows are identical
whether the faulty or the corrected path runs; what differs is the *logical work*
a step performs — rows and bytes a step would assemble, request/call counts,
projected orchestration work-items, retained store size, and a size-profile
recorded for a join. Every figure here is a deterministic logical counter. None
of it is laptop milliseconds, real memory, network latency, or a Spark / Flink /
Airflow / PostgreSQL artifact.

This module holds only mechanism-neutral plumbing: CSV/JSON writers, the evidence
index, a sanitized timeline, a quality-result writer, and small table helpers.
The reader-facing evidence pack these helpers assemble must describe symptoms
only; the root-cause vocabulary lives in the private reports and tests.
"""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path
from typing import Any

from .. import db
from ..context import RunContext
from ..reports import write_json

# Every simulator artifact in this family is labeled with this origin so a reader
# can never mistake a deterministic logical projection for a production plan,
# log, timing, or memory curve.
EVIDENCE_ORIGIN = "metropulse_deterministic_simulator"

FICTIONAL_NOTICE = (
    "MetroPulse is a fictional, synthetic city-transit system. These are lab "
    "values produced by a deterministic local simulator, not a real transit "
    "agency, a real incident, or a production engine's measurements."
)

# Structured-log components that may appear in the sanitized reader timeline.
# Incident-internal components (incident, recovery) name the mechanism and are
# excluded so the evidence pack cannot reveal the answer early.
_TIMELINE_COMPONENTS = {"receive", "stage", "model", "publish"}


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c) for c in columns})


def table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def write_quality_result(ctx: RunContext, check_id: str, dimension: str,
                         expected: str, actual: str, status: str,
                         evidence_ptr: str = "evidence/tables/quality_results.csv") -> None:
    """Record a mechanism-neutral quality result for the current run."""
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO quality_result "
            "(run_id, check_id, scope_key, dimension, expected, actual, status, evidence_ptr) "
            "VALUES (?, ?, 'all', ?, ?, ?, ?, ?)",
            (ctx.run_id, check_id, dimension, expected, actual, status, evidence_ptr))


def record_recovery_publication(ctx: RunContext, dataset_name: str, publication_id: str) -> str:
    """Register a bounded-recovery publication so control consistency can see it.

    Deterministic id + logical time (fresh context clock) keep a second recovery
    byte-for-byte identical, satisfying the idempotency contract.
    """
    dv_id = f"DV-{dataset_name}-{publication_id}"
    now = ctx.clock.now()
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO dataset_version "
            "(dataset_version_id, dataset_name, target_scope, state, referenced_object, "
            " catalog_version, created_logical_time) "
            "VALUES (?, ?, 'all-baseline', 'PUBLISHED', ?, '1.0', ?)",
            (dv_id, dataset_name, publication_id, now))
    return dv_id


def has_recovery_publication(ctx: RunContext, dataset_name: str) -> bool:
    return db.scalar(
        ctx.control,
        "SELECT COUNT(*) FROM dataset_version WHERE dataset_name = ? "
        "AND state = 'PUBLISHED' AND referenced_object LIKE '%recover%'",
        (dataset_name,)) >= 1


# ---------------------------------------------------------------------------
# Evidence pack scaffolding (symptom only)
# ---------------------------------------------------------------------------

def build_alert(incident_id: str, title: str, reported_symptom: str,
                time_pressure: str, affected_product: str,
                observed: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "incident_id": incident_id,
        "title": title,
        "reported_symptom": reported_symptom,
        "time_pressure": time_pressure,
        "affected_product": affected_product,
        "observed": observed or {},
        "evidence_origin": EVIDENCE_ORIGIN,
        "fictional_notice": FICTIONAL_NOTICE,
    }


def sanitized_timeline(ctx: RunContext) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for e in ctx.log.read_all():
        if e.get("component") not in _TIMELINE_COMPONENTS:
            continue
        fields = {k: v for k, v in (e.get("fields") or {}).items() if k != "fault"}
        out.append({"logical_time": e.get("logical_time"),
                    "component": e.get("component"),
                    "event_type": e.get("event_type"), "fields": fields})
    return out


def write_common_evidence_tables(ctx: RunContext) -> None:
    """Row counts, input manifest, and quality results shared by every incident."""
    tables = ctx.paths.evidence_tables_dir
    tables.mkdir(parents=True, exist_ok=True)
    write_csv(
        tables / "input_manifest.csv",
        db.rows_to_dicts(db.query(ctx.control,
            "SELECT delivery_id, source_name, record_count, byte_count, "
            "completeness_token, commit_state FROM ingestion_manifest ORDER BY delivery_id")),
        ["delivery_id", "source_name", "record_count", "byte_count",
         "completeness_token", "commit_state"])
    write_csv(
        tables / "quality_results.csv",
        db.rows_to_dicts(db.query(ctx.control,
            "SELECT check_id, scope_key, dimension, expected, actual, status "
            "FROM quality_result WHERE run_id = ? ORDER BY check_id", (ctx.run_id,))),
        ["check_id", "scope_key", "dimension", "expected", "actual", "status"])


def write_evidence_index(ctx: RunContext, incident_id: str) -> dict[str, Any]:
    ev = ctx.paths.evidence_dir
    artifacts: dict[str, Any] = {}
    for path in sorted(ev.rglob("*")):
        if path.is_file() and path.name != "evidence.json":
            rel = str(path.relative_to(ev))
            artifacts[rel] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size,
            }
    index = {
        "incident_id": incident_id,
        "run_id": ctx.run_id,
        "description": "Evidence pack. Symptom and logical-work facts only; no root-cause label.",
        "evidence_origin": EVIDENCE_ORIGIN,
        "artifacts": artifacts,
    }
    write_json(ctx.paths.evidence_index, index)
    return index
