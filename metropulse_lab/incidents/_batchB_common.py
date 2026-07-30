"""Shared helpers for the "Missing or late" incident family (MP-06 .. MP-10).

These incidents are self-contained: they carry their own ``mp0X_*`` tables and
control ledgers so they never edit the shared pipeline or its schema. This module
factors out the deterministic plumbing every one of them needs — CSV export,
evidence indexing, sanitized timelines, quality-result rows, an idempotent
outbox, and the boundedness check against the faulty-run snapshot.

Determinism rules (lab_architecture.md section 2): no wall clock, no builtin
``hash()``, stable ORDER BY on evidence queries, ISO-8601 UTC only. The logical
rows are the oracle. Two runs in two ``--workspace`` dirs produce byte-identical
evidence CSVs.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from .. import config, db
from ..context import RunContext
from ..reports import read_json, write_json

# Shared warehouse/mart tables. A batch-B fault only ever changes incident-private
# tables and control ledgers, so every one of these must be byte-identical between
# the baseline snapshot and the recovered snapshot.
SHARED_TABLES = tuple(config.WAREHOUSE_TABLES) + tuple(config.MART_TABLES)

# The snapshot the ``run`` command persists after the faulty pipeline (see
# commands.FAULTY_SNAPSHOT). Recovery must leave every shared table exactly as the
# faulty run left it, proving boundedness.
FAULTY_SNAPSHOT = "faulty_snapshots.json"

FICTIONAL_NOTICE = (
    "MetroPulse is a fictional, synthetic city-transit system. These are lab "
    "values, not a real transit agency or a real incident."
)


# ---------------------------------------------------------------------------
# CSV / evidence plumbing
# ---------------------------------------------------------------------------

def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    """Write a UTF-8 CSV with a fixed column order and ``\\n`` line endings.

    Rows must already be sorted by the caller so the bytes are deterministic.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c) for c in columns})


def evidence_index(incident_id: str, run_id: str, ev_dir: Path) -> dict[str, Any]:
    """Index every artifact under the evidence dir with its sha256 and size."""
    artifacts: dict[str, Any] = {}
    for path in sorted(ev_dir.rglob("*")):
        if path.is_file() and path.name != "evidence.json":
            rel = str(path.relative_to(ev_dir))
            artifacts[rel] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size,
            }
    return {
        "incident_id": incident_id,
        "run_id": run_id,
        "description": "Evidence pack. Symptom and facts only; no root-cause label.",
        "artifacts": artifacts,
    }


def sanitized_timeline(ctx: RunContext, leak_tokens: Iterable[str]) -> list[dict[str, Any]]:
    """A bounded, filtered log excerpt: operational events only.

    Injection/repair/recovery internals live under non-operational components and
    are dropped. As a final guard, any entry whose serialized form contains a
    mechanism leak token is dropped as well.
    """
    allowed = {"ingest", "publish", "serve", "read", "receive"}
    lowered = [t.lower() for t in leak_tokens]
    out: list[dict[str, Any]] = []
    for e in ctx.log.read_all():
        if e.get("component") not in allowed:
            continue
        entry = {
            "logical_time": e.get("logical_time"),
            "component": e.get("component"),
            "event_type": e.get("event_type"),
            "fields": e.get("fields") or {},
        }
        blob = json.dumps(entry, sort_keys=True).lower()
        if any(tok in blob for tok in lowered):
            continue
        out.append(entry)
    return out


# ---------------------------------------------------------------------------
# Control-plane helpers
# ---------------------------------------------------------------------------

def write_quality_result(ctx: RunContext, check_id: str, dimension: str,
                         expected: str, actual: str, status: str,
                         scope_key: str = "all") -> None:
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO quality_result "
            "(run_id, check_id, scope_key, dimension, expected, actual, status, evidence_ptr) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'evidence/tables/quality_results.csv')",
            (ctx.run_id, check_id, scope_key, dimension, expected, actual, status))


def quality_results_rows(ctx: RunContext) -> list[dict[str, Any]]:
    return db.rows_to_dicts(db.query(
        ctx.control,
        "SELECT check_id, scope_key, dimension, expected, actual, status "
        "FROM quality_result WHERE run_id = ? ORDER BY check_id, scope_key",
        (ctx.run_id,)))


def record_outbox(ctx: RunContext, message_id: str, topic: str, payload: dict[str, Any]) -> None:
    """Idempotent local-only notification. Fixed logical time so a second recovery
    reproduces byte-identical rows."""
    logical_time = ctx.clock.at_offset(0)
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO simulated_outbox "
            "(message_id, topic, payload_json, logical_time) VALUES (?, ?, ?, ?)",
            (message_id, topic, json.dumps(payload, sort_keys=True), logical_time))


def outbox_count(ctx: RunContext, message_id: str) -> int:
    return db.scalar(ctx.control,
                     "SELECT COUNT(*) FROM simulated_outbox WHERE message_id = ?",
                     (message_id,))


def upsert_dataset_version(ctx: RunContext, dataset_version_id: str, dataset_name: str,
                           target_scope: str, state: str, referenced_object: str) -> None:
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO dataset_version "
            "(dataset_version_id, dataset_name, target_scope, state, referenced_object, "
            " catalog_version, created_logical_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (dataset_version_id, dataset_name, target_scope, state, referenced_object,
             config.CATALOG_VERSION, ctx.clock.at_offset(0)))


def set_dataset_state(ctx: RunContext, dataset_name: str, new_state: str,
                      from_state: str | None = None) -> None:
    with db.transaction(ctx.control):
        if from_state is None:
            ctx.control.execute(
                "UPDATE dataset_version SET state = ? WHERE dataset_name = ?",
                (new_state, dataset_name))
        else:
            ctx.control.execute(
                "UPDATE dataset_version SET state = ? WHERE dataset_name = ? AND state = ?",
                (new_state, dataset_name, from_state))


# ---------------------------------------------------------------------------
# Boundedness against the faulty-run snapshot
# ---------------------------------------------------------------------------

def read_faulty_snapshot(ctx: RunContext) -> dict[str, str]:
    path = ctx.paths.checkpoints_dir / FAULTY_SNAPSHOT
    if not path.exists():
        return {}
    return read_json(path)


def shared_tables_changed(ctx: RunContext) -> list[str]:
    """Shared warehouse/mart tables that differ from the faulty-run snapshot.

    A batch-B recovery only touches incident-private tables, so this must be empty
    both before repair (nothing changed yet) and after recovery.
    """
    from ..checks.snapshots import table_logical_hash
    snap = read_faulty_snapshot(ctx)
    if not snap:
        return []
    return [t for t in SHARED_TABLES if snap.get(t) != table_logical_hash(ctx.warehouse, t)]


# ---------------------------------------------------------------------------
# Alert scaffold
# ---------------------------------------------------------------------------

def base_alert(incident_id: str, title: str, reported_symptom: str,
               time_pressure: str, affected_product: str,
               extra: dict[str, Any] | None = None) -> dict[str, Any]:
    alert = {
        "incident_id": incident_id,
        "title": title,
        "reported_symptom": reported_symptom,
        "time_pressure": time_pressure,
        "affected_product": affected_product,
        "fictional_notice": FICTIONAL_NOTICE,
    }
    if extra:
        alert.update(extra)
    return alert
