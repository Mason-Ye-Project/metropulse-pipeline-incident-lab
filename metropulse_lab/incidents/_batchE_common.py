"""Shared helpers for the batch-E incidents (MP-21 .. MP-25).

Batch E is the "Recovery accidents" family: publish/object commit-order gaps,
retention shorter than a promised replay window, whole-history reverts that erase
an unrelated good change, maintenance that deletes an in-progress writer's file,
and a target/feed pair restored to inconsistent positions.

Every batch-E incident is a ``DETERMINISTIC_SIMULATOR``: the fault and its
evidence live in the control plane, in incident-private ``mp2X_*`` tables, in a
run-local **object store** simulator (``object_inventory`` + files under
``objectstore/``), and in a **recoverable trash** directory
(``ctx.paths.trash_dir``). The shared warehouse baseline the pipeline built at
``create`` is never touched, so the reference lifecycle's "unrelated tables
unchanged" check passes trivially and ``affected_tables`` is empty.

Determinism rules (lab_architecture.md section 2 and 10.1): no wall clock, no
builtin ``hash()`` (use ``hashlib``), no threads/sleeps (interleavings come from a
declared event queue), stable ``ORDER BY`` on evidence queries, ISO-8601 UTC only,
and objects are moved to recoverable trash — never permanently deleted. The
logical rows are the oracle: two runs in two ``--workspace`` dirs produce
byte-identical evidence.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Sequence

from .. import config, db
from ..checks import integrity
from ..checks.snapshots import table_logical_hash
from ..context import RunContext
from ..reports import Assertion, read_json, write_json

FICTIONAL_NOTICE = (
    "MetroPulse is a fictional, synthetic city-transit system. These are lab "
    "values produced by a deterministic simulator, not a real transit agency or "
    "a real incident."
)

# Shared warehouse/mart tables. A batch-E fault only ever changes incident-private
# tables, control ledgers, and the run-local object store, so each of these must
# be byte-identical between the faulty-run snapshot and the recovered snapshot.
SHARED_TABLES = tuple(config.WAREHOUSE_TABLES) + tuple(config.MART_TABLES)
FAULTY_SNAPSHOT = "faulty_snapshots.json"

OBJECTSTORE_DIRNAME = "objectstore"

# Object lifecycle states used by the local object-store simulator.
OBJ_DURABLE = "DURABLE"      # bytes written, checksum verified, readable
OBJ_PENDING = "PENDING"      # referenced but bytes not yet written/verified
OBJ_WRITING = "WRITING"      # an active write is in progress
OBJ_REMOVED = "REMOVED"      # moved to recoverable trash (not permanently deleted)


# ---------------------------------------------------------------------------
# Deterministic primitives
# ---------------------------------------------------------------------------

def det_int(*parts: Any, mod: int) -> int:
    """Deterministic non-negative integer from stable inputs (never builtin hash)."""
    payload = "|".join(str(p) for p in parts).encode("utf-8")
    return int(hashlib.sha256(payload).hexdigest(), 16) % mod


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


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


def sanitized_timeline(ctx: RunContext, leak_tokens: Iterable[str],
                       allowed: Iterable[str] | None = None) -> list[dict[str, Any]]:
    """A bounded, filtered log excerpt: operational events only.

    Injection/detection/repair/recovery internals live under non-operational
    components and are dropped. As a final guard, any entry whose serialized form
    contains a mechanism leak token is dropped as well.
    """
    allow = set(allowed) if allowed is not None else {
        "receive", "stage", "model", "publish", "feed", "writer", "reader", "publisher"
    }
    lowered = [t.lower() for t in leak_tokens]
    out: list[dict[str, Any]] = []
    for e in ctx.log.read_all():
        if e.get("component") not in allow:
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
        "description": "Evidence pack. Symptom and facts only; no root-cause label.",
        "evidence_origin": "metropulse_deterministic_simulator",
        "artifacts": artifacts,
    }
    write_json(ctx.paths.evidence_index, index)
    return index


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


# ---------------------------------------------------------------------------
# Control-plane helpers
# ---------------------------------------------------------------------------

def write_quality_result(ctx: RunContext, check_id: str, dimension: str,
                         expected: str, actual: str, status: str,
                         scope_key: str = "all",
                         evidence_ptr: str = "evidence/tables/quality_results.csv") -> None:
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO quality_result "
            "(run_id, check_id, scope_key, dimension, expected, actual, status, evidence_ptr) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ctx.run_id, check_id, scope_key, dimension, expected, actual, status, evidence_ptr))


def quality_result_rows(ctx: RunContext) -> list[dict[str, Any]]:
    return db.rows_to_dicts(db.query(
        ctx.control,
        "SELECT check_id, scope_key, dimension, expected, actual, status "
        "FROM quality_result WHERE run_id = ? ORDER BY check_id, scope_key",
        (ctx.run_id,)))


def write_quality_results_csv(ctx: RunContext) -> None:
    write_csv(
        ctx.paths.evidence_tables_dir / "quality_results.csv",
        quality_result_rows(ctx),
        ["check_id", "scope_key", "dimension", "expected", "actual", "status"],
    )


def record_outbox(ctx: RunContext, message_id: str, topic: str, payload: dict[str, Any]) -> None:
    """Idempotent local-only notification. Fixed logical time so a second recovery
    reproduces byte-identical rows."""
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO simulated_outbox "
            "(message_id, topic, payload_json, logical_time) VALUES (?, ?, ?, ?)",
            (message_id, topic, json.dumps(payload, sort_keys=True), ctx.clock.at_offset(0)))


def outbox_count(ctx: RunContext, message_id: str) -> int:
    return db.scalar(ctx.control,
                     "SELECT COUNT(*) FROM simulated_outbox WHERE message_id = ?",
                     (message_id,))


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

    A batch-E recovery only touches incident-private tables, control ledgers and
    the object store, so this is empty both before repair and after recovery.
    """
    snap = read_faulty_snapshot(ctx)
    if not snap:
        return []
    return [t for t in SHARED_TABLES if snap.get(t) != table_logical_hash(ctx.warehouse, t)]


def fixture_integrity_assertion(ctx: RunContext, assertion_id: str) -> Assertion:
    ok, detail = integrity.fixtures_untouched(ctx)
    return Assertion(
        assertion_id, "fixture_integrity",
        "run landing inputs match checked-in fixture checksums",
        expected=True, actual=ok,
        status="PASS" if ok else "FAIL",
        evidence=str(detail.get("status")))


# ---------------------------------------------------------------------------
# Local object-store simulator (object_inventory + files under objectstore/)
# ---------------------------------------------------------------------------

def objectstore_dir(ctx: RunContext) -> Path:
    d = ctx.paths.run_dir / OBJECTSTORE_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _object_abs(ctx: RunContext, run_relative_path: str) -> Path:
    return ctx.paths.run_dir / run_relative_path


def object_relpath(object_id: str) -> str:
    return f"{OBJECTSTORE_DIRNAME}/{object_id}.dat"


def put_object(ctx: RunContext, object_id: str, content: bytes,
               active_writer: str | None = None,
               lifecycle_state: str = OBJ_DURABLE) -> str:
    """Write object bytes and record the row with a checksum over those bytes.

    Defaults to ``DURABLE``; pass ``OBJ_WRITING`` to model bytes that are present
    on the store but not yet committed (the writer still holds them).
    """
    objectstore_dir(ctx)
    rel = object_relpath(object_id)
    _object_abs(ctx, rel).write_bytes(content)
    checksum = sha256_bytes(content)
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO object_inventory "
            "(object_id, run_relative_path, checksum, lifecycle_state, active_writer) "
            "VALUES (?, ?, ?, ?, ?)",
            (object_id, rel, checksum, lifecycle_state, active_writer))
    return checksum


def commit_object(ctx: RunContext, object_id: str) -> bool:
    """Bring an object to DURABLE from bytes already present on the store.

    Used when an in-progress write finishes: the bytes exist, so the object is
    marked committed and the active-writer marker cleared. No-op if already
    durable or if the bytes are not present.
    """
    row = object_row(ctx, object_id)
    if row is None or object_durable(ctx, object_id):
        return False
    dest = _object_abs(ctx, row["run_relative_path"])
    if not dest.exists():
        return False
    checksum = sha256_bytes(dest.read_bytes())
    with db.transaction(ctx.control):
        ctx.control.execute(
            "UPDATE object_inventory SET lifecycle_state = ?, checksum = ?, active_writer = NULL "
            "WHERE object_id = ?", (OBJ_DURABLE, checksum, object_id))
    return True


def register_object(ctx: RunContext, object_id: str, checksum: str | None,
                    lifecycle_state: str, active_writer: str | None = None) -> None:
    """Record an object row without writing bytes (e.g. a PENDING/WRITING ref)."""
    rel = object_relpath(object_id)
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO object_inventory "
            "(object_id, run_relative_path, checksum, lifecycle_state, active_writer) "
            "VALUES (?, ?, ?, ?, ?)",
            (object_id, rel, checksum, lifecycle_state, active_writer))


def object_row(ctx: RunContext, object_id: str) -> dict[str, Any] | None:
    row = db.query_one(
        ctx.control,
        "SELECT object_id, run_relative_path, checksum, lifecycle_state, active_writer "
        "FROM object_inventory WHERE object_id = ?", (object_id,))
    return dict(row) if row is not None else None


def object_durable(ctx: RunContext, object_id: str) -> bool:
    """True iff the object is DURABLE, its file exists, and the checksum matches."""
    row = object_row(ctx, object_id)
    if row is None or row["lifecycle_state"] != OBJ_DURABLE:
        return False
    path = _object_abs(ctx, row["run_relative_path"])
    if not path.exists():
        return False
    return sha256_bytes(path.read_bytes()) == row["checksum"]


def read_object(ctx: RunContext, object_id: str) -> bytes | None:
    """Return object bytes if it resolves to a durable object, else None."""
    if not object_durable(ctx, object_id):
        return None
    row = object_row(ctx, object_id)
    return _object_abs(ctx, row["run_relative_path"]).read_bytes()


def trash_object(ctx: RunContext, object_id: str, set_writer_null: bool = False) -> bool:
    """Move an object's bytes to recoverable trash (never a permanent delete).

    Records the object REMOVED. Returns True if a file was moved.
    """
    row = object_row(ctx, object_id)
    if row is None:
        return False
    src = _object_abs(ctx, row["run_relative_path"])
    moved = False
    trash_dir = ctx.paths.trash_dir
    trash_dir.mkdir(parents=True, exist_ok=True)
    if src.exists():
        dest = trash_dir / f"{object_id}.dat"
        src.replace(dest)
        moved = True
    with db.transaction(ctx.control):
        if set_writer_null:
            ctx.control.execute(
                "UPDATE object_inventory SET lifecycle_state = ?, active_writer = NULL "
                "WHERE object_id = ?", (OBJ_REMOVED, object_id))
        else:
            ctx.control.execute(
                "UPDATE object_inventory SET lifecycle_state = ? WHERE object_id = ?",
                (OBJ_REMOVED, object_id))
    return moved


def restore_object(ctx: RunContext, object_id: str) -> bool:
    """Move an object's bytes back from recoverable trash and mark it DURABLE.

    Idempotent: if the object is already durable and present, this is a no-op.
    """
    row = object_row(ctx, object_id)
    if row is None:
        return False
    dest = _object_abs(ctx, row["run_relative_path"])
    if object_durable(ctx, object_id):
        return False
    trash_file = ctx.paths.trash_dir / f"{object_id}.dat"
    if trash_file.exists():
        objectstore_dir(ctx)
        trash_file.replace(dest)
    if not dest.exists():
        return False
    checksum = sha256_bytes(dest.read_bytes())
    with db.transaction(ctx.control):
        ctx.control.execute(
            "UPDATE object_inventory SET lifecycle_state = ?, checksum = ?, active_writer = NULL "
            "WHERE object_id = ?", (OBJ_DURABLE, checksum, object_id))
    return True


def object_inventory_rows(ctx: RunContext) -> list[dict[str, Any]]:
    return db.rows_to_dicts(db.query(
        ctx.control,
        "SELECT object_id, lifecycle_state, checksum FROM object_inventory "
        "ORDER BY object_id"))
