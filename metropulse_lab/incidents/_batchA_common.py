"""Batch-A private helpers shared by MP-02 .. MP-05.

Only the four batch-A incident modules import this. It carries small utilities
that mirror the reference incident (mp_01.py): deterministic CSV export, the
read-only evidence index, a sanitized operational timeline, a quality_result
writer, and boundedness/idempotency helpers. Nothing here reads the wall clock
or the host time zone, and no function names a mechanism.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from .. import db
from ..checks.snapshots import table_logical_hash
from ..context import RunContext

# A single fictional-system notice reused by every batch-A alert.
FICTIONAL_NOTICE = (
    "MetroPulse is a fictional, synthetic city-transit system. These are lab "
    "values, not a real transit agency or a real incident."
)

# Operational pipeline components whose events may appear in the sanitized
# timeline. Injection/repair/recovery internals are excluded so the evidence
# pack cannot reveal the root cause early.
_TIMELINE_COMPONENTS = {"receive", "stage", "model", "publish"}


# ---------------------------------------------------------------------------
# Deterministic file helpers
# ---------------------------------------------------------------------------

def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    """Write a CSV with a fixed column order. ``None`` becomes an empty cell."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c) for c in columns})


def evidence_index(incident_id: str, run_id: str, ev: Path) -> dict[str, Any]:
    """sha256 + size for every evidence artifact except the index itself."""
    artifacts: dict[str, Any] = {}
    for path in sorted(ev.rglob("*")):
        if path.is_file() and path.name != "evidence.json":
            rel = str(path.relative_to(ev))
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


def sanitized_timeline(ctx: RunContext, include_logical_time: bool = True) -> list[dict[str, Any]]:
    """A bounded, filtered log excerpt: operational events only, no root cause.

    ``include_logical_time`` may be turned off by an incident whose leak-word list
    could collide with the lab clock's timestamp text (e.g. a boundary hour).
    Event order is preserved regardless.
    """
    out: list[dict[str, Any]] = []
    for order, e in enumerate(ctx.log.read_all()):
        if e.get("component") not in _TIMELINE_COMPONENTS:
            continue
        fields = {
            k: v for k, v in (e.get("fields") or {}).items()
            if k != "fault" and v is not None
        }
        entry: dict[str, Any] = {"event_order": order}
        if include_logical_time:
            entry["logical_time"] = e.get("logical_time")
        entry["component"] = e.get("component")
        entry["event_type"] = e.get("event_type")
        entry["fields"] = fields
        out.append(entry)
    return out


# ---------------------------------------------------------------------------
# Control-plane quality result
# ---------------------------------------------------------------------------

def write_quality_result(ctx: RunContext, check_id: str, scope_key: str,
                         dimension: str, expected: str, actual: str,
                         status: str, evidence_ptr: str = "evidence/tables") -> None:
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO quality_result "
            "(run_id, check_id, scope_key, dimension, expected, actual, status, evidence_ptr) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ctx.run_id, check_id, scope_key, dimension, expected, actual, status,
             evidence_ptr))


# ---------------------------------------------------------------------------
# Boundedness / idempotency helpers (logical rows are the oracle)
# ---------------------------------------------------------------------------

def capture_table_hashes(conn, tables: Iterable[str]) -> dict[str, str]:
    return {t: table_logical_hash(conn, t) for t in tables}


def write_hash_checkpoint(ctx: RunContext, filename: str, payload: dict[str, str]) -> None:
    path = ctx.paths.checkpoints_dir / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, sort_keys=True, indent=2)
        fh.write("\n")


def read_hash_checkpoint(ctx: RunContext, filename: str) -> dict[str, str]:
    path = ctx.paths.checkpoints_dir / filename
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)
