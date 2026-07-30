"""Shared helpers for the batch-C incidents (MP-11 .. MP-15).

The batch-C family is "Execution / dependency / access". Every incident in it is
a ``DETERMINISTIC_SIMULATOR``: the fault and its evidence live in the control
plane and in incident-private ``mp1X_*`` tables, never in the shared warehouse.
The warehouse baseline the pipeline built at ``create`` is left untouched, so the
reference lifecycle's "unrelated tables unchanged" check passes trivially.

This module centralises the pieces every batch-C incident repeats:

- deterministic integer derivation (``det_int``) with ``hashlib`` — never the
  builtin ``hash``;
- read-only evidence scaffolding (CSV writer, evidence index, sanitised
  timeline, alert writer);
- a ``quality_result`` writer and a fixture-integrity assertion helper.

Reader-facing text produced through these helpers must describe only the
observable symptom. Each incident keeps its frozen mechanism vocabulary out of
everything written under ``evidence/`` (enforced by ``tests/incidents/
test_batch_C.py``).
"""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path
from typing import Any, Iterable

from .. import db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, write_json

FICTIONAL_NOTICE = (
    "MetroPulse is a fictional, synthetic city-transit system. These are lab "
    "values produced by a deterministic simulator, not a real transit agency or "
    "a real incident."
)

# Components whose structured log events are safe to expose in a sanitised
# reader timeline. Injection/detection/repair/recovery internals name the
# mechanism and are always excluded.
DEFAULT_TIMELINE_COMPONENTS = frozenset(
    {"receive", "stage", "model", "publish", "task"}
)


def det_int(*parts: Any, mod: int) -> int:
    """Deterministic non-negative integer from stable inputs.

    Uses ``hashlib.sha256`` (never the process-seeded builtin ``hash``) so two
    runs on any host produce byte-identical evidence.
    """
    payload = "|".join(str(p) for p in parts).encode("utf-8")
    return int(hashlib.sha256(payload).hexdigest(), 16) % mod


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c) for c in columns})


def write_quality_result(
    ctx: RunContext,
    check_id: str,
    dimension: str,
    expected: str,
    actual: str,
    status: str,
    scope_key: str = "all",
    evidence_ptr: str = "evidence/tables/quality_results.csv",
) -> None:
    with db.transaction(ctx.control):
        ctx.control.execute(
            "INSERT OR REPLACE INTO quality_result "
            "(run_id, check_id, scope_key, dimension, expected, actual, status, evidence_ptr) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ctx.run_id, check_id, scope_key, dimension, expected, actual, status, evidence_ptr),
        )


def quality_result_rows(ctx: RunContext) -> list[dict[str, Any]]:
    return db.rows_to_dicts(
        db.query(
            ctx.control,
            "SELECT check_id, scope_key, dimension, expected, actual, status "
            "FROM quality_result WHERE run_id = ? ORDER BY check_id, scope_key",
            (ctx.run_id,),
        )
    )


def input_manifest_rows(ctx: RunContext) -> list[dict[str, Any]]:
    # ``completeness_token`` is projected to the neutral header
    # ``completeness_marker`` so a generic control-plane column name can never be
    # mistaken for an incident's forbidden mechanism vocabulary.
    return [
        {"delivery_id": r["delivery_id"], "source_name": r["source_name"],
         "record_count": r["record_count"], "byte_count": r["byte_count"],
         "completeness_marker": r["completeness_token"], "commit_state": r["commit_state"]}
        for r in db.query(
            ctx.control,
            "SELECT delivery_id, source_name, record_count, byte_count, "
            "completeness_token, commit_state FROM ingestion_manifest "
            "ORDER BY delivery_id",
        )
    ]


def write_standard_tables(ctx: RunContext) -> None:
    """Write the evidence tables every incident shares: input manifest and the
    detection quality results. Both are read-only projections of control state."""
    tables = ctx.paths.evidence_tables_dir
    tables.mkdir(parents=True, exist_ok=True)
    write_csv(
        tables / "input_manifest.csv",
        input_manifest_rows(ctx),
        ["delivery_id", "source_name", "record_count", "byte_count",
         "completeness_marker", "commit_state"],
    )
    write_csv(
        tables / "quality_results.csv",
        quality_result_rows(ctx),
        ["check_id", "scope_key", "dimension", "expected", "actual", "status"],
    )


def sanitized_timeline(
    ctx: RunContext, allowed: Iterable[str] = DEFAULT_TIMELINE_COMPONENTS
) -> list[dict[str, Any]]:
    """A bounded, filtered log excerpt: only operational events, never the
    injection/repair/recovery internals that would name the mechanism."""
    allow = set(allowed)
    out: list[dict[str, Any]] = []
    for e in ctx.log.read_all():
        if e.get("component") not in allow:
            continue
        fields = {k: v for k, v in (e.get("fields") or {}).items()
                  if k not in ("fault", "mechanism")}
        out.append({
            "logical_time": e.get("logical_time"),
            "component": e.get("component"),
            "event_type": e.get("event_type"),
            "fields": fields,
        })
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


def fixture_integrity_assertion(ctx: RunContext, assertion_id: str) -> Assertion:
    ok, detail = integrity.fixtures_untouched(ctx)
    return Assertion(
        assertion_id, "fixture_integrity",
        "run landing inputs match checked-in fixture checksums",
        expected=True, actual=ok,
        status="PASS" if ok else "FAIL",
        evidence=str(detail.get("status")),
    )
