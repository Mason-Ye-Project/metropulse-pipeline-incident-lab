"""Logical-row snapshots.

SQLite database-file bytes are never an oracle. Instead we hash the *logical
rows* of a table exported in a stable order. Snapshots let verification prove
boundedness (unrelated tables unchanged) and idempotency (a second recovery
changes nothing).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Iterable


def table_logical_rows(conn: sqlite3.Connection, table: str) -> list[dict]:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    order = ", ".join(f'"{c}"' for c in cols)
    rows = conn.execute(f"SELECT * FROM {table} ORDER BY {order}").fetchall()
    return [dict(r) for r in rows]


def table_logical_hash(conn: sqlite3.Connection, table: str) -> str:
    rows = table_logical_rows(conn, table)
    payload = json.dumps(rows, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# Provenance columns record which publication wrote a row. A full pipeline run
# legitimately re-publishes unrelated tables with a new publication id, so a
# domain-content comparison excludes these.
PROVENANCE_COLUMNS = frozenset({
    "source_publication", "publication_id", "serving_version",
    "source_publication_ids", "snapshot_version",
})


def table_domain_hash(conn: sqlite3.Connection, table: str) -> str:
    """Logical hash of a table's rows excluding provenance columns."""
    rows = table_logical_rows(conn, table)
    stripped = [
        {k: v for k, v in row.items() if k not in PROVENANCE_COLUMNS}
        for row in rows
    ]
    payload = json.dumps(stripped, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def domain_snapshot(conn: sqlite3.Connection, tables: Iterable[str]) -> dict[str, str]:
    return {t: table_domain_hash(conn, t) for t in tables}


def user_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def snapshot_all_tables(conn: sqlite3.Connection) -> dict[str, str]:
    """Logical hash of every user table. Incident-agnostic: automatically
    covers control-plane and incident-private tables an incident may create."""
    return {t: table_logical_hash(conn, t) for t in user_tables(conn)}


def snapshot_tables(conn: sqlite3.Connection, tables: Iterable[str]) -> dict[str, str]:
    return {t: table_logical_hash(conn, t) for t in tables}
