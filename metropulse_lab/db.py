"""Thin SQLite helpers.

Design rules (lab_architecture.md sections 2, 16):
- data values are always bound as parameters; only allowlisted code chooses
  table/column names;
- every query that becomes evidence specifies a stable ``ORDER BY``;
- SQLite database-file bytes are never a golden assertion. Logical rows and
  sorted exports are the oracle.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


def connect(db_path: Path) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit transaction with rollback on exception."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def executescript(conn: sqlite3.Connection, sql_text: str) -> None:
    conn.executescript(sql_text)
    conn.commit()


def query(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, params).fetchall())


def query_one(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
    return conn.execute(sql, params).fetchone()


def scalar(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return None
    return row[0]


def insert_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> int:
    """Bulk insert. ``table`` and ``columns`` come only from allowlisted code."""
    placeholders = ", ".join("?" for _ in columns)
    collist = ", ".join(columns)
    sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders})"
    cur = conn.executemany(sql, [tuple(r) for r in rows])
    return cur.rowcount


def rows_to_dicts(rows: Sequence[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]


def sqlite_version(conn: sqlite3.Connection) -> str:
    return str(scalar(conn, "SELECT sqlite_version()"))
