"""Structured, append-only event log.

``logs/events.jsonl`` uses a versioned schema (lab_architecture.md section 12.1).
Clues live in structured fields, not only in prose. Redaction happens before
serialization; stack traces are avoided in normal evidence.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from . import config

# Root-cause / solution words that must never leak into a reader-facing event.
# Detection and evidence report the failed invariant or abnormal metric, not the
# answer. (Enforced by contract tests over captured evidence.)
REDACT_MARKERS = ("ROOT_CAUSE", "SOLUTION", "ANSWER")


def _redact(value: Any) -> Any:
    if isinstance(value, str):
        for marker in REDACT_MARKERS:
            if marker in value:
                return "[redacted]"
        return value
    if isinstance(value, dict):
        return {k: _redact(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


class EventLog:
    def __init__(self, path: Path, run_id: str, incident_id: str) -> None:
        self.path = Path(path)
        self.run_id = run_id
        self.incident_id = incident_id
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(
        self,
        *,
        logical_time: str,
        component: str,
        event_type: str,
        level: str = "INFO",
        task: str | None = None,
        attempt: int | None = None,
        fields: dict[str, Any] | None = None,
    ) -> None:
        record = {
            "schema_version": config.SCHEMA_VERSION,
            "logical_time": logical_time,
            "run_id": self.run_id,
            "incident_id": self.incident_id,
            "component": component,
            "task": task,
            "attempt": attempt,
            "level": level,
            "event_type": event_type,
            "fields": _redact(fields or {}),
        }
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True, ensure_ascii=True) + "\n")

    def read_all(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        out: list[dict[str, Any]] = []
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out
