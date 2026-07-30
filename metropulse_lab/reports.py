"""Stable JSON reports and assertion records.

Every lifecycle step returns a report that is serializable to deterministic JSON
(sorted keys, no wall-clock, no absolute host paths). A verification report never
emits a single unexplained ``PASS``: each assertion carries an id, expected
value, actual value, status, and an evidence pointer.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


def write_json(path: Path, payload: Any) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, sort_keys=True, indent=2, ensure_ascii=True)
        fh.write("\n")
    return path


def read_json(path: Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


@dataclass
class Assertion:
    """One verification assertion. Status is PASS or FAIL, never bare."""

    assertion_id: str
    dimension: str
    description: str
    expected: Any
    actual: Any
    status: str  # "PASS" | "FAIL"
    evidence: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class StepReport:
    """Common envelope for a lifecycle step's structured result."""

    step: str
    incident_id: str
    run_id: str
    status: str
    fields: dict[str, Any] = field(default_factory=dict)
    assertions: list[Assertion] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "incident_id": self.incident_id,
            "run_id": self.run_id,
            "status": self.status,
            "fields": self.fields,
            "assertions": [a.to_dict() for a in self.assertions],
        }

    def passed(self) -> bool:
        return all(a.status == "PASS" for a in self.assertions)


def summarize_assertions(assertions: list[Assertion]) -> dict[str, Any]:
    total = len(assertions)
    failed = [a.assertion_id for a in assertions if a.status != "PASS"]
    return {
        "total": total,
        "passed": total - len(failed),
        "failed": len(failed),
        "failed_ids": failed,
        "status": "PASS" if not failed else "FAIL",
    }
