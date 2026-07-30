"""Shared fixture-integrity primitive.

Injection and verification both prove that a run's landing inputs still match the
checked-in baseline fixtures (lab_architecture.md sections 10 and 13.1). Faults
mutate run data, control metadata, policy, or the fault profile — never the
canonical fixtures.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ..context import RunContext
from ..paths import fixtures_root


def landing_checksums(landing_dir: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    landing = Path(landing_dir)
    if not landing.exists():
        return out
    for f in sorted(landing.iterdir()):
        if f.is_file():
            out[f.name] = hashlib.sha256(f.read_bytes()).hexdigest()
    return out


def baseline_checksums() -> dict[str, str]:
    checksums_file = fixtures_root() / "baseline" / "input_checksums.json"
    if not checksums_file.exists():
        return {}
    with checksums_file.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def fixtures_untouched(ctx: RunContext) -> tuple[bool, dict[str, Any]]:
    """True iff run landing inputs equal the checked-in baseline checksums."""
    expected = baseline_checksums()
    actual = landing_checksums(ctx.paths.landing_dir)
    if not expected:
        return True, {"status": "skipped", "reason": "no baseline checksum file"}
    mismatches = [name for name, sha in expected.items()
                  if actual.get(name) != sha]
    return (not mismatches), {
        "status": "ok" if not mismatches else "mismatch",
        "files_checked": len(expected),
        "mismatches": mismatches,
    }
