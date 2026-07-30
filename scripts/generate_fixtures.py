"""Generate the checked-in deterministic baseline fixtures.

Run from the lab root:

    python -m scripts.generate_fixtures      # if lab root on sys.path
    python scripts/generate_fixtures.py

Writes fixtures/baseline/input/*, fixtures/baseline/input_checksums.json, and a
combined fixtures/checksums.sha256. Re-running is a no-op if the data is
unchanged, because generation is fully deterministic.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent
if str(LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(LAB_ROOT))

from metropulse_lab.baseline import write_baseline_inputs  # noqa: E402


def main() -> int:
    fixtures = LAB_ROOT / "fixtures" / "baseline"
    input_dir = fixtures / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    checksums = write_baseline_inputs(input_dir)

    with (fixtures / "input_checksums.json").open("w", encoding="utf-8") as fh:
        json.dump(checksums, fh, sort_keys=True, indent=2)
        fh.write("\n")

    # Combined checksum manifest for the whole fixtures tree.
    combined = {}
    root = LAB_ROOT / "fixtures"
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "checksums.sha256":
            rel = path.relative_to(root).as_posix()
            combined[rel] = hashlib.sha256(path.read_bytes()).hexdigest()
    with (root / "checksums.sha256").open("w", encoding="utf-8") as fh:
        for rel, sha in combined.items():
            fh.write(f"{sha}  {rel}\n")

    print(f"wrote {len(checksums)} baseline input files to {input_dir}")
    for name, sha in checksums.items():
        print(f"  {sha[:12]}  {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
