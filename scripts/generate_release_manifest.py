"""Write a deterministic hash ledger for the public first-edition release."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "release-manifest.json"
INCLUDED_TOP_LEVEL = {
    ".gitattributes",
    ".gitignore",
    "CHANGELOG.md",
    "LICENSE",
    "Makefile",
    "NOTICE.md",
    "README.md",
    "SECURITY.md",
    "SUPPORT.md",
    "pyproject.toml",
}
INCLUDED_DIRECTORIES = {
    ".github",
    "docs",
    "fixtures",
    "metropulse_lab",
    "scripts",
    "sql",
    "tests",
}


def included(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    if relative == Path("release-manifest.json"):
        return False
    if any(part in {".git", "__pycache__"} for part in relative.parts):
        return False
    if path.name.endswith((".pyc", ".pyo")):
        return False
    return (
        len(relative.parts) == 1 and relative.name in INCLUDED_TOP_LEVEL
    ) or relative.parts[0] in INCLUDED_DIRECTORIES


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    files = []
    for path in sorted(ROOT.rglob("*")):
        if path.is_file() and included(path):
            files.append(
                {
                    "path": path.relative_to(ROOT).as_posix(),
                    "sha256": sha256(path),
                    "bytes": path.stat().st_size,
                }
            )
    manifest = {
        "release": "v1.0.0",
        "release_date": "2026-07-31",
        "book": "The Data Pipeline Troubleshooting Lab",
        "author": "MASON YE",
        "supported_python": "CPython 3.12",
        "fixture_catalog_version": "1.0",
        "baseline_id": "metro-v1",
        "incident_ids": [f"MP-{number:02d}" for number in range(1, 26)],
        "files": files,
    }
    OUTPUT.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {OUTPUT} with {len(files)} files")


if __name__ == "__main__":
    main()
