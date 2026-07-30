"""Workspace and run-directory layout.

The default workspace is repository-local ``.lab/`` and is git-ignored. A run
directory holds everything an incident lifecycle produces; nothing is written
outside it during normal operation.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from . import safety

DEFAULT_WORKSPACE_DIRNAME = ".lab"


def package_root() -> Path:
    """Directory that contains the ``metropulse_lab`` package (the lab root)."""
    return Path(__file__).resolve().parent.parent


def fixtures_root() -> Path:
    return package_root() / "fixtures"


def sql_root() -> Path:
    return package_root() / "sql"


def default_workspace() -> Path:
    return package_root() / DEFAULT_WORKSPACE_DIRNAME


def runs_root(workspace: Path) -> Path:
    return Path(workspace) / "runs"


@dataclass(frozen=True)
class RunPaths:
    """Absolute paths inside one run directory. All are validated to stay inside."""

    run_dir: Path

    @property
    def run_json(self) -> Path:
        return self.run_dir / "run.json"

    @property
    def fault_profile(self) -> Path:
        return self.run_dir / "fault_profile.json"

    @property
    def input_dir(self) -> Path:
        return self.run_dir / "input"

    @property
    def landing_dir(self) -> Path:
        return self.input_dir / "landing"

    @property
    def reference_snapshot_dir(self) -> Path:
        return self.input_dir / "reference_snapshot"

    @property
    def state_db(self) -> Path:
        return self.run_dir / "state" / "control.sqlite3"

    @property
    def warehouse_db(self) -> Path:
        return self.run_dir / "warehouse" / "metropulse.sqlite3"

    @property
    def checkpoints_dir(self) -> Path:
        return self.run_dir / "checkpoints"

    @property
    def quarantine_dir(self) -> Path:
        return self.run_dir / "quarantine"

    @property
    def logs_dir(self) -> Path:
        return self.run_dir / "logs"

    @property
    def events_log(self) -> Path:
        return self.logs_dir / "events.jsonl"

    @property
    def evidence_dir(self) -> Path:
        return self.run_dir / "evidence"

    @property
    def evidence_tables_dir(self) -> Path:
        return self.evidence_dir / "tables"

    @property
    def evidence_index(self) -> Path:
        return self.evidence_dir / "evidence.json"

    @property
    def reports_dir(self) -> Path:
        return self.run_dir / "reports"

    @property
    def outbox_dir(self) -> Path:
        return self.run_dir / "outbox"

    @property
    def trash_dir(self) -> Path:
        # Recoverable local "trash" for lifecycle/orphan simulators. Never a
        # permanent delete of fixture data.
        return self.run_dir / "trash"

    def report_path(self, name: str) -> Path:
        return self.reports_dir / name

    def all_dirs(self) -> tuple[Path, ...]:
        return (
            self.run_dir,
            self.landing_dir,
            self.reference_snapshot_dir,
            self.run_dir / "state",
            self.run_dir / "warehouse",
            self.checkpoints_dir,
            self.quarantine_dir,
            self.logs_dir,
            self.evidence_tables_dir,
            self.reports_dir,
            self.outbox_dir,
            self.trash_dir,
        )

    def create_layout(self) -> None:
        for d in self.all_dirs():
            d.mkdir(parents=True, exist_ok=True)


def new_run_id(workspace: Path, incident_id: str) -> str:
    """Collision-free run id from the maximum existing numeric suffix + 1.

    ``run-mp01-0001`` for the first MP-01 run. Counting directories is unsafe:
    with ``run-mp01-0001`` and ``run-mp01-0003`` present, a count would return
    ``0003`` and collide. We take (max valid suffix + 1) and then probe to be
    certain the chosen directory does not already exist. The id may appear in
    output; logical rows, not the id, are the oracle.
    """
    inc = incident_id.replace("-", "").lower()
    prefix = f"run-{inc}-"
    root = runs_root(workspace)
    max_suffix = 0
    if root.exists():
        for p in root.iterdir():
            if p.is_dir() and p.name.startswith(prefix):
                suffix = p.name[len(prefix):]
                if suffix.isdigit():
                    max_suffix = max(max_suffix, int(suffix))
    n = max_suffix + 1
    while (root / f"{prefix}{n:04d}").exists():
        n += 1
    return f"{prefix}{n:04d}"


def run_dir_for(workspace: Path, run_id: str) -> Path:
    """Resolve and validate a run directory for a given run id."""
    root = runs_root(workspace)
    return safety.resolve_within(root, run_id)
