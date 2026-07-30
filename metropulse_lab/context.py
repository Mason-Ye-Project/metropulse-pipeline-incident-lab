"""RunContext: the handle every lifecycle step receives.

It owns the run's paths, lazy database connections, structured event log, logical
clock, and fault profile. It never reads wall-clock time for anything that
becomes evidence.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from . import config, db
from .clock import LogicalClock
from .events import EventLog
from .paths import RunPaths


class RunContext:
    def __init__(self, run_id: str, incident_id: str, run_dir: Path, workspace: Path) -> None:
        self.run_id = run_id
        self.incident_id = incident_id
        self.workspace = Path(workspace)
        self.paths = RunPaths(Path(run_dir))
        self.log = EventLog(self.paths.events_log, run_id, incident_id)
        self.clock = LogicalClock(config.BASELINE_CLOCK_START_UTC)
        self._control: sqlite3.Connection | None = None
        self._warehouse: sqlite3.Connection | None = None

    # -- database connections -------------------------------------------------
    @property
    def control(self) -> sqlite3.Connection:
        if self._control is None:
            self._control = db.connect(self.paths.state_db)
        return self._control

    @property
    def warehouse(self) -> sqlite3.Connection:
        if self._warehouse is None:
            self._warehouse = db.connect(self.paths.warehouse_db)
        return self._warehouse

    def close(self) -> None:
        for conn in (self._control, self._warehouse):
            if conn is not None:
                conn.close()
        self._control = None
        self._warehouse = None

    # -- fault profile --------------------------------------------------------
    def load_fault_profile(self) -> dict[str, Any]:
        if self.paths.fault_profile.exists():
            with self.paths.fault_profile.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        return {"incident_id": self.incident_id, "faulty_transforms": [], "params": {}}

    def save_fault_profile(self, profile: dict[str, Any]) -> None:
        with self.paths.fault_profile.open("w", encoding="utf-8") as fh:
            json.dump(profile, fh, sort_keys=True, indent=2)
            fh.write("\n")

    def fault_active(self, name: str) -> bool:
        return name in self.load_fault_profile().get("faulty_transforms", [])

    # -- run relative paths for portable reports ------------------------------
    def run_relative(self, path: Path) -> str:
        try:
            return str(Path(path).resolve().relative_to(self.paths.run_dir.resolve()))
        except ValueError:
            return Path(path).name
