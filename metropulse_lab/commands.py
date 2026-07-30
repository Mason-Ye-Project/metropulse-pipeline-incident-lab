"""Command implementations behind the CLI.

Each command validates lifecycle state, holds the run-local operation lease, and
returns a plain dict result plus an exit code. Human output and JSON emission are
handled in ``cli.py``.
"""

from __future__ import annotations

import platform
import sqlite3
import sys
from pathlib import Path
from typing import Any

from . import config, db, paths, state
from .checks.snapshots import snapshot_tables
from .context import RunContext
from .incidents import registry
from .paths import RunPaths, runs_root
from .pipeline import build
from .reports import read_json, write_json
from . import safety
from .safety import RunLease, UnsafePathError, safe_rmtree_run

BASELINE_SNAPSHOT = "baseline_snapshots.json"
FAULTY_SNAPSHOT = "faulty_snapshots.json"
SNAPSHOT_TABLES = tuple(config.WAREHOUSE_TABLES) + tuple(config.MART_TABLES)


class CommandError(Exception):
    def __init__(self, message: str, exit_code: int) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def _workspace(args) -> Path:
    return Path(getattr(args, "workspace", None) or paths.default_workspace())


def _resolve_run(args) -> tuple[Path, str]:
    run_id = args.run
    workspace = _workspace(args)
    run_dir = paths.run_dir_for(workspace, run_id)
    if not run_dir.exists():
        raise CommandError(f"run not found: {run_id}", config.EXIT_USAGE)
    return run_dir, run_id


def _write_run_json(ctx: RunContext, lifecycle_state: str) -> None:
    write_json(ctx.paths.run_json, {
        "run_id": ctx.run_id,
        "incident_id": ctx.incident_id,
        "catalog_version": config.CATALOG_VERSION,
        "baseline_id": config.BASELINE_ID,
        "runtime_profile": config.RUNTIME_PROFILE,
        "lifecycle_state": lifecycle_state,
        "logical_date": config.BASELINE_SERVICE_DATES[0],
    })


def _snapshot(ctx: RunContext, filename: str) -> None:
    snap = snapshot_tables(ctx.warehouse, SNAPSHOT_TABLES)
    write_json(ctx.paths.checkpoints_dir / filename, snap)


# ---------------------------------------------------------------------------
# doctor / list
# ---------------------------------------------------------------------------

def doctor(args) -> dict[str, Any]:
    conn = sqlite3.connect(":memory:")
    window_ok = True
    try:
        conn.execute("CREATE TABLE t(a INTEGER)")
        conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
        conn.execute("SELECT a, LAG(a) OVER (ORDER BY a) FROM t").fetchall()
    except sqlite3.OperationalError:
        window_ok = False
    finally:
        conn.close()
    py_ok = sys.version_info[:2] >= (3, 12)
    healthy = py_ok and window_ok
    data = {
        "python_version": platform.python_version(),
        "python_ok": py_ok,
        "sqlite_version": sqlite3.sqlite_version,
        "window_functions": window_ok,
        "network_required": False,
        "status": "ok" if healthy else "degraded",
    }
    return {"ok": healthy,
            "exit_code": config.EXIT_OK if healthy else config.EXIT_INTERNAL,
            "data": data}


def incidents_list(args) -> dict[str, Any]:
    return {"ok": True, "exit_code": config.EXIT_OK,
            "data": {"incidents": registry.list_incidents(),
                     "implemented": registry.implemented_ids()}}


# ---------------------------------------------------------------------------
# create + baseline
# ---------------------------------------------------------------------------

def create(args) -> dict[str, Any]:
    incident_id = args.incident
    if not registry.is_valid_id(incident_id):
        raise CommandError(f"unknown incident id: {incident_id}", config.EXIT_USAGE)
    try:
        registry.load(incident_id)
    except NotImplementedError as exc:
        raise CommandError(str(exc), config.EXIT_USAGE) from exc

    workspace = _workspace(args)
    run_id = paths.new_run_id(workspace, incident_id)
    run_dir = paths.run_dir_for(workspace, run_id)
    rp = RunPaths(run_dir)
    rp.create_layout()

    ctx = RunContext(run_id, incident_id, run_dir, workspace)
    with RunLease(run_dir, "create"):
        # Apply schemas.
        db.executescript(ctx.warehouse, (paths.sql_root() / "schema.sql").read_text())
        db.executescript(ctx.control, (paths.sql_root() / "control_schema.sql").read_text())
        now = ctx.clock.now()
        ctx.control.execute(
            "INSERT INTO pipeline_run (run_id, incident_id, catalog_version, baseline_id, "
            "logical_date, lifecycle_state, fault_profile_json, created_logical_time, "
            "updated_logical_time) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            (run_id, incident_id, config.CATALOG_VERSION, config.BASELINE_ID,
             config.BASELINE_SERVICE_DATES[0], state.CREATED, now, now))
        ctx.control.commit()
        ctx.save_fault_profile({"incident_id": incident_id, "faulty_transforms": [], "params": {}})

        _materialize_fixtures(ctx)
        build.build_all(ctx, "baseline")
        # Incident-specific clean scenario (self-contained incidents).
        registry.load(incident_id).build_baseline(ctx)
        state.advance(ctx.control, run_id, "baseline", ctx.clock.now())
        _snapshot(ctx, BASELINE_SNAPSHOT)
        _write_run_json(ctx, state.BASELINED)
    ctx.close()
    return {"ok": True, "exit_code": config.EXIT_OK,
            "data": {"run_id": run_id, "incident_id": incident_id,
                     "lifecycle_state": state.BASELINED}}


def _materialize_fixtures(ctx: RunContext) -> None:
    """Copy checked-in baseline inputs into the run landing dir (copy, not link)."""
    import shutil
    from .safety import assert_no_symlink_in_tree

    src = paths.fixtures_root() / "baseline" / "input"
    if not src.exists():
        raise CommandError(
            "baseline fixtures missing; run scripts/generate_fixtures.py first",
            config.EXIT_INTEGRITY_FAILED)
    assert_no_symlink_in_tree(src)
    dest = ctx.paths.landing_dir
    dest.mkdir(parents=True, exist_ok=True)
    for f in sorted(src.iterdir()):
        if f.is_file():
            shutil.copy2(f, dest / f.name)


# ---------------------------------------------------------------------------
# lifecycle steps
# ---------------------------------------------------------------------------

def _run_step(args, command: str, method_name: str,
              snapshot_after: str | None = None) -> dict[str, Any]:
    run_dir, run_id = _resolve_run(args)
    workspace = _workspace(args)
    row = _read_incident_id(run_dir)
    ctx = RunContext(run_id, row, run_dir, workspace)
    incident = registry.load(row)
    result: dict[str, Any]
    with RunLease(run_dir, command):
        state.require_state(ctx.control, run_id, command)
        report = getattr(incident, method_name)(ctx)
        step_succeeded = not (
            command == "recover" and report.status != "recovered"
        )
        if step_succeeded:
            state.advance(ctx.control, run_id, command, ctx.clock.now())
            if snapshot_after:
                _snapshot(ctx, snapshot_after)
            _write_run_json(ctx, state.RESULTING_STATE[command])
        result = report.to_dict()
    ctx.close()
    exit_code = _exit_for(command, report)
    return {"ok": exit_code in (config.EXIT_OK, config.EXIT_DETECTED),
            "exit_code": exit_code, "data": result}


def _exit_for(command: str, report) -> int:
    if command == "detect" and report.status == "incident_detected":
        return config.EXIT_DETECTED
    if command == "verify":
        return config.EXIT_OK if report.status == "PASS" else config.EXIT_VERIFY_FAILED
    if command == "recover" and report.status != "recovered":
        return config.EXIT_VERIFY_FAILED
    if command in ("inject",) and report.status.endswith("failed"):
        return config.EXIT_INTERNAL
    return config.EXIT_OK


def _read_incident_id(run_dir: Path) -> str:
    data = read_json(RunPaths(run_dir).run_json)
    return data["incident_id"]


def inject(args):
    return _run_step(args, "inject", "inject")


def run(args):
    return _run_step(args, "run", "run_faulty", snapshot_after=FAULTY_SNAPSHOT)


def detect(args):
    return _run_step(args, "detect", "detect")


def evidence(args):
    return _run_step(args, "evidence", "collect_evidence")


def contain(args):
    return _run_step(args, "contain", "contain")


def repair(args):
    return _run_step(args, "repair", "repair")


def recover(args):
    return _run_step(args, "recover", "recover")


def verify(args):
    return _run_step(args, "verify", "verify")


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

def reset(args) -> dict[str, Any]:
    workspace = _workspace(args)
    run_dir = paths.run_dir_for(workspace, args.run)
    try:
        removed = safe_rmtree_run(run_dir, runs_root(workspace))
    except UnsafePathError as exc:
        raise CommandError(str(exc), config.EXIT_UNSAFE) from exc
    return {"ok": True, "exit_code": config.EXIT_OK,
            "data": {"removed_run": removed.name}}


def recover_lease(args) -> dict[str, Any]:
    """Remove a stale operation lease left by an interrupted command (§10.1)."""
    workspace = _workspace(args)
    run_dir = paths.run_dir_for(workspace, args.run)
    try:
        removed = safety.recover_lease(run_dir, runs_root(workspace))
    except UnsafePathError as exc:
        raise CommandError(str(exc), config.EXIT_UNSAFE) from exc
    return {"ok": True, "exit_code": config.EXIT_OK,
            "data": {"run": args.run, "lease_removed": removed}}


# ---------------------------------------------------------------------------
# convenience: start, reference-recovery, test-all
# ---------------------------------------------------------------------------

def start(args) -> dict[str, Any]:
    created = create(args)
    run_id = created["data"]["run_id"]
    step_args = _Args(run=run_id, workspace=getattr(args, "workspace", None))
    inject(step_args)
    run(step_args)
    detect_result = detect(step_args)
    evidence(step_args)
    return {"ok": True, "exit_code": config.EXIT_DETECTED,
            "data": {"run_id": run_id, "incident_id": created["data"]["incident_id"],
                     "detection": detect_result["data"]["fields"]}}


def reference_recovery(args) -> dict[str, Any]:
    step_args = _Args(run=args.run, workspace=getattr(args, "workspace", None))
    contain(step_args)
    repair(step_args)
    recover(step_args)
    verify_result = verify(step_args)
    return {"ok": verify_result["exit_code"] == config.EXIT_OK,
            "exit_code": verify_result["exit_code"],
            "data": {"run": args.run, "verify": verify_result["data"]["status"]}}


def test_all(args) -> dict[str, Any]:
    from .reference import reference_lifecycle
    results = []
    ok = True
    for incident_id in registry.implemented_ids():
        outcome = reference_lifecycle(incident_id, _workspace(args))
        results.append(outcome)
        ok = ok and outcome["passed"]
    return {"ok": ok, "exit_code": config.EXIT_OK if ok else config.EXIT_VERIFY_FAILED,
            "data": {"results": results}}


class _Args:
    def __init__(self, **kw):
        self.__dict__.update(kw)
