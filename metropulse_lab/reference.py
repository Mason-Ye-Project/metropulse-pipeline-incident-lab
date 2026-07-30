"""Reference-lifecycle contract driver (lab_architecture.md section 13.2).

For an incident it runs the full lifecycle and asserts the invariants that make
an incident publishable: the clean baseline verifies, the fault is detected,
verify fails before repair, evidence collection does not mutate, recovery is
bounded and idempotent, fixtures are untouched, and reset removes only the run.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import commands, config, paths
from .checks.snapshots import snapshot_tables, domain_snapshot, snapshot_all_tables
from .context import RunContext
from .incidents import registry
from .paths import RunPaths


def _ctx(workspace: Path, run_id: str) -> RunContext:
    run_dir = paths.run_dir_for(workspace, run_id)
    incident_id = commands._read_incident_id(run_dir)
    return RunContext(run_id, incident_id, run_dir, workspace)


def reference_lifecycle(incident_id: str, workspace: Path) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def record(name: str, ok: bool, detail: Any = None) -> None:
        checks.append({"check": name, "passed": bool(ok), "detail": detail})

    incident = registry.load(incident_id)
    args = commands._Args(incident=incident_id, workspace=str(workspace))

    # 1. create baseline; clean baseline reconciles.
    created = commands.create(args)
    run_id = created["data"]["run_id"]
    step = commands._Args(run=run_id, workspace=str(workspace))

    ctx = _ctx(workspace, run_id)
    baseline_clean = incident.baseline_ok(ctx)
    baseline_domain = domain_snapshot(ctx.warehouse, commands.SNAPSHOT_TABLES)
    ctx.close()
    record("clean_baseline_reconciles", baseline_clean)

    # 2. inject + faulty run; detect finds the incident.
    commands.inject(step)
    commands.run(step)
    det = commands.detect(step)
    record("fault_detected", det["exit_code"] == config.EXIT_DETECTED,
           det["data"]["fields"])

    # 3. unrelated invariants remain true: the fault is confined to the ridership
    #    lineage; every other table's domain content is unchanged from baseline.
    ctx = _ctx(workspace, run_id)
    faulty_domain = domain_snapshot(ctx.warehouse, commands.SNAPSHOT_TABLES)
    affected = set(incident.metadata.affected_tables)
    unrelated_changed = [t for t in commands.SNAPSHOT_TABLES
                         if t not in affected and baseline_domain[t] != faulty_domain[t]]
    record("unrelated_tables_unchanged_by_fault", not unrelated_changed, unrelated_changed)

    # 4. verify BEFORE repair fails (an accidentally harmless injection cannot pass).
    pre_repair = incident.verify(ctx)
    record("verify_before_repair_fails", pre_repair.status == "FAIL")

    # 5. evidence collected twice does not mutate warehouse/control.
    ctx.close()
    commands.evidence(step)
    ctx = _ctx(workspace, run_id)
    before = snapshot_tables(ctx.warehouse, commands.SNAPSHOT_TABLES)
    before_ctrl = snapshot_tables(ctx.control, ("quality_result", "dataset_version"))
    ctx.close()
    commands.evidence(step)
    ctx = _ctx(workspace, run_id)
    after = snapshot_tables(ctx.warehouse, commands.SNAPSHOT_TABLES)
    after_ctrl = snapshot_tables(ctx.control, ("quality_result", "dataset_version"))
    ctx.close()
    record("evidence_is_read_only", before == after and before_ctrl == after_ctrl)

    # 6. contain + repair + recover. Snapshot EVERY table in both DBs right after
    #    the first recover (incident-agnostic; covers control-plane and
    #    incident-private tables). Capturing at state RECOVERED — not after verify —
    #    excludes lifecycle bookkeeping (lifecycle_state) from the comparison.
    commands.contain(step)
    commands.repair(step)
    commands.recover(step)
    ctx = _ctx(workspace, run_id)
    recover_snap_1 = (snapshot_all_tables(ctx.warehouse), snapshot_all_tables(ctx.control))
    ctx.close()
    v1 = commands.verify(step)
    record("verify_after_recovery_passes", v1["exit_code"] == config.EXIT_OK,
           v1["data"]["fields"])

    # 7. recover a second time (also lands at RECOVERED); zero additional logical
    #    change; verify PASS.
    commands.recover(step)
    ctx = _ctx(workspace, run_id)
    recover_snap_2 = (snapshot_all_tables(ctx.warehouse), snapshot_all_tables(ctx.control))
    ctx.close()
    record("second_recovery_is_idempotent", recover_snap_1 == recover_snap_2)
    v2 = commands.verify(step)
    record("verify_after_second_recovery_passes", v2["exit_code"] == config.EXIT_OK)

    # 8. reset removes only the selected run.
    workspace_runs = paths.runs_root(workspace)
    other_runs_before = {p.name for p in workspace_runs.iterdir() if p.is_dir()} - {run_id}
    commands.reset(commands._Args(run=run_id, workspace=str(workspace)))
    still_exists = (workspace_runs / run_id).exists()
    other_runs_after = {p.name for p in workspace_runs.iterdir() if p.is_dir()} if workspace_runs.exists() else set()
    record("reset_removed_only_selected_run",
           (not still_exists) and other_runs_before <= other_runs_after)

    passed = all(c["passed"] for c in checks)
    return {"incident_id": incident_id, "passed": passed, "checks": checks}
