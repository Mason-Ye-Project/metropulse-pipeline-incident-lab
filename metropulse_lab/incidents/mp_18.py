"""MP-18 — The scheduler's metadata store and queue are swamped before any work.

Frozen mechanism (incident_taxonomy.md MP-18): the data grain was modeled
directly as the orchestration work-item grain. Projecting one work-item per
station-per-minute-per-metric for a single service date yields tens of thousands
of tiny work-items, which swamp the scheduler's metadata store and queue before
any data is processed.

The reference fix admits a work manifest only up to a hard budget, records the
rejected projected count, and processes the service date as a small number of
coarse partitions instead. The processed result is identical.

The reader-facing title and evidence describe only the symptom.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from .contract import IncidentMetadata
from . import _batchD_common as common
from ._batchD_scheduler_sim import SchedulerSimulator

FAULT = "mp18_fine_grain_manifest"
WORKITEM_BUDGET = 512
FINE_DIMS = {"stations": 12, "service_minutes": 1440, "metrics": 3}  # -> 51,840
RECOVERY_DATASET = "mart_route_rollup"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mp18_probe (
    approach        TEXT PRIMARY KEY,
    projected_units INTEGER NOT NULL,
    budget          INTEGER NOT NULL,
    over_budget     INTEGER NOT NULL,
    admitted_units  INTEGER NOT NULL,
    queue_depth     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mp18_route_rollup (
    route_id  TEXT PRIMARY KEY,
    boardings INTEGER NOT NULL
);
"""


class MP18Incident:
    metadata = IncidentMetadata(
        incident_id="MP-18",
        family="D. Performance and cost",
        reader_title="Before any data is processed, the scheduler's store and queue are swamped for one service date",
        primary_invariant_id="MP-18-WORKITEM-BUDGET",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-18-WORKITEMS-001", "MP-18-QUEUE-001"),
        transfer_mappings=(
            "Airflow dynamic work-item generation and its per-run admission limits",
        ),
        nearest_old_scenario="None; the old books have no orchestration work-item budget incident.",
        old_scenario_difference=(
            "Not a backfill contending for workers and not a slow query: the scheduler "
            "is swamped before any work runs because the data grain was projected "
            "directly onto the work-item grain."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        ctx.warehouse.executescript(_SCHEMA)

    def _partitions(self, ctx: RunContext) -> list[str]:
        return [r["route_id"] for r in db.query(ctx.warehouse,
            "SELECT DISTINCT route_id FROM fct_station_gate_count "
            "WHERE route_id IS NOT NULL ORDER BY route_id")]

    def _reference_rollup(self, ctx: RunContext) -> dict[str, int]:
        return {r["route_id"]: r["boardings"] for r in db.query(ctx.warehouse,
            "SELECT route_id, SUM(passenger_count) AS boardings FROM fct_station_gate_count "
            "WHERE route_id IS NOT NULL GROUP BY route_id")}

    def _coarse_rollup(self, ctx: RunContext) -> dict[str, int]:
        """Process the service date as one coarse partition per route."""
        out: dict[str, int] = {}
        for route in self._partitions(ctx):
            total = db.scalar(ctx.warehouse,
                "SELECT SUM(passenger_count) FROM fct_station_gate_count WHERE route_id = ?", (route,))
            out[route] = int(total or 0)
        return out

    def _record_probe(self, ctx: RunContext, approach: str, projected_units: int,
                      over_budget: bool, admitted_units: int, queue_depth: int) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "INSERT OR REPLACE INTO mp18_probe "
                "(approach, projected_units, budget, over_budget, admitted_units, queue_depth) "
                "VALUES (?,?,?,?,?,?)",
                (approach, projected_units, WORKITEM_BUDGET, int(over_budget),
                 admitted_units, queue_depth))

    def _read_probe(self, ctx: RunContext, approach: str) -> dict[str, Any] | None:
        row = db.query_one(ctx.warehouse, "SELECT * FROM mp18_probe WHERE approach = ?", (approach,))
        return dict(row) if row else None

    def baseline_ok(self, ctx: RunContext) -> bool:
        # The recoverable coarse plan admits one work-item per route: within budget.
        return len(self._partitions(ctx)) <= WORKITEM_BUDGET

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        sim = SchedulerSimulator(WORKITEM_BUDGET)
        coarse_before = len(self._partitions(ctx)) <= WORKITEM_BUDGET
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(set(profile.get("faulty_transforms", [])) | {FAULT})
        profile.setdefault("params", {})["fine_dims"] = FINE_DIMS
        ctx.save_fault_profile(profile)

        projected = sim.project_units(FINE_DIMS)
        fine = sim.admit_fine(ctx, projected, FINE_DIMS)
        self._record_probe(ctx, "fine_grain", projected, fine["over_budget"],
                           fine["admitted_units"], projected)

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fixtures_ok, fixture_detail = integrity.fixtures_untouched(ctx)
        fixtures_ok = fixtures_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-18-INJECT-PRE", "precondition",
                      "coarse plan is within the work-item budget at baseline",
                      expected=True, actual=coarse_before, status="PASS" if coarse_before else "FAIL"),
            Assertion("MP-18-INJECT-POST", "postcondition",
                      "fine-grain manifest projects above the work-item budget and is rejected",
                      expected=True, actual=fine["over_budget"],
                      status="PASS" if fine["over_budget"] else "FAIL"),
            Assertion("MP-18-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection (control-plane fault only)",
                      expected=True, actual=fixtures_ok, status="PASS" if fixtures_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected",
                     fields={"projected_units": projected, "budget": WORKITEM_BUDGET})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-18", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "projected_units": projected, "budget": WORKITEM_BUDGET,
                                    "fixture_checksums_unchanged": fixtures_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        sim = SchedulerSimulator(WORKITEM_BUDGET)
        projected = sim.project_units(FINE_DIMS)
        fine = sim.admit_fine(ctx, projected, FINE_DIMS)
        self._record_probe(ctx, "fine_grain", projected, fine["over_budget"],
                           fine["admitted_units"], projected)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="faulty_run", fields={"projected_units": projected})
        return StepReport("run", "MP-18", ctx.run_id, "executed",
                          fields={"projected_units": projected, "admitted_units": fine["admitted_units"],
                                  "budget": WORKITEM_BUDGET})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        probe = self._read_probe(ctx, "fine_grain") or {}
        projected = probe.get("projected_units", 0)
        queue_depth = probe.get("queue_depth", 0)

        units_bad = projected > WORKITEM_BUDGET
        queue_bad = queue_depth > WORKITEM_BUDGET
        detected = bool(units_bad or queue_bad)

        coarse_matches = self._coarse_rollup(ctx) == self._reference_rollup(ctx)

        common.write_quality_result(
            ctx, "MP-18-WORKITEMS-001", "valid_domain",
            expected=f"projected work-items for one service date stay within {WORKITEM_BUDGET}",
            actual=f"the fine grain projects {projected} work-items for one service date",
            status="FAIL" if units_bad else "PASS")
        common.write_quality_result(
            ctx, "MP-18-QUEUE-001", "scan_work",
            expected=f"the scheduler queue admits at most {WORKITEM_BUDGET} work-items",
            actual=f"the fine manifest would queue {queue_depth} work-items and was rejected",
            status="FAIL" if queue_bad else "PASS")

        assertions = [
            Assertion("MP-18-DET-WORKITEMS", "valid_domain",
                      "projected work-items within the per-service-date budget",
                      expected=f"<= {WORKITEM_BUDGET}", actual=projected,
                      status="FAIL" if units_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-18-DET-QUEUE", "scan_work",
                      "scheduler queue depth within the admission budget",
                      expected=f"<= {WORKITEM_BUDGET}", actual=queue_depth,
                      status="FAIL" if queue_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "projected_units": projected,
                  "budget": WORKITEM_BUDGET, "queue_depth": queue_depth,
                  "coarse_partitions": len(self._partitions(ctx)),
                  "result_identical_via_coarse_path": coarse_matches}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-18", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-18", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        probe = self._read_probe(ctx, "fine_grain") or {}
        common.write_csv(
            tables / "work_item_projection.csv",
            [{"grain": "one work-item per station-minute-metric",
              "projected_work_items": probe.get("projected_units"),
              "admission_budget": probe.get("budget"),
              "admitted_work_items": probe.get("admitted_units"),
              "queue_depth": probe.get("queue_depth"),
              "coarse_partitions": len(self._partitions(ctx))}],
            ["grain", "projected_work_items", "admission_budget", "admitted_work_items",
             "queue_depth", "coarse_partitions"])
        common.write_csv(
            tables / "scheduler_runs.csv",
            db.rows_to_dicts(db.query(ctx.control,
                "SELECT scheduler_run_id, logical_run_date, creation_reason, state "
                "FROM scheduler_run ORDER BY scheduler_run_id")),
            ["scheduler_run_id", "logical_run_date", "creation_reason", "state"])
        common.write_common_evidence_tables(ctx)

        alert = common.build_alert(
            "MP-18", self.metadata.reader_title,
            reported_symptom=(
                "For a single service date the scheduler's metadata store and its "
                "run queue filled with tens of thousands of tiny queued work-items "
                "before any data processing started. Each queued work-item, once it "
                "runs, does almost no work."),
            time_pressure="The service-date run is blocking the daily publish window.",
            affected_product="daily service-date processing",
            observed={"queued_before_processing": "tens of thousands of work-items",
                      "work_per_item": "almost none"})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx))
        index = common.write_evidence_index(ctx, "MP-18")
        return StepReport("evidence", "MP-18", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        blast_radius = {
            "held_product": RECOVERY_DATASET,
            "action": "pause fine-grain admission and hold the service-date publication",
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast_radius)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-18", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-18", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["grain_strategy"] = "coarse_partitions_per_route"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident", event_type="repaired",
                     fields={"strategy": "coarse partitions per route with a hard admission budget"})
        report = StepReport("repair", "MP-18", ctx.run_id, "repaired",
                            fields={"change": "process each service date as coarse per-route "
                                             "partitions instead of one work-item per data unit",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        pub = f"PUB-{ctx.run_id}-recover"
        sim = SchedulerSimulator(WORKITEM_BUDGET)
        partitions = self._partitions(ctx)
        coarse = sim.run_coarse(ctx, partitions)
        self._record_probe(ctx, "coarse", len(partitions), False,
                           coarse["admitted_units"], coarse["queue_depth"])

        rollup = self._coarse_rollup(ctx)
        with db.transaction(conn):
            conn.execute("DELETE FROM mp18_route_rollup")
            for route in sorted(rollup):
                conn.execute("INSERT INTO mp18_route_rollup (route_id, boardings) VALUES (?, ?)",
                             (route, rollup[route]))
        dv_id = common.record_recovery_publication(ctx, RECOVERY_DATASET, pub)

        manifest = {
            "mode": "REPLAY", "publication_id": pub, "dataset_version_id": dv_id,
            "scope": {"datasets": [RECOVERY_DATASET], "partitions": partitions},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key at route grain; deterministic coarse recompute",
            "strategy": "coarse per-route partitions within the admission budget",
            "admitted_units": coarse["admitted_units"], "budget": WORKITEM_BUDGET,
            "route_rows": len(rollup), "evidence_origin": common.EVIDENCE_ORIGIN,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"route_rows": len(rollup)})
        return StepReport("recover", "MP-18", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        assertions: list[Assertion] = []

        reference = self._reference_rollup(ctx)
        persisted = {r["route_id"]: r["boardings"] for r in db.query(conn,
            "SELECT route_id, boardings FROM mp18_route_rollup")}

        assertions.append(Assertion(
            "MP-18-VER-CORRECT", "correctness",
            "coarse-partitioned result equals an independent direct recompute",
            expected=reference, actual=persisted,
            status="PASS" if persisted == reference else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-18-VER-COMPLETE", "completeness",
            "coarse result covers every route present in the gate events",
            expected=len(reference), actual=len(persisted),
            status="PASS" if set(persisted) == set(reference) else "FAIL"))

        coarse = self._read_probe(ctx, "coarse")
        bounded_ok = bool(coarse) and coarse["admitted_units"] <= WORKITEM_BUDGET \
            and coarse["over_budget"] == 0
        assertions.append(Assertion(
            "MP-18-VER-BOUNDED", "boundedness",
            "the recovered coarse plan admits work-items within the budget",
            expected=f"<= {WORKITEM_BUDGET}", actual=(coarse or {}).get("admitted_units"),
            status="PASS" if bounded_ok else "FAIL"))

        coarse_runs = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM scheduler_run WHERE scheduler_run_id LIKE ? AND state = 'COARSE_ADMITTED'",
            (f"SR-{ctx.run_id}-coarse-%",))
        control_ok = coarse_runs >= 1 and common.has_recovery_publication(ctx, RECOVERY_DATASET)
        assertions.append(Assertion(
            "MP-18-VER-CONTROL", "control_consistency",
            "coarse scheduler runs and a bounded-recovery publication are recorded",
            expected=">=1 coarse run + recovered publication",
            actual={"coarse_runs": coarse_runs, "recovered_publication": control_ok},
            status="PASS" if control_ok else "FAIL"))

        assertions.append(Assertion(
            "MP-18-VER-IDEMPOTENT", "idempotency",
            "independent recompute equals the persisted coarse result",
            expected=True, actual=(persisted == reference and bool(persisted)),
            status="PASS" if (persisted == reference and bool(persisted)) else "FAIL"))

        integrity_ok, integrity_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-18-VER-FIXTURE", "fixture_integrity",
            "run landing inputs match checked-in fixture checksums",
            expected=True, actual=integrity_ok, status="PASS" if integrity_ok else "FAIL",
            evidence=str(integrity_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-18", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP18Incident()
