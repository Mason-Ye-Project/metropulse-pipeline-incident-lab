"""MP-07 — A manual rerun succeeds but refreshes the wrong service day.

Frozen mechanism (incident_taxonomy.md MP-07): the job mixes the requested
logical service date, the data window, and a frozen wall-clock instant. A manual
re-trigger's resolved window differs from the engineer's assumption, so the run
that logs the requested date actually rewrites the partition for the day before.

Scheduled and manual runs must share one explicit, validated source/target window
contract, and a dry run must print the concrete read/write boundaries before any
data moves. Reader-facing text describes only the symptom: the log shows one
service day, the refreshed output is a different one.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchB_common as bc
from . import _batchB_mp07_scheduler as sched
from .contract import IncidentMetadata

DATASET = "mp07_route_day_partitions"
OUTBOX_ID = "MSG-mp07-recovered"
MANUAL_RUN_ID = "SR-mp07-manual-0001"
RECOVER_RUN_ID = "SR-mp07-recover-0001"
LEAK = ("logical date", "wall clock", "wall-clock", "now()", "interval", "timetable", "catchup")


class MP07Incident:
    metadata = IncidentMetadata(
        incident_id="MP-07",
        family="B. Missing or late",
        reader_title="A manual rerun succeeds but refreshes the wrong service day",
        primary_invariant_id="MP-07-WINDOW-CONTRACT",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("enable_faulty_window_resolution:mp07",),
        expected_detection_ids=("MP-07-TARGET-001", "MP-07-CONTROL-001"),
        transfer_mappings=("Airflow DAG run, data interval and timetable behavior",),
        nearest_old_scenario="Adjacent to generic scheduling discussions.",
        old_scenario_difference=(
            "Not object-store eventual consistency and not a directory naming typo: a "
            "manual re-trigger resolves a different window than assumed and rewrites the "
            "wrong service-day partition while logging the requested one."
        ),
        affected_tables=("mp07_partition_output",),
    )

    # -- schema / baseline ----------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp07_partition_output ("
                "service_date TEXT NOT NULL PRIMARY KEY, content_token TEXT NOT NULL, "
                "written_by_run TEXT NOT NULL)")
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp07_run_ledger ("
                "scheduler_run_id TEXT NOT NULL PRIMARY KEY, run_kind TEXT NOT NULL, "
                "requested_service_date TEXT NOT NULL, window_start TEXT NOT NULL, "
                "window_end TEXT NOT NULL, read_path TEXT NOT NULL, write_path TEXT NOT NULL, "
                "write_partition TEXT NOT NULL, validated INTEGER NOT NULL)")

    def _record_run(self, ctx: RunContext, run_id: str, run_kind: str,
                    r: "sched.ResolvedRun", reason: str) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "INSERT OR REPLACE INTO mp07_run_ledger (scheduler_run_id, run_kind, "
                "requested_service_date, window_start, window_end, read_path, write_path, "
                "write_partition, validated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, run_kind, r.requested_service_date, r.window_start, r.window_end,
                 r.read_path, r.write_path, r.write_partition, int(r.validated)))
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO scheduler_run (scheduler_run_id, logical_run_date, "
                "creation_reason, state) VALUES (?, ?, ?, 'SUCCEEDED')",
                (run_id, r.requested_service_date, reason))

    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp07_partition_output")
            for p in sched.PARTITIONS:
                ctx.warehouse.execute(
                    "INSERT INTO mp07_partition_output (service_date, content_token, written_by_run) "
                    "VALUES (?, ?, 'SCHED')", (p, sched.scheduled_content(p)))
        for p in sched.PARTITIONS:
            r = sched.resolve_manual_run(p, faulty=False)  # a validated scheduled window per day
            self._record_run(ctx, f"SR-mp07-sched-{p}", "scheduled", r, "scheduled")
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-baseline", DATASET, "route-day",
                                  "PUBLISHED", "PUB-baseline")
        ctx.log.emit(logical_time=ctx.clock.now(), component="publish",
                     event_type="partitions_published",
                     fields={"partitions": list(sched.PARTITIONS)})

    def baseline_ok(self, ctx: RunContext) -> bool:
        rows = db.query(ctx.warehouse,
                        "SELECT service_date, content_token FROM mp07_partition_output")
        return all(r["content_token"] == sched.scheduled_content(r["service_date"]) for r in rows) \
            and len(rows) == len(sched.PARTITIONS)

    # -- faulty manual rerun --------------------------------------------------
    def _apply_faulty(self, ctx: RunContext) -> "sched.ResolvedRun":
        r = sched.resolve_manual_run(sched.REQUESTED_SERVICE_DATE, faulty=True)
        # The manual fix content is written to the (wrong) resolved partition.
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "UPDATE mp07_partition_output SET content_token = ?, written_by_run = 'MANUAL-FIX' "
                "WHERE service_date = ?",
                (sched.fix_content(sched.REQUESTED_SERVICE_DATE), r.write_partition))
        self._record_run(ctx, MANUAL_RUN_ID, "manual", r, "manual")
        return r

    def inject(self, ctx: RunContext) -> StepReport:
        req = sched.REQUESTED_SERVICE_DATE
        before = db.scalar(ctx.warehouse,
                           "SELECT content_token FROM mp07_partition_output WHERE service_date = ?",
                           (req,))
        pre_ok = (before == sched.scheduled_content(req))
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp07_faulty_window"] = True
        ctx.save_fault_profile(profile)

        r = self._apply_faulty(ctx)
        refreshed = r.write_partition
        req_after = db.scalar(ctx.warehouse,
                              "SELECT content_token FROM mp07_partition_output WHERE service_date = ?",
                              (req,))
        # Postcondition: the manual fix landed on a different day, and the requested
        # day still holds its pre-fix (scheduled) content.
        post_ok = (refreshed != req) and (req_after == sched.scheduled_content(req))

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, _ = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-07-INJECT-PRE", "precondition",
                      "requested service day starts from its scheduled content",
                      expected=sched.scheduled_content(req), actual=before,
                      status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-07-INJECT-POST", "postcondition",
                      "manual fix refreshed a different service day than requested",
                      expected=f"!= {req}", actual=refreshed,
                      status="PASS" if post_ok else "FAIL"),
            Assertion("MP-07-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.at_offset(60), component="incident",
                     event_type="fault_injected",
                     fields={"requested": req, "refreshed": refreshed})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-07", ctx.run_id, status,
                            fields={"requested_service_date": req, "refreshed_service_date": refreshed,
                                    "fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        r = self._apply_faulty(ctx)
        return StepReport("run", "MP-07", ctx.run_id, "executed",
                          fields={"requested_service_date": r.requested_service_date,
                                  "refreshed_service_date": r.write_partition})

    # -- detect ---------------------------------------------------------------
    def _signals(self, ctx: RunContext) -> dict[str, Any]:
        req = sched.REQUESTED_SERVICE_DATE
        manual = db.query_one(ctx.warehouse,
                              "SELECT requested_service_date, write_partition, write_path "
                              "FROM mp07_run_ledger WHERE scheduler_run_id = ?", (MANUAL_RUN_ID,))
        req_content = db.scalar(ctx.warehouse,
                                "SELECT content_token FROM mp07_partition_output WHERE service_date = ?",
                                (req,))
        requested_day_refreshed = (req_content == sched.fix_content(req))
        write_partition = manual["write_partition"] if manual else None
        control_gap = bool(manual and manual["write_partition"] != manual["requested_service_date"])
        return {"requested_service_date": req, "refreshed_service_date": write_partition,
                "requested_day_refreshed": requested_day_refreshed,
                "write_path": manual["write_path"] if manual else None,
                "data_gap": not requested_day_refreshed, "control_gap": control_gap}

    def detect(self, ctx: RunContext) -> StepReport:
        s = self._signals(ctx)
        bc.write_quality_result(
            ctx, "MP-07-TARGET-001", "correctness",
            expected=f"requested service day {s['requested_service_date']} is refreshed",
            actual=(f"service day {s['refreshed_service_date']} was refreshed instead"),
            status="FAIL" if s["data_gap"] else "PASS")
        bc.write_quality_result(
            ctx, "MP-07-CONTROL-001", "control_consistency",
            expected="the run's write partition equals the requested service day",
            actual=("the run's write partition is a different service day than requested"
                    if s["control_gap"] else "write partition matches the requested day"),
            status="FAIL" if s["control_gap"] else "PASS")

        assertions = [
            Assertion("MP-07-DET-TARGET", "correctness",
                      "requested service day was actually refreshed",
                      expected=True, actual=s["requested_day_refreshed"],
                      status="FAIL" if s["data_gap"] else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-07-DET-CONTROL", "control_consistency",
                      "run write partition equals requested service day",
                      expected=False, actual=s["control_gap"],
                      status="FAIL" if s["control_gap"] else "PASS",
                      evidence="reports/detection.json"),
        ]
        detected = s["data_gap"] or s["control_gap"]
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-07", "run_id": ctx.run_id, "fields": s,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-07", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=s, assertions=assertions)

    # -- evidence -------------------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        runs = db.query(ctx.warehouse,
                        "SELECT scheduler_run_id, run_kind, requested_service_date, "
                        "write_partition, window_start, window_end, read_path, write_path "
                        "FROM mp07_run_ledger WHERE run_kind = 'manual' "
                        "ORDER BY scheduler_run_id")
        bc.write_csv(tables / "manual_run_targets.csv",
                     [{"scheduler_run_id": r["scheduler_run_id"],
                       "requested_service_date": r["requested_service_date"],
                       "refreshed_service_date": r["write_partition"],
                       "read_window_start": r["window_start"],
                       "read_window_end": r["window_end"],
                       "read_path": r["read_path"], "write_path": r["write_path"]}
                      for r in runs],
                     ["scheduler_run_id", "requested_service_date", "refreshed_service_date",
                      "read_window_start", "read_window_end", "read_path", "write_path"])

        parts = db.query(ctx.warehouse,
                         "SELECT service_date, content_token, written_by_run "
                         "FROM mp07_partition_output ORDER BY service_date")
        bc.write_csv(tables / "partition_contents.csv",
                     [{"service_date": p["service_date"], "content_token": p["content_token"],
                       "written_by_run": p["written_by_run"],
                       "matches_scheduled": "yes"
                       if p["content_token"] == sched.scheduled_content(p["service_date"]) else "no"}
                      for p in parts],
                     ["service_date", "content_token", "written_by_run", "matches_scheduled"])
        bc.write_csv(tables / "quality_results.csv", bc.quality_results_rows(ctx),
                     ["check_id", "scope_key", "dimension", "expected", "actual", "status"])

        s = self._signals(ctx)
        alert = bc.base_alert(
            "MP-07", self.metadata.reader_title,
            reported_symptom=(
                f"An engineer manually re-triggered a fix for service day "
                f"{s['requested_service_date']}. The run reported success and the log shows "
                f"that date, but the output that actually changed belongs to a different "
                f"service day ({s['refreshed_service_date']}). The requested day still shows "
                f"its old content."),
            time_pressure="The corrected service report is expected the same morning.",
            affected_product="route_day_partitions",
            extra={"observed": {"requested_service_date": s["requested_service_date"],
                                "refreshed_service_date": s["refreshed_service_date"]}})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", bc.sanitized_timeline(ctx, LEAK))
        write_json(ctx.paths.evidence_index, bc.evidence_index("MP-07", ctx.run_id, ev))
        return StepReport("evidence", "MP-07", ctx.run_id, "collected",
                          fields={"refreshed_service_date": s["refreshed_service_date"]})

    # -- contain / repair / recover ------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        bc.set_dataset_state(ctx, DATASET, "HELD", from_state="PUBLISHED")
        s = self._signals(ctx)
        blast = {"held_dataset": DATASET,
                 "affected_partitions": sorted({s["requested_service_date"],
                                                s["refreshed_service_date"]}),
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.now(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-07", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-07", ctx.run_id, "contained", fields=blast)

    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp07_faulty_window"] = False
        profile["params"]["mp07_window_contract"] = "explicit_validated_shared_window"
        ctx.save_fault_profile(profile)
        report = StepReport("repair", "MP-07", ctx.run_id, "repaired",
                            fields={"change": "scheduled and manual runs share one explicit, "
                                             "validated source/target window; a dry run prints "
                                             "the read/write boundaries before writing"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    def recover(self, ctx: RunContext) -> StepReport:
        # REPLAY the correct validated window: the manual fix lands on the requested
        # day, and the day the faulty run clobbered is restored to its scheduled content.
        correct = sched.correct_final_partitions()
        with db.transaction(ctx.warehouse):
            for service_date, token in sorted(correct.items()):
                written_by = "MANUAL-FIX" if token == sched.fix_content(service_date) else "SCHED"
                ctx.warehouse.execute(
                    "INSERT OR REPLACE INTO mp07_partition_output "
                    "(service_date, content_token, written_by_run) VALUES (?, ?, ?)",
                    (service_date, token, written_by))
        r = sched.resolve_manual_run(sched.REQUESTED_SERVICE_DATE, faulty=False)
        self._record_run(ctx, RECOVER_RUN_ID, "recovery", r, "recovery")
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-recover", DATASET, "route-day",
                                  "PUBLISHED", "PUB-recover")
        bc.record_outbox(ctx, OUTBOX_ID, "mp07.recovered",
                         {"dataset": DATASET, "requested_service_date": r.requested_service_date})
        manifest = {
            "mode": "REPLAY",
            "scope": {"partitions": sorted(correct.keys()), "datasets": ["mp07_partition_output"]},
            "source": "explicit validated source/target window for the requested service day",
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key on service_date; deterministic rewrite",
            "validation": "run write partition equals requested service day",
            "requested_service_date": r.requested_service_date,
            "write_partition": r.write_partition,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.now(), component="recovery",
                     event_type="replay_completed",
                     fields={"requested_service_date": r.requested_service_date})
        return StepReport("recover", "MP-07", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        correct = sched.correct_final_partitions()
        rows = db.query(ctx.warehouse,
                        "SELECT service_date, content_token FROM mp07_partition_output "
                        "ORDER BY service_date")
        persisted = {r["service_date"]: r["content_token"] for r in rows}
        req = sched.REQUESTED_SERVICE_DATE
        rec = db.query_one(ctx.warehouse,
                           "SELECT requested_service_date, write_partition, validated "
                           "FROM mp07_run_ledger WHERE scheduler_run_id = ?", (RECOVER_RUN_ID,))
        control_ok = bool(rec and rec["validated"] == 1
                          and rec["write_partition"] == rec["requested_service_date"] == req)
        dup = db.scalar(ctx.warehouse,
                        "SELECT COUNT(*) - COUNT(DISTINCT service_date) FROM mp07_partition_output")
        changed = bc.shared_tables_changed(ctx)

        assertions = [
            Assertion("MP-07-VER-CORRECT", "correctness",
                      "each partition holds its correct content after the targeted fix",
                      expected=correct, actual=persisted,
                      status="PASS" if persisted == correct else "FAIL",
                      evidence="reports/verification.json"),
            Assertion("MP-07-VER-COMPLETE", "completeness",
                      "all service-day partitions present",
                      expected=len(sched.PARTITIONS), actual=len(persisted),
                      status="PASS" if len(persisted) == len(sched.PARTITIONS) else "FAIL"),
            Assertion("MP-07-VER-UNIQUE", "uniqueness",
                      "no duplicate service-day partition", expected=0, actual=dup,
                      status="PASS" if dup == 0 else "FAIL"),
            Assertion("MP-07-VER-CONTROL", "control_consistency",
                      "recovery run's write partition equals the requested service day",
                      expected=True, actual=control_ok,
                      status="PASS" if control_ok else "FAIL"),
            Assertion("MP-07-VER-BOUNDED", "boundedness",
                      "no shared warehouse table changed since the faulty run",
                      expected=[], actual=changed, status="PASS" if not changed else "FAIL"),
            Assertion("MP-07-VER-IDEMPOTENT", "idempotency",
                      "independent recompute equals persisted partitions",
                      expected=True, actual=(persisted == correct),
                      status="PASS" if persisted == correct else "FAIL"),
            Assertion("MP-07-VER-SIDEEFFECT", "side_effects",
                      "exactly one recovery notification emitted",
                      expected=1, actual=bc.outbox_count(ctx, OUTBOX_ID),
                      status="PASS" if bc.outbox_count(ctx, OUTBOX_ID) == 1 else "FAIL"),
        ]
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion("MP-07-VER-FIXTURE", "fixture_integrity",
                                    "run landing inputs match checked-in fixtures",
                                    expected=True, actual=fx_ok,
                                    status="PASS" if fx_ok else "FAIL",
                                    evidence=str(fx_detail.get("status"))))
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-07", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP07Incident()
