"""MP-14 — The scheduler starts a second attempt while the first is still producing.

Frozen mechanism (incident_taxonomy.md MP-14): the scheduler loses contact with a
task's external worker, declares it timed out, and starts a replacement attempt.
It cannot reliably stop the first attempt, and the replacement never checks for an
existing external job holding the target. Both attempts produce into the same
target, so it ends up with interleaved rows for two different service days.

Concurrency is modelled by a declared event queue with a fixed interleaving
(lab_architecture.md 10.1) — no real threads, processes, sleeps, or wall-clock
ordering. The guard that fixes it (a single-holder claim with a monotonic token)
is applied on the corrected replay.

Reader-facing evidence describes only the symptom: two attempts produced into one
target and it now holds rows for two service days. The frozen mechanism
vocabulary is kept out of ``evidence/``.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchC_common as cc
from . import _batchC_sim as sim
from .contract import IncidentMetadata

TARGET = "mp14_target"
INTENDED = [(v) for v in sim.INTENDED_VALUES]  # DAY1 values written by one producer

PRIVATE_TABLES = ("mp14_guard_policy", "mp14_target_staging", "mp14_attempt_event")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS mp14_guard_policy (
    policy_id             TEXT NOT NULL PRIMARY KEY,
    enforce_single_holder INTEGER NOT NULL,
    use_token             INTEGER NOT NULL,
    attempt_scoped_target INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mp14_target_staging (
    attempt_kind     TEXT NOT NULL,
    row_id           INTEGER NOT NULL,
    producer_attempt TEXT NOT NULL,
    service_date     TEXT NOT NULL,
    value            INTEGER NOT NULL,
    PRIMARY KEY (attempt_kind, row_id)
);
CREATE TABLE IF NOT EXISTS mp14_attempt_event (
    attempt_kind TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    offset_s     INTEGER NOT NULL,
    attempt_id   TEXT NOT NULL,
    event        TEXT NOT NULL,
    PRIMARY KEY (attempt_kind, seq)
);
"""

PHASE_PREFIX = {"baseline": "JOB-BL", "faulty": "JOB-FL", "recovery": "JOB-RC"}
HEALTHY_POLICY = dict(enforce_single_holder=1, use_token=1, attempt_scoped_target=1)
FAULTY_POLICY = dict(enforce_single_holder=0, use_token=0, attempt_scoped_target=0)


class MP14Incident:
    metadata = IncidentMetadata(
        incident_id="MP-14",
        family="C. Execution / dependency / access",
        reader_title="The scheduler starts a second attempt while the first is still producing output",
        primary_invariant_id="MP-14-SINGLE-COMMITTER",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("set_guard_policy:no_single_holder",),
        expected_detection_ids=("MP-14-STAGE-001", "MP-14-COMMITTERS-001"),
        transfer_mappings=("Orchestrator heartbeat/on-kill contracts and external job ids",),
        nearest_old_scenario="Old S5 produced duplicates from a retried load.",
        old_scenario_difference=(
            "Not duplicate rows from one retried load; two independent attempts produce "
            "into one target at the same time because the scheduler could not stop the "
            "first and the second never checked for an existing job."
        ),
        affected_tables=PRIVATE_TABLES,
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        ctx.control.executescript(CREATE_SQL)
        ctx.control.commit()
        self._set_policy(ctx, HEALTHY_POLICY)
        self._persist_run(ctx, "baseline", sim.CLEAN_INTERLEAVING, enforce=True)

    def baseline_ok(self, ctx: RunContext) -> bool:
        s = self._staging_summary(ctx, "baseline")
        return (s["producers"] == 1 and s["dates"] == 1
                and s["values"] == sorted(INTENDED))

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        from ..checks import integrity
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"no_single_holder_guard"})
        ctx.save_fault_profile(profile)
        self._set_policy(ctx, FAULTY_POLICY)

        policy = self._policy(ctx)
        post_faulty = policy["enforce_single_holder"] == 0
        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-14-INJECT-PRE", "precondition",
                      "clean baseline commits one producer's rows for one service day",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-14-INJECT-POST", "postcondition",
                      "the single-holder guard on the target is disabled",
                      expected=True, actual=post_faulty, status="PASS" if post_faulty else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-14-INJECT-FIXTURE"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected", fields={"policy": "no_single_holder_guard"})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-14", ctx.run_id, status,
                            fields={"fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before),
                                    "fixture_detail": fx_detail},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._persist_run(ctx, "faulty", sim.FAULTY_INTERLEAVING, enforce=False)
        s = self._staging_summary(ctx, "faulty")
        committers = self._committers(ctx, "faulty")
        ctx.log.emit(logical_time=ctx.clock.tick(), component="task",
                     event_type="target_produced",
                     fields={"rows": s["rows"], "distinct_attempts": s["producers"],
                             "distinct_service_days": s["dates"], "committed_attempts": committers})
        return StepReport("run", "MP-14", ctx.run_id, "executed",
                          fields={"rows": s["rows"], "distinct_attempts": s["producers"]})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        phase = self._effective_phase(ctx)
        s = self._staging_summary(ctx, phase)
        committers = self._committers(ctx, phase)
        policy = self._policy(ctx)

        data_bad = s["producers"] > 1 or s["dates"] > 1
        control_bad = committers > 1 or policy["enforce_single_holder"] == 0
        detected = data_bad or control_bad

        cc.write_quality_result(
            ctx, "MP-14-STAGE-001", "uniqueness",
            expected="the target holds rows from one attempt for one service day",
            actual=f"{s['rows']} rows from {s['producers']} attempt(s) across "
                   f"{s['dates']} service day(s)",
            status="FAIL" if data_bad else "PASS")
        cc.write_quality_result(
            ctx, "MP-14-COMMITTERS-001", "control_consistency",
            expected="at most one attempt can commit to the target",
            actual=f"{committers} attempt(s) committed to the target; single-holder guard "
                   f"{'off' if policy['enforce_single_holder'] == 0 else 'on'}",
            status="FAIL" if control_bad else "PASS")

        assertions = [
            Assertion("MP-14-DET-STAGE", "uniqueness",
                      "the target holds one attempt's rows for one service day",
                      expected=1, actual=max(s["producers"], s["dates"]),
                      status="FAIL" if data_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-14-DET-WRITERS", "control_consistency",
                      "at most one attempt can commit to the target",
                      expected=1, actual=committers,
                      status="FAIL" if control_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "phase": phase, "rows": s["rows"],
                  "distinct_attempts": s["producers"], "distinct_service_days": s["dates"],
                  "committed_attempts": committers}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-14", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-14", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        phase = self._effective_phase(ctx)

        staging = db.query(ctx.control,
            "SELECT row_id, producer_attempt, service_date, value FROM mp14_target_staging "
            "WHERE attempt_kind = ? ORDER BY row_id", (phase,))
        cc.write_csv(tables / "target_rows.csv", [dict(r) for r in staging],
                     ["row_id", "producer_attempt", "service_date", "value"])

        events = db.query(ctx.control,
            "SELECT offset_s, attempt_id, event FROM mp14_attempt_event "
            "WHERE attempt_kind = ? ORDER BY seq", (phase,))
        cc.write_csv(tables / "attempt_timeline.csv", [dict(r) for r in events],
                     ["offset_s", "attempt_id", "event"])

        jobs = db.query(ctx.control,
            "SELECT external_job_id, state, writer_attempt FROM external_job "
            "WHERE external_job_id LIKE ? ORDER BY external_job_id",
            (f"{PHASE_PREFIX[phase]}-%",))
        cc.write_csv(tables / "attempt_status.csv",
                     [{"attempt_id": r["external_job_id"].split("-")[-1],
                       "state": r["state"], "sequence": r["writer_attempt"]} for r in jobs],
                     ["attempt_id", "state", "sequence"])
        cc.write_standard_tables(ctx)

        s = self._staging_summary(ctx, phase)
        alert = {
            "incident_id": "MP-14", "title": self.metadata.reader_title,
            "reported_symptom": (
                "The scheduler decided the first attempt had stopped and started a second "
                "one, but the first attempt kept producing rows into the same target. The "
                "target now holds rows from two attempts for two different service days."),
            "time_pressure": "The target must not be published while it mixes two attempts.",
            "affected_product": "target staging",
            "observed": {"rows": s["rows"], "distinct_attempts": s["producers"],
                         "distinct_service_days": s["dates"]},
            "fictional_notice": cc.FICTIONAL_NOTICE,
        }
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json", cc.sanitized_timeline(ctx))
        index = cc.write_evidence_index(ctx, "MP-14")
        return StepReport("evidence", "MP-14", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        s = self._staging_summary(ctx, "faulty")
        blast = {"affected_product": "target staging", "rows": s["rows"],
                 "distinct_attempts": s["producers"],
                 "action": "publish stopped; target held; evidence preserved",
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-14", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-14", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        self._set_policy(ctx, HEALTHY_POLICY)
        with db.transaction(ctx.control):
            # Stop and reclaim the still-running first attempt(s) by persistent id.
            ctx.control.execute(
                "UPDATE external_job SET state = 'CANCELLED' "
                "WHERE external_job_id LIKE ? AND state = 'COMMITTED'",
                (f"{PHASE_PREFIX['faulty']}-%",))
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", [])
                                        if t != "no_single_holder_guard"]
        profile.setdefault("params", {})["guard"] = "single_holder_with_token"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"guard": "single_holder_with_token"})
        report = StepReport("repair", "MP-14", ctx.run_id, "repaired",
                            fields={"change": "the earlier attempts are stopped by persistent id; "
                                    "the target uses attempt-scoped staging and a single-holder "
                                    "claim with a monotonic token"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (REPLAY to the same final data, single committer) ------------
    def recover(self, ctx: RunContext) -> StepReport:
        self._persist_run(ctx, "recovery", sim.CLEAN_INTERLEAVING, enforce=True)
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM simulated_outbox WHERE message_id = 'MSG-mp14-target'")
            ctx.control.execute(
                "INSERT OR REPLACE INTO simulated_outbox "
                "(message_id, topic, payload_json, logical_time) VALUES (?, ?, ?, ?)",
                ("MSG-mp14-target", "target_committed",
                 f'{{"target": "{TARGET}", "service_date": "{sim.DAY1}"}}',
                 ctx.clock.at_offset(0)))
        s = self._staging_summary(ctx, "recovery")
        manifest = {
            "mode": "REPLAY",
            "scope": {"target": TARGET, "service_date": sim.DAY1},
            "write_mode": "bounded_replace",
            "duplicate_protection": "single-holder claim + one idempotent commit message",
            "rows": s["rows"], "distinct_attempts": s["producers"],
            "committed_attempts": self._committers(ctx, "recovery"),
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"rows": s["rows"]})
        return StepReport("recover", "MP-14", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        s = self._staging_summary(ctx, "recovery")
        committers = self._committers(ctx, "recovery")
        policy = self._policy(ctx)
        orphans_active = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM external_job WHERE external_job_id LIKE ? "
            "AND state NOT IN ('CANCELLED', 'REFUSED')", (f"{PHASE_PREFIX['faulty']}-%",))
        outbox = [r["message_id"] for r in db.query(ctx.control,
            "SELECT message_id FROM simulated_outbox WHERE message_id = 'MSG-mp14-target'")]
        present = s["rows"] > 0

        assertions = [
            Assertion("MP-14-VER-CORRECT", "correctness",
                      "the replay commits exactly the intended rows for one service day",
                      expected=sorted(INTENDED), actual=s["values"],
                      status="PASS" if present and s["values"] == sorted(INTENDED)
                             and s["dates"] == 1 else "FAIL",
                      evidence="reports/recovery.json"),
            Assertion("MP-14-VER-UNIQUE", "uniqueness",
                      "the target holds one attempt's rows for one service day",
                      expected=1, actual=max(s["producers"], s["dates"]) if present else 0,
                      status="PASS" if present and s["producers"] == 1 and s["dates"] == 1 else "FAIL"),
            Assertion("MP-14-VER-CONTROL", "control_consistency",
                      "one attempt committed, the earlier attempts are stopped, guard is on",
                      expected="1 committer / 0 still-active / guard on",
                      actual=f"{committers} committer / {orphans_active} still-active / "
                             f"{'on' if policy['enforce_single_holder'] else 'off'}",
                      status="PASS" if committers == 1 and orphans_active == 0
                             and policy["enforce_single_holder"] == 1 else "FAIL"),
            Assertion("MP-14-VER-SIDEEFFECT", "side_effects",
                      "exactly one idempotent commit message for the target",
                      expected=1, actual=len(outbox),
                      status="PASS" if len(outbox) == 1 else "FAIL"),
            Assertion("MP-14-VER-IDEMPOTENT", "idempotency",
                      "no duplicate commit message id",
                      expected=len(outbox), actual=len(set(outbox)),
                      status="PASS" if len(outbox) == len(set(outbox)) else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-14-VER-FIXTURE"),
        ]
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-14", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _persist_run(self, ctx, phase, steps, enforce) -> None:
        result = sim.run_writer_queue(steps, enforce_guard=enforce)
        prefix = PHASE_PREFIX[phase]
        staging_rows = [(phase, r["row_id"], r["producer_attempt"], r["service_date"], r["value"])
                        for r in result["rows"]]
        event_rows = [(phase, i + 1, e["offset_s"], e["attempt_id"], e["event"])
                      for i, e in enumerate(result["timeline"])]
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp14_target_staging WHERE attempt_kind = ?", (phase,))
            db.insert_rows(ctx.control, "mp14_target_staging",
                           ("attempt_kind", "row_id", "producer_attempt", "service_date", "value"),
                           staging_rows)
            ctx.control.execute("DELETE FROM mp14_attempt_event WHERE attempt_kind = ?", (phase,))
            db.insert_rows(ctx.control, "mp14_attempt_event",
                           ("attempt_kind", "seq", "offset_s", "attempt_id", "event"), event_rows)
            ctx.control.execute("DELETE FROM external_job WHERE external_job_id LIKE ?",
                                (f"{prefix}-%",))
            for attempt_id, j in sorted(result["jobs"].items()):
                state = {"STILL_ACTIVE": "ACTIVE"}.get(j["state"], j["state"])
                token = j["fence"] if enforce and state == "COMMITTED" else None
                ctx.control.execute(
                    "INSERT OR REPLACE INTO external_job "
                    "(external_job_id, target_name, state, heartbeat_seq, writer_attempt, fencing_token) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (f"{prefix}-{attempt_id}", TARGET, state, j["rows_written"], j["fence"], token))
            # The single-holder claim: only used on a guarded run.
            holder = next((aid for aid, j in result["jobs"].items()
                           if j["state"] == "COMMITTED"), None) if enforce else None
            ctx.control.execute(
                "INSERT OR REPLACE INTO writer_lease (target_name, current_writer, fence) VALUES (?, ?, ?)",
                (TARGET, holder, result["fence"] if enforce else 0))

    def _staging_summary(self, ctx, phase) -> dict[str, Any]:
        rows = db.query(ctx.control,
            "SELECT producer_attempt, service_date, value FROM mp14_target_staging "
            "WHERE attempt_kind = ? ORDER BY row_id", (phase,))
        return {"rows": len(rows),
                "producers": len({r["producer_attempt"] for r in rows}),
                "dates": len({r["service_date"] for r in rows}),
                "values": sorted(r["value"] for r in rows)}

    def _committers(self, ctx, phase) -> int:
        return db.scalar(ctx.control,
            "SELECT COUNT(*) FROM external_job WHERE external_job_id LIKE ? AND state = 'COMMITTED'",
            (f"{PHASE_PREFIX[phase]}-%",))

    def _effective_phase(self, ctx) -> str:
        has_recovery = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM mp14_target_staging WHERE attempt_kind = 'recovery'")
        return "recovery" if has_recovery else "faulty"

    def _set_policy(self, ctx, p) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp14_guard_policy "
                "(policy_id, enforce_single_holder, use_token, attempt_scoped_target) "
                "VALUES ('current', ?, ?, ?)",
                (p["enforce_single_holder"], p["use_token"], p["attempt_scoped_target"]))

    def _policy(self, ctx) -> dict[str, Any]:
        row = db.query_one(ctx.control, "SELECT * FROM mp14_guard_policy WHERE policy_id = 'current'")
        return dict(row) if row is not None else {}


INCIDENT = MP14Incident()
