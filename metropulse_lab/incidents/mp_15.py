"""MP-15 — Every aggregate task stalls at the same step waiting for a database session.

Frozen mechanism (incident_taxonomy.md MP-15): a database session is acquired but
the error path forgets to return it, so under load the in-use count climbs to the
capacity limit and never comes back down. Later tasks then stall at the acquire
step and time out, even though the database itself is nearly idle.

The session lifecycle is a deterministic fake with acquire/release/rollback and an
injected error — no SQLAlchemy, no PostgreSQL, no real sockets. The fix is the
context-managed lifecycle that always returns the session.

Reader-facing evidence describes only the symptom: tasks time out at the same
step waiting for a database session while the database stays idle, and the number
of in-use sessions never returns to baseline. The frozen mechanism vocabulary is
kept out of ``evidence/``.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchC_common as cc
from . import _batchC_sim as sim
from .contract import IncidentMetadata

CAPACITY = 5
N_TASKS = 12
TASK_IDS = [f"AGG_{i:02d}" for i in range(1, N_TASKS + 1)]

PRIVATE_TABLES = ("mp15_session_policy", "mp15_task_result", "mp15_pool_stat")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS mp15_session_policy (
    policy_id        TEXT NOT NULL PRIMARY KEY,
    capacity         INTEGER NOT NULL,
    context_managed  INTEGER NOT NULL,
    release_on_error INTEGER NOT NULL,
    throttle         INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mp15_task_result (
    attempt_kind  TEXT NOT NULL,
    task_index    INTEGER NOT NULL,
    task_id       TEXT NOT NULL,
    outcome       TEXT NOT NULL,
    acquire_step  INTEGER,
    session_state TEXT NOT NULL,
    PRIMARY KEY (attempt_kind, task_index)
);
CREATE TABLE IF NOT EXISTS mp15_pool_stat (
    attempt_kind TEXT NOT NULL PRIMARY KEY,
    capacity     INTEGER NOT NULL,
    max_in_use   INTEGER NOT NULL,
    final_in_use INTEGER NOT NULL,
    timeouts     INTEGER NOT NULL,
    completed    INTEGER NOT NULL
);
"""

HEALTHY_POLICY = dict(capacity=CAPACITY, context_managed=1, release_on_error=1, throttle=CAPACITY)
FAULTY_POLICY = dict(capacity=CAPACITY, context_managed=0, release_on_error=0, throttle=-1)
_SESSION_STATE = {"IN_USE": "HELD_OPEN", "RELEASED": "RETURNED",
                  "ROLLED_BACK": "ROLLED_BACK", "ENDED_BY_RECOVERY": "ENDED"}


class MP15Incident:
    metadata = IncidentMetadata(
        incident_id="MP-15",
        family="C. Execution / dependency / access",
        reader_title="Every aggregate task stalls at the same step waiting for a database session",
        primary_invariant_id="MP-15-SESSION-RETURN",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("set_session_policy:skip_release_on_error",),
        expected_detection_ids=("MP-15-STALL-001", "MP-15-SESSIONS-001"),
        transfer_mappings=("SQLAlchemy QueuePool timeouts and pg_stat_activity",),
        nearest_old_scenario="The old books had no session-capacity incident.",
        old_scenario_difference=(
            "The database is nearly idle. Tasks stall because the error path never "
            "returns its session, so the in-use count climbs to the capacity limit and "
            "stays there — not slow queries or missing indexes."
        ),
        affected_tables=PRIVATE_TABLES,
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        ctx.control.executescript(CREATE_SQL)
        ctx.control.commit()
        self._set_policy(ctx, HEALTHY_POLICY)
        self._run_phase(ctx, "baseline", HEALTHY_POLICY, raise_all=False, persist_sessions=False)

    def baseline_ok(self, ctx: RunContext) -> bool:
        s = self._stat(ctx, "baseline")
        return bool(s) and s["final_in_use"] == 0 and s["timeouts"] == 0 and s["completed"] == N_TASKS

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        from ..checks import integrity
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"skip_release_on_error"})
        ctx.save_fault_profile(profile)
        self._set_policy(ctx, FAULTY_POLICY)

        policy = self._policy(ctx)
        post_faulty = policy["release_on_error"] == 0 and policy["context_managed"] == 0
        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-15-INJECT-PRE", "precondition",
                      "clean baseline returns every session and completes every task",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-15-INJECT-POST", "postcondition",
                      "the error path no longer returns the session",
                      expected=True, actual=post_faulty, status="PASS" if post_faulty else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-15-INJECT-FIXTURE"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected", fields={"policy": "skip_release_on_error"})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-15", ctx.run_id, status,
                            fields={"fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before),
                                    "fixture_detail": fx_detail},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._run_phase(ctx, "faulty", self._policy(ctx), raise_all=True, persist_sessions=True)
        s = self._stat(ctx, "faulty")
        ctx.log.emit(logical_time=ctx.clock.tick(), component="task",
                     event_type="aggregate_pass_finished",
                     fields={"completed": s["completed"], "timed_out": s["timeouts"],
                             "in_use_at_end": s["final_in_use"]})
        return StepReport("run", "MP-15", ctx.run_id, "executed",
                          fields={"completed": s["completed"], "timed_out": s["timeouts"],
                                  "in_use_at_end": s["final_in_use"]})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        phase = self._effective_phase(ctx)
        s = self._stat(ctx, phase)
        held = self._held_open(ctx, phase)

        data_bad = s["timeouts"] > 0 or s["completed"] < N_TASKS
        control_bad = s["final_in_use"] != 0 or held > 0
        detected = data_bad or control_bad

        cc.write_quality_result(
            ctx, "MP-15-STALL-001", "completeness",
            expected=f"all {N_TASKS} aggregate tasks complete",
            actual=f"{s['completed']} completed, {s['timeouts']} timed out waiting for a session",
            status="FAIL" if data_bad else "PASS")
        cc.write_quality_result(
            ctx, "MP-15-SESSIONS-001", "control_consistency",
            expected="in-use sessions return to baseline (0) after the pass",
            actual=f"{s['final_in_use']} of {s['capacity']} sessions still in use; "
                   f"{held} left held open",
            status="FAIL" if control_bad else "PASS")

        assertions = [
            Assertion("MP-15-DET-STALL", "completeness",
                      "every aggregate task completes",
                      expected=N_TASKS, actual=s["completed"],
                      status="FAIL" if data_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-15-DET-SESSIONS", "control_consistency",
                      "in-use sessions return to baseline after the pass",
                      expected=0, actual=s["final_in_use"],
                      status="FAIL" if control_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "phase": phase,
                  "completed": s["completed"], "timed_out": s["timeouts"],
                  "capacity": s["capacity"], "max_in_use": s["max_in_use"],
                  "final_in_use": s["final_in_use"], "sessions_held_open": held}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-15", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-15", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        phase = self._effective_phase(ctx)

        rows = db.query(ctx.control,
            "SELECT task_index, task_id, outcome, acquire_step, session_state "
            "FROM mp15_task_result WHERE attempt_kind = ? ORDER BY task_index", (phase,))
        cc.write_csv(tables / "task_outcomes.csv", [dict(r) for r in rows],
                     ["task_index", "task_id", "outcome", "acquire_step", "session_state"])

        s = self._stat(ctx, phase)
        cc.write_csv(tables / "session_trajectory.csv",
                     [{"capacity": s["capacity"], "max_in_use": s["max_in_use"],
                       "final_in_use": s["final_in_use"], "tasks_completed": s["completed"],
                       "tasks_timed_out": s["timeouts"]}],
                     ["capacity", "max_in_use", "final_in_use", "tasks_completed", "tasks_timed_out"])

        sessions = db.query(ctx.control,
            "SELECT session_id, acquired_seq, released_seq, transaction_state "
            "FROM fake_pool_session WHERE session_id LIKE ? ORDER BY session_id",
            (f"SESS-{phase[0].upper()}-%",))
        cc.write_csv(tables / "session_states.csv",
                     [{"session_id": r["session_id"], "acquired_step": r["acquired_seq"],
                       "returned_step": r["released_seq"], "state": r["transaction_state"]}
                      for r in sessions],
                     ["session_id", "acquired_step", "returned_step", "state"])
        cc.write_standard_tables(ctx)

        alert = {
            "incident_id": "MP-15", "title": self.metadata.reader_title,
            "reported_symptom": (
                "At higher concurrency, every aggregate task stalled at the same step, "
                "waiting for a database session, and then timed out. The database itself "
                "stayed nearly idle. The number of in-use sessions rose to the capacity "
                "limit and never came back down."),
            "time_pressure": "Station aggregates are not refreshing while tasks stall.",
            "affected_product": "station aggregate tasks",
            "observed": {"tasks_completed": s["completed"], "tasks_timed_out": s["timeouts"],
                         "capacity": s["capacity"], "final_in_use": s["final_in_use"]},
            "fictional_notice": cc.FICTIONAL_NOTICE,
        }
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json", cc.sanitized_timeline(ctx))
        index = cc.write_evidence_index(ctx, "MP-15")
        return StepReport("evidence", "MP-15", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        s = self._stat(ctx, "faulty")
        blast = {"affected_product": "station aggregate tasks",
                 "tasks_timed_out": s["timeouts"], "sessions_held_open": self._held_open(ctx, "faulty"),
                 "action": "concurrency throttled; safe-to-end sessions identified; evidence preserved",
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-15", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-15", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        self._set_policy(ctx, HEALTHY_POLICY)
        with db.transaction(ctx.control):
            # End the sessions the faulty run left held open.
            ctx.control.execute(
                "UPDATE fake_pool_session SET transaction_state = 'ENDED', released_seq = -1 "
                "WHERE session_id LIKE 'SESS-F-%' AND transaction_state = 'HELD_OPEN'")
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", [])
                                        if t != "skip_release_on_error"]
        profile.setdefault("params", {})["session_policy"] = "context_managed_with_throttle"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"session_policy": "context_managed_with_throttle"})
        report = StepReport("repair", "MP-15", ctx.run_id, "repaired",
                            fields={"change": "a context-managed lifecycle returns the session on "
                                    "every path (including rollback on error); concurrency is "
                                    "throttled and the held-open sessions are ended"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (REPLAY the failed tasks after fixing the lifecycle) ----------
    def recover(self, ctx: RunContext) -> StepReport:
        self._run_phase(ctx, "recovery", self._policy(ctx), raise_all=False, persist_sessions=True)
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM simulated_outbox WHERE message_id LIKE 'MSG-mp15-%'")
            for task_id in TASK_IDS:
                ctx.control.execute(
                    "INSERT OR REPLACE INTO simulated_outbox "
                    "(message_id, topic, payload_json, logical_time) VALUES (?, ?, ?, ?)",
                    (f"MSG-mp15-{task_id}", "aggregate_written",
                     f'{{"task": "{task_id}", "state": "aggregated"}}',
                     ctx.clock.at_offset(0)))
        s = self._stat(ctx, "recovery")
        manifest = {
            "mode": "REPLAY",
            "scope": {"tasks": TASK_IDS, "target": "station aggregate tasks"},
            "write_mode": "bounded_replace",
            "duplicate_protection": "one idempotent aggregate message per task (stable id)",
            "tasks_completed": s["completed"], "final_in_use": s["final_in_use"],
            "returns_to_baseline": s["final_in_use"] == 0,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"tasks_completed": s["completed"]})
        return StepReport("recover", "MP-15", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        s = self._stat(ctx, "recovery")
        policy = self._policy(ctx)
        held_all = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM fake_pool_session WHERE transaction_state = 'HELD_OPEN'")
        outbox = [r["message_id"] for r in db.query(ctx.control,
            "SELECT message_id FROM simulated_outbox WHERE message_id LIKE 'MSG-mp15-%'")]
        present = bool(s)

        assertions = [
            Assertion("MP-15-VER-COMPLETE", "completeness",
                      "the replay completes every aggregate task",
                      expected=N_TASKS, actual=s["completed"] if present else 0,
                      status="PASS" if present and s["completed"] == N_TASKS else "FAIL",
                      evidence="reports/recovery.json"),
            Assertion("MP-15-VER-CORRECT", "correctness",
                      "no task times out waiting for a session in the replay",
                      expected=0, actual=s["timeouts"] if present else -1,
                      status="PASS" if present and s["timeouts"] == 0 else "FAIL"),
            Assertion("MP-15-VER-CONTROL", "control_consistency",
                      "in-use sessions return to baseline, none left held open, lifecycle fixed",
                      expected="0 in use / 0 held / context-managed",
                      actual=f"{s['final_in_use'] if present else '-'} in use / {held_all} held / "
                             f"{'ctx' if policy['context_managed'] else 'raw'}",
                      status="PASS" if present and s["final_in_use"] == 0 and held_all == 0
                             and policy["context_managed"] == 1 and policy["release_on_error"] == 1
                             else "FAIL"),
            Assertion("MP-15-VER-SIDEEFFECT", "side_effects",
                      "exactly one idempotent aggregate message per task",
                      expected=N_TASKS, actual=len(outbox),
                      status="PASS" if len(outbox) == N_TASKS else "FAIL"),
            Assertion("MP-15-VER-IDEMPOTENT", "idempotency",
                      "no duplicate aggregate message id",
                      expected=len(outbox), actual=len(set(outbox)),
                      status="PASS" if len(outbox) == len(set(outbox)) else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-15-VER-FIXTURE"),
        ]
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-15", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _run_phase(self, ctx, phase, policy, raise_all, persist_sessions) -> None:
        pool = sim.FakePool(policy["capacity"])
        results = sim.run_pool_tasks(
            pool, TASK_IDS, raises=lambda t: raise_all,
            release_on_error=bool(policy["release_on_error"]))
        task_rows = [(phase, i + 1, r["task_id"], r["outcome"], r["acquire_seq"],
                      r["session_state"]) for i, r in enumerate(results)]
        completed = sum(1 for r in results if r["outcome"] == "COMPLETED")
        timeouts = sum(1 for r in results if r["outcome"] == "TIMED_OUT")
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp15_task_result WHERE attempt_kind = ?", (phase,))
            db.insert_rows(ctx.control, "mp15_task_result",
                           ("attempt_kind", "task_index", "task_id", "outcome",
                            "acquire_step", "session_state"), task_rows)
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp15_pool_stat "
                "(attempt_kind, capacity, max_in_use, final_in_use, timeouts, completed) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (phase, policy["capacity"], pool.max_in_use, pool.in_use, timeouts, completed))
            if persist_sessions:
                pfx = f"SESS-{phase[0].upper()}-"
                ctx.control.execute("DELETE FROM fake_pool_session WHERE session_id LIKE ?",
                                    (f"{pfx}%",))
                for sid, sess in sorted(pool.sessions.items()):
                    task = sid[len("SESS-"):]
                    ctx.control.execute(
                        "INSERT OR REPLACE INTO fake_pool_session "
                        "(session_id, acquired_seq, released_seq, transaction_state) "
                        "VALUES (?, ?, ?, ?)",
                        (f"{pfx}{task}", sess["acquired_seq"], sess["released_seq"],
                         _SESSION_STATE.get(sess["state"], sess["state"])))

    def _stat(self, ctx, phase) -> dict[str, Any]:
        row = db.query_one(ctx.control, "SELECT * FROM mp15_pool_stat WHERE attempt_kind = ?", (phase,))
        return dict(row) if row is not None else {}

    def _held_open(self, ctx, phase) -> int:
        return db.scalar(ctx.control,
            "SELECT COUNT(*) FROM fake_pool_session WHERE session_id LIKE ? "
            "AND transaction_state = 'HELD_OPEN'", (f"SESS-{phase[0].upper()}-%",))

    def _effective_phase(self, ctx) -> str:
        has_recovery = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM mp15_pool_stat WHERE attempt_kind = 'recovery'")
        return "recovery" if has_recovery else "faulty"

    def _set_policy(self, ctx, p) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp15_session_policy "
                "(policy_id, capacity, context_managed, release_on_error, throttle) "
                "VALUES ('current', ?, ?, ?, ?)",
                (p["capacity"], p["context_managed"], p["release_on_error"], p["throttle"]))

    def _policy(self, ctx) -> dict[str, Any]:
        row = db.query_one(ctx.control, "SELECT * FROM mp15_session_policy WHERE policy_id = 'current'")
        return dict(row) if row is not None else {}


INCIDENT = MP15Incident()
