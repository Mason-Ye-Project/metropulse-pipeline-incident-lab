"""MP-23 — Reverting one bad calendar load also erased another team's good update.

Frozen mechanism (incident_taxonomy.md MP-23): to undo one bad service-calendar
load, an operator reverted the whole table history back to the change both teams
branched from. That discarded every change recorded after the bad one — including
an unrelated, correct accessibility-station update made by a different team.

The reader-facing title and the evidence describe only the symptom: a correct
update from an unrelated team disappeared after a bad change was reverted. The
frozen mechanism vocabulary never appears under ``evidence/``.
"""

from __future__ import annotations

import json
from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchE_common as be
from .contract import IncidentMetadata

DATASET = "service_calendar_table"

C0 = "COMMIT-base"                 # common starting change both teams branched from
C1 = "COMMIT-calendar-bad"         # the bad service-calendar load (owner: calendar team)
C2 = "COMMIT-accessibility-good"   # unrelated, correct accessibility update (owner: a11y team)
C3 = "COMMIT-recovery"             # the corrected recovery change

BAD_COMMIT = C1
BAD_OWNER = "calendar_team"

BASE_ROWS = {"STN_BASE_1": "v1", "STN_BASE_2": "v1"}
BAD_ROWS = {"CAL_HOLIDAY_BAD": "wrong-holiday"}
GOOD_ROWS = {"STN_ACCESS_RAMP": "ramp-open"}

LEAK_TOKENS = ("rollback", "roll back", "snapshot", "compensation", "compensating",
               "selective", "ancestor", "cherry-pick", "cherry pick", "collateral")


class MP23Incident:
    metadata = IncidentMetadata(
        incident_id="MP-23",
        family="E. Recovery accidents",
        reader_title="Reverting one bad calendar load also erased another team's correct accessibility update",
        primary_invariant_id="MP-23-SCOPE",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("revert_whole_history_to_common_base",),
        expected_detection_ids=("MP-23-PRESENCE-001", "MP-23-SCOPE-001"),
        transfer_mappings=("Iceberg branches, tags, and cherry-pick/rollback semantics",),
        nearest_old_scenario="No close prior scenario in the earlier books.",
        old_scenario_difference=(
            "The earlier books have no whole-history revert that removes an "
            "unrelated team's later, correct change as a side effect of undoing a "
            "different bad change."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM snapshot_commit")
            ctx.control.execute("DELETE FROM mp23_action")
        self._put_commit(ctx, C0, None, "platform", added=BASE_ROWS, removed=[])
        self._put_commit(ctx, C1, C0, BAD_OWNER, added=BAD_ROWS, removed=[])
        self._put_commit(ctx, C2, C1, "accessibility_team", added=GOOD_ROWS, removed=[])
        self._set_head(ctx, C2)
        self._materialize(ctx, self._fold(ctx, [C0, C1, C2]))

    def baseline_ok(self, ctx: RunContext) -> bool:
        # DAG integrity: the materialized head equals the accumulated change
        # history, and both teams' recorded changes are reflected.
        head = self._head(ctx)
        if head != C2:
            return False
        expected = self._fold(ctx, [C0, C1, C2])
        return self._current_rows(ctx) == expected

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        fixtures_before = self._fixture_ok(ctx)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"revert_whole_history_to_common_base"})
        profile.setdefault("params", {})
        ctx.save_fault_profile(profile)

        self._materialize_faulty(ctx)

        missing = self._missing_approved_rows(ctx)
        assertions = [
            Assertion("MP-23-INJECT-PRE", "precondition",
                      "both teams' changes are present before injection",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-23-INJECT-POST", "postcondition",
                      "an unrelated team's change is now missing",
                      expected=">0", actual=len(missing), status="PASS" if missing else "FAIL"),
            be.fixture_integrity_assertion(ctx, "MP-23-INJECT-FIXTURE"),
        ]
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-23", ctx.run_id, status,
                            fields={"missing_unrelated_rows": len(missing),
                                    "fixtures_unchanged": fixtures_before and self._fixture_ok(ctx)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._materialize_faulty(ctx)
        return StepReport("run", "MP-23", ctx.run_id, "executed",
                          fields={"head": self._head(ctx),
                                  "missing_unrelated_rows": len(self._missing_approved_rows(ctx))})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        missing = self._missing_approved_rows(ctx)
        discarded_other = self._discarded_other_owners(ctx)
        detected = bool(missing) or bool(discarded_other)

        be.write_quality_result(
            ctx, "MP-23-PRESENCE-001", "presence",
            expected="an unrelated team's correct change is still present after undoing a bad change",
            actual=f"{len(missing)} row(s) from an unrelated change are missing",
            status="FAIL" if missing else "PASS")
        be.write_quality_result(
            ctx, "MP-23-SCOPE-001", "control_consistency",
            expected="undoing one change removes only that change's rows",
            actual=("undoing one change also removed rows introduced by a different team"
                    if discarded_other else "only the intended change was removed"),
            status="FAIL" if discarded_other else "PASS")

        assertions = [
            Assertion("MP-23-DET-PRESENCE", "presence",
                      "rows introduced by unrelated changes remain present",
                      expected=0, actual=len(missing),
                      status="FAIL" if missing else "PASS", evidence="reports/detection.json"),
            Assertion("MP-23-DET-SCOPE", "control_consistency",
                      "undoing one change does not remove another team's change",
                      expected=[], actual=discarded_other,
                      status="FAIL" if discarded_other else "PASS",
                      evidence="reports/detection.json"),
        ]
        fields = {
            "incident_detected": detected,
            "head": self._head(ctx),
            "missing_unrelated_rows": sorted(missing),
            "discarded_changes_by_other_teams": discarded_other,
        }
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-23", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-23", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- evidence (read-only) -------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        history = []
        for r in db.query(ctx.control,
                          "SELECT snapshot_id, owner, change_set_json FROM snapshot_commit "
                          "ORDER BY snapshot_id"):
            cs = json.loads(r["change_set_json"])
            history.append({
                "change_id": r["snapshot_id"], "owner": r["owner"],
                "rows_added": ",".join(sorted(cs.get("added", {}).keys())),
                "rows_removed": ",".join(sorted(cs.get("removed", []))),
            })
        be.write_csv(tables / "change_history.csv", history,
                     ["change_id", "owner", "rows_added", "rows_removed"])

        introduced_by = self._introduced_by(ctx)
        current = self._current_rows(ctx)
        rows = []
        for key in sorted(introduced_by):
            change_id, owner = introduced_by[key]
            rows.append({"row_key": key, "in_current_table": 1 if key in current else 0,
                         "introduced_by_change": change_id, "introduced_by_team": owner})
        be.write_csv(tables / "row_presence.csv", rows,
                     ["row_key", "in_current_table", "introduced_by_change", "introduced_by_team"])

        be.write_quality_results_csv(ctx)

        missing = self._missing_approved_rows(ctx)
        alert = be.base_alert(
            "MP-23",
            self.metadata.reader_title,
            reported_symptom=(
                "A bad service-calendar load was undone. Afterward, a different "
                "team's accessibility-station update — made in the same period but "
                "unrelated to the calendar load — also disappeared from the table."),
            time_pressure="The accessibility team needs their correct update restored.",
            affected_product=DATASET,
            extra={"missing_rows": sorted(missing)})
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json",
                   be.sanitized_timeline(ctx, LEAK_TOKENS))
        index = be.write_evidence_index(ctx, "MP-23")
        return StepReport("evidence", "MP-23", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        missing = self._missing_approved_rows(ctx)
        blast = {"dataset": DATASET, "missing_rows": sorted(missing),
                 "unrelated_change": {"change_id": C2, "team": "accessibility_team"},
                 "evidence_preserved": True}
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp23_action (action_id, kind, from_head, to_head, note) "
                "VALUES ('contain', 'hold', ?, ?, 'table held pending corrected recovery')",
                (self._head(ctx), self._head(ctx)))
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-23", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-23", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [
            t for t in profile.get("faulty_transforms", [])
            if t != "revert_whole_history_to_common_base"]
        profile.setdefault("params", {})["mp23_guardrail"] = "undo_only_the_approved_change_set"
        ctx.save_fault_profile(profile)
        # The corrected plan: undo only the approved change, re-applying the
        # unrelated change on the current head.
        desired = self._desired_rows(ctx)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp23_action (action_id, kind, from_head, to_head, note) "
                "VALUES ('repair', 'plan', ?, ?, ?)",
                (self._head(ctx), C3,
                 f"undo {BAD_COMMIT}; preserve {C2}; target {len(desired)} rows"))
        report = StepReport("repair", "MP-23", ctx.run_id, "repaired",
                            fields={"undo_change": BAD_COMMIT, "preserve_change": C2,
                                    "target_row_count": len(desired)})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (REPLAY selective compensation) ------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        desired = self._desired_rows(ctx)
        self._materialize(ctx, desired)
        # Record the corrected recovery change on top of the current head.
        added = {k: v for k, v in GOOD_ROWS.items()}
        self._put_commit(ctx, C3, C0, "platform_recovery", added=added, removed=[])
        self._set_head(ctx, C3)
        be.record_outbox(ctx, f"MP-23-{ctx.run_id}-restored", "unrelated_change_restored",
                         {"dataset": DATASET, "restored_change": C2})
        manifest = {
            "mode": "REPLAY",
            "scope": {"dataset": DATASET, "undo_change": BAD_COMMIT,
                      "preserved_change": C2, "target_rows": sorted(desired.keys())},
            "write_mode": "bounded_replace",
            "duplicate_protection": "row-key grain; deterministic desired-state recompute",
            "concurrency_fence": f"new head {C3}",
            "downstream_descendants": [DATASET],
            "validation": "current-head difference stays within the approved change; "
                          "the unrelated change is preserved",
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-23", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        desired = self._desired_rows(ctx)
        current = self._current_rows(ctx)
        good_present = all(k in current for k in GOOD_ROWS)
        bad_absent = all(k not in current for k in BAD_ROWS)
        head = self._head(ctx)
        guardrail = ctx.load_fault_profile().get("params", {}).get("mp23_guardrail")
        # Boundedness: the difference from the reverted base is only approved rows.
        diff = self._diff_keys(current, BASE_ROWS)
        approved = set(GOOD_ROWS.keys())
        within_scope = diff <= approved

        assertions: list[Assertion] = []
        assertions.append(Assertion(
            "MP-23-VER-CORRECT", "correctness",
            "the table matches the approved end state (bad change undone, good change kept)",
            expected=sorted(desired.keys()), actual=sorted(current.keys()),
            status="PASS" if current == desired else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-23-VER-COMPLETE", "completeness",
            "the unrelated team's correct rows are present",
            expected=True, actual=good_present, status="PASS" if good_present else "FAIL"))
        assertions.append(Assertion(
            "MP-23-VER-HISTORY", "history",
            "the bad change's rows are not present",
            expected=True, actual=bad_absent, status="PASS" if bad_absent else "FAIL"))
        assertions.append(Assertion(
            "MP-23-VER-CONTROL", "control_consistency",
            "the head is the corrected recovery change and the guardrail is in place",
            expected=f"{C3};guardrail", actual=f"{head};{guardrail}",
            status="PASS" if head == C3 and guardrail else "FAIL"))
        assertions.append(Assertion(
            "MP-23-VER-BOUNDED", "boundedness",
            "the difference from the reverted base stays within the approved change",
            expected=[], actual=sorted(diff - approved),
            status="PASS" if within_scope else "FAIL"))
        changed = be.shared_tables_changed(ctx)
        assertions.append(Assertion(
            "MP-23-VER-SHARED", "boundedness",
            "no shared warehouse/mart table changed during recovery",
            expected=[], actual=changed, status="PASS" if not changed else "FAIL"))
        assertions.append(be.fixture_integrity_assertion(ctx, "MP-23-VER-FIXTURE"))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-23", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions), "head": head})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.control):
            ctx.control.executescript(
                """
                CREATE TABLE IF NOT EXISTS mp23_table (
                    row_key TEXT NOT NULL PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp23_head (
                    dataset_name  TEXT NOT NULL PRIMARY KEY,
                    head_snapshot TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp23_action (
                    action_id TEXT NOT NULL PRIMARY KEY,
                    kind      TEXT NOT NULL,
                    from_head TEXT,
                    to_head   TEXT,
                    note      TEXT
                );
                """)

    def _put_commit(self, ctx, snapshot_id, parent, owner, added, removed) -> None:
        change_set = json.dumps({"added": added, "removed": removed}, sort_keys=True)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO snapshot_commit "
                "(snapshot_id, parent_snapshot, owner, change_set_json) VALUES (?, ?, ?, ?)",
                (snapshot_id, parent, owner, change_set))

    def _commit_change(self, ctx, snapshot_id) -> dict[str, Any]:
        raw = db.scalar(ctx.control,
                        "SELECT change_set_json FROM snapshot_commit WHERE snapshot_id = ?",
                        (snapshot_id,))
        return json.loads(raw) if raw else {"added": {}, "removed": []}

    def _fold(self, ctx, commit_ids) -> dict[str, str]:
        rows: dict[str, str] = {}
        for cid in commit_ids:
            cs = self._commit_change(ctx, cid)
            rows.update(cs.get("added", {}))
            for k in cs.get("removed", []):
                rows.pop(k, None)
        return rows

    def _desired_rows(self, ctx) -> dict[str, str]:
        # Undo only the bad change: apply the base and the unrelated good change,
        # skipping the bad change entirely.
        return self._fold(ctx, [C0, C2])

    def _materialize(self, ctx, rows: dict[str, str]) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp23_table")
            for k in sorted(rows):
                ctx.control.execute(
                    "INSERT INTO mp23_table (row_key, payload) VALUES (?, ?)", (k, rows[k]))

    def _materialize_faulty(self, ctx: RunContext) -> None:
        # The naive whole-history revert: head returns to the common base and the
        # table is rebuilt to the base state, dropping every later change.
        self._materialize(ctx, self._fold(ctx, [C0]))
        self._set_head(ctx, C0)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp23_action (action_id, kind, from_head, to_head, note) "
                "VALUES ('faulty_revert', 'whole_history_revert', ?, ?, 'table rebuilt to the base change')",
                (C2, C0))

    def _current_rows(self, ctx) -> dict[str, str]:
        return {r["row_key"]: r["payload"] for r in db.query(
            ctx.control, "SELECT row_key, payload FROM mp23_table ORDER BY row_key")}

    def _head(self, ctx) -> str | None:
        return db.scalar(ctx.control,
                         "SELECT head_snapshot FROM mp23_head WHERE dataset_name = ?", (DATASET,))

    def _set_head(self, ctx, snapshot_id) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp23_head (dataset_name, head_snapshot) VALUES (?, ?)",
                (DATASET, snapshot_id))

    def _missing_approved_rows(self, ctx) -> set[str]:
        desired = self._desired_rows(ctx)
        current = self._current_rows(ctx)
        return {k for k in desired if k not in current}

    def _discarded_other_owners(self, ctx) -> list[dict[str, str]]:
        """Original commits removed by the revert whose owner is not the bad-change
        owner (i.e. a different team's change removed as a side effect)."""
        head = self._head(ctx)
        if head != C0:
            return []
        out = []
        for cid, owner in ((C1, BAD_OWNER), (C2, "accessibility_team")):
            if owner != BAD_OWNER and self._missing_approved_rows(ctx):
                # C2 is an original commit after the new head; its rows are gone.
                cs = self._commit_change(ctx, cid)
                if any(k not in self._current_rows(ctx) for k in cs.get("added", {})):
                    out.append({"change_id": cid, "team": owner})
        return out

    def _introduced_by(self, ctx) -> dict[str, tuple[str, str]]:
        out: dict[str, tuple[str, str]] = {}
        for cid in (C0, C1, C2):
            owner = db.scalar(ctx.control,
                              "SELECT owner FROM snapshot_commit WHERE snapshot_id = ?", (cid,))
            for k in self._commit_change(ctx, cid).get("added", {}):
                out[k] = (cid, owner)
        return out

    @staticmethod
    def _diff_keys(current: dict[str, str], base: dict[str, str]) -> set[str]:
        keys = set()
        for k in set(current) | set(base):
            if current.get(k) != base.get(k):
                keys.add(k)
        return keys

    def _fixture_ok(self, ctx) -> bool:
        ok, _ = be.integrity.fixtures_untouched(ctx)
        return ok


INCIDENT = MP23Incident()
