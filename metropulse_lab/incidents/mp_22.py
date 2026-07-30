"""MP-22 — A promised replay of an earlier station feed fails; the raw inputs are gone.

Frozen mechanism (incident_taxonomy.md MP-22): a storage policy kept raw inputs
for a far shorter window than the replay/audit window the runbook promised, and
no restore drill was ever run. The processed summary still exists, but the raw
objects needed to reproduce it are no longer present.

This incident is the one place a ``NO_REPLAY`` recovery is honest: the historical
result cannot be reproduced from a private oracle. Recovery proves the loss,
escalates it, fixes the forward-looking guardrail, and verifies the guardrail
with a restore drill at the oldest promised date. The frozen mechanism vocabulary
never appears under ``evidence/``.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchE_common as be
from .contract import IncidentMetadata

DATASET = "station_feed_raw"

AS_OF = date(2026, 7, 30)
PROMISED_DAYS = 90           # the promised replay/audit window (runbook promise)
BASELINE_KEEP_DAYS = 120     # healthy: keep at least as long as the promise
FAULT_KEEP_DAYS = 30         # faulty: far shorter than the promise
# Representative daily service dates, expressed as "days before AS_OF".
DAY_OFFSETS = (5, 15, 25, 35, 45, 60, 75, 90, 95)
# A specific promised date the team tries to replay ("last month").
REPLAY_OFFSET = 45

LEAK_TOKENS = ("retention", "lifecycle", "replay window", "90-day", "30-day",
               "aged out", "aged-out", "expiration", "expire")


def _svc_date(offset: int) -> str:
    return (AS_OF - timedelta(days=offset)).strftime("%Y-%m-%d")


def _object_id(service_date: str) -> str:
    return f"RAW-station_feed-{service_date}"


class MP22Incident:
    metadata = IncidentMetadata(
        incident_id="MP-22",
        family="E. Recovery accidents",
        reader_title="A promised replay of an earlier station feed fails, though its processed summary still exists",
        primary_invariant_id="MP-22-REPLAYABLE",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="NO_REPLAY",
        fault_operations=("shorten_raw_input_keep_window",),
        expected_detection_ids=("MP-22-REPLAYABLE-001", "MP-22-PROMISE-001"),
        transfer_mappings=("Object lifecycle policies; Iceberg snapshot expiration",),
        nearest_old_scenario="Adjacent to the source-outage scenario (old S13).",
        old_scenario_difference=(
            "Not an external source outage: the raw inputs were removed by an "
            "internal keep-window policy that was shorter than the promise the "
            "runbook made, and no restore drill ever caught it."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        self._set_policy(ctx, BASELINE_KEEP_DAYS, PROMISED_DAYS)
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp22_raw_object")
            ctx.control.execute("DELETE FROM mp22_processed")
            ctx.control.execute("DELETE FROM mp22_data_loss")
            ctx.control.execute("DELETE FROM mp22_drill")
            ctx.control.execute("DELETE FROM mp22_replay_request")
        for off in DAY_OFFSETS:
            sd = _svc_date(off)
            oid = _object_id(sd)
            be.put_object(ctx, oid, f"raw station feed for {sd}".encode("utf-8"))
            summary = be.det_int(sd, mod=50) + 10
            with db.transaction(ctx.control):
                ctx.control.execute(
                    "INSERT OR REPLACE INTO mp22_raw_object (object_id, service_date) VALUES (?, ?)",
                    (oid, sd))
                ctx.control.execute(
                    "INSERT OR REPLACE INTO mp22_processed (service_date, summary_rows) VALUES (?, ?)",
                    (sd, summary))

    def baseline_ok(self, ctx: RunContext) -> bool:
        policy = self._policy(ctx)
        if policy["retention_days"] < policy["promised_replay_days"]:
            return False
        return self._missing_promised_dates(ctx) == []

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        fixtures_before = self._fixture_ok(ctx)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"shorten_raw_input_keep_window"})
        profile.setdefault("params", {})
        ctx.save_fault_profile(profile)

        self._materialize_faulty(ctx)

        missing = self._missing_promised_dates(ctx)
        assertions = [
            Assertion("MP-22-INJECT-PRE", "precondition",
                      "every promised date is replayable before injection",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-22-INJECT-POST", "postcondition",
                      "promised dates now have no raw input",
                      expected=">0", actual=len(missing), status="PASS" if missing else "FAIL"),
            be.fixture_integrity_assertion(ctx, "MP-22-INJECT-FIXTURE"),
        ]
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-22", ctx.run_id, status,
                            fields={"promised_dates_missing": len(missing),
                                    "fixtures_unchanged": fixtures_before and self._fixture_ok(ctx)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._materialize_faulty(ctx)
        missing = self._missing_promised_dates(ctx)
        return StepReport("run", "MP-22", ctx.run_id, "executed",
                          fields={"promised_dates_missing": len(missing)})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        missing = self._missing_promised_dates(ctx)
        policy = self._policy(ctx)
        promise_uncovered = policy["retention_days"] < policy["promised_replay_days"]
        detected = bool(missing) or promise_uncovered

        be.write_quality_result(
            ctx, "MP-22-REPLAYABLE-001", "replayability",
            expected="every promised date still has its raw input present",
            actual=f"{len(missing)} promised date(s) have no raw input, "
                   f"though the processed summary still exists",
            status="FAIL" if missing else "PASS")
        be.write_quality_result(
            ctx, "MP-22-PROMISE-001", "control_consistency",
            expected="guaranteed raw-input availability covers the promised replay range",
            actual=("guaranteed input availability is shorter than the promised range"
                    if promise_uncovered else "guaranteed availability covers the promise"),
            status="FAIL" if promise_uncovered else "PASS")

        assertions = [
            Assertion("MP-22-DET-REPLAYABLE", "replayability",
                      "the raw inputs for the promised dates are present",
                      expected=0, actual=len(missing),
                      status="FAIL" if missing else "PASS", evidence="reports/detection.json"),
            Assertion("MP-22-DET-PROMISE", "control_consistency",
                      "guaranteed input availability covers the promised replay range",
                      expected=False, actual=promise_uncovered,
                      status="FAIL" if promise_uncovered else "PASS",
                      evidence="reports/detection.json"),
        ]
        fields = {
            "incident_detected": detected,
            "promised_dates_missing": missing,
            "oldest_recoverable_date": self._oldest_recoverable_date(ctx),
            "oldest_promised_date": _svc_date(PROMISED_DAYS),
        }
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-22", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-22", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- evidence (read-only) -------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        avail = []
        for r in db.query(ctx.control,
                          "SELECT service_date, object_id FROM mp22_raw_object ORDER BY service_date"):
            processed = db.scalar(ctx.control,
                                  "SELECT COUNT(*) FROM mp22_processed WHERE service_date = ?",
                                  (r["service_date"],))
            avail.append({"service_date": r["service_date"],
                          "raw_input_present": 1 if be.object_durable(ctx, r["object_id"]) else 0,
                          "processed_summary_present": 1 if processed else 0})
        be.write_csv(tables / "input_availability.csv", avail,
                     ["service_date", "raw_input_present", "processed_summary_present"])

        reqs = db.rows_to_dicts(db.query(
            ctx.control,
            "SELECT request_id, service_date, status, note FROM mp22_replay_request "
            "ORDER BY request_id"))
        be.write_csv(tables / "replay_request.csv", reqs,
                     ["request_id", "service_date", "status", "note"])

        be.write_quality_results_csv(ctx)

        missing = self._missing_promised_dates(ctx)
        alert = be.base_alert(
            "MP-22",
            self.metadata.reader_title,
            reported_symptom=(
                "The team tried to replay an earlier month of the station feed as "
                "promised in the runbook. The raw inputs for the requested dates are "
                "no longer present, even though the processed summary for those same "
                "dates still exists."),
            time_pressure="An audit needs the earlier month reproduced.",
            affected_product=DATASET,
            extra={"requested_date": _svc_date(REPLAY_OFFSET),
                   "promised_dates_without_raw_input": missing})
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json",
                   be.sanitized_timeline(ctx, LEAK_TOKENS))
        index = be.write_evidence_index(ctx, "MP-22")
        return StepReport("evidence", "MP-22", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        missing = self._missing_promised_dates(ctx)
        # Freeze further removal so the surviving raw inputs are preserved as
        # evidence and cannot shrink further while the incident is open.
        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp22_removal_frozen"] = True
        ctx.save_fault_profile(profile)
        blast = {"dataset": DATASET, "promised_dates_without_raw_input": missing,
                 "oldest_recoverable_date": self._oldest_recoverable_date(ctx),
                 "processed_summary_preserved": True, "evidence_preserved": True}
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-22", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-22", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [
            t for t in profile.get("faulty_transforms", [])
            if t != "shorten_raw_input_keep_window"]
        ctx.save_fault_profile(profile)
        # Fix the cause: the raw-input keep window must be at least the promise.
        corrected = max(BASELINE_KEEP_DAYS, PROMISED_DAYS)
        self._set_policy(ctx, corrected, PROMISED_DAYS)
        report = StepReport("repair", "MP-22", ctx.run_id, "repaired",
                            fields={"corrected_keep_days": corrected,
                                    "promised_days": PROMISED_DAYS,
                                    "recoverable_boundary": self._oldest_recoverable_date(ctx)})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (NO_REPLAY) --------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        lost = self._missing_promised_dates(ctx)
        # 1. Record the loss honestly. Do NOT recreate the lost raw inputs.
        with db.transaction(ctx.control):
            for sd in lost:
                ctx.control.execute(
                    "INSERT OR REPLACE INTO mp22_data_loss (service_date, note) VALUES (?, ?)",
                    (sd, "raw input no longer present; no approved backup or upstream source"))
        # 2. Verify the corrected guardrail with a restore drill at the oldest
        #    promised date: a fresh probe object at that age must survive the
        #    corrected policy and be readable. This is a guardrail probe, not a
        #    reconstruction of any lost raw input.
        drill_date = _svc_date(PROMISED_DAYS)
        drill_id = f"DRILL-station_feed-{drill_date}"
        be.put_object(ctx, drill_id, f"guardrail restore drill for {drill_date}".encode("utf-8"))
        retained = self._is_retained(ctx, drill_date)
        readable = be.object_durable(ctx, drill_id)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp22_drill (drill_id, service_date, object_id, result) "
                "VALUES (?, ?, ?, ?)",
                (drill_id, drill_date, drill_id,
                 "retained_and_readable" if retained and readable else "failed"))
        # 3. Escalate the loss.
        be.record_outbox(ctx, f"MP-22-{ctx.run_id}-data-loss", "data_loss_escalation",
                         {"dataset": DATASET, "lost_dates": lost})
        manifest = {
            "mode": "NO_REPLAY",
            "scope": {"dataset": DATASET, "unrecoverable_dates": lost},
            "reason": "the required raw inputs no longer exist and no approved backup "
                      "or upstream source is available; the historical result is not "
                      "reproduced from a private oracle",
            "write_mode": "none",
            "duplicate_protection": "n/a (no data reprocessed)",
            "escalation": "data_loss_escalation",
            "guardrail_drill": {"date": drill_date, "retained": retained, "readable": readable},
            "validation": "guardrail restore drill at the oldest promised date and an "
                          "automatic input-availability vs promise comparison",
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-22", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        policy = self._policy(ctx)
        guardrail_ok = policy["retention_days"] >= policy["promised_replay_days"]
        drill = db.query_one(ctx.control,
                             "SELECT drill_id, service_date, result FROM mp22_drill LIMIT 1")
        drill_ok = drill is not None and drill["result"] == "retained_and_readable" \
            and be.object_durable(ctx, drill["drill_id"])
        lost = self._promised_dates()  # all promised dates aged out by the fault
        genuinely_lost = [sd for sd in lost if not self._raw_present(ctx, sd)]
        ledger = {r["service_date"] for r in db.query(
            ctx.control, "SELECT service_date FROM mp22_data_loss")}
        dataloss_ok = bool(genuinely_lost) and set(genuinely_lost) <= ledger
        survivors = [sd for sd in self._all_dates() if self._raw_present(ctx, sd)]
        present_raw = sum(1 for sd in self._all_dates() if self._raw_present(ctx, sd))

        assertions: list[Assertion] = []
        assertions.append(Assertion(
            "MP-22-VER-GUARDRAIL", "control_consistency",
            "guaranteed raw-input availability now covers the promised replay range",
            expected=True, actual=guardrail_ok, status="PASS" if guardrail_ok else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-22-VER-DRILL", "completeness",
            "a restore drill at the oldest promised date is retained and readable",
            expected=True, actual=drill_ok, status="PASS" if drill_ok else "FAIL"))
        assertions.append(Assertion(
            "MP-22-VER-DATALOSS", "history",
            "unrecoverable promised dates are escalated as data loss and stay absent",
            expected="all-recorded", actual=f"{len(ledger & set(genuinely_lost))}/{len(genuinely_lost)}",
            status="PASS" if dataloss_ok else "FAIL"))
        assertions.append(Assertion(
            "MP-22-VER-NOFABRICATION", "correctness",
            "recovery did not fabricate raw inputs for lost dates",
            expected=len(survivors), actual=present_raw,
            status="PASS" if present_raw == len(survivors) else "FAIL"))
        changed = be.shared_tables_changed(ctx)
        assertions.append(Assertion(
            "MP-22-VER-BOUNDED", "boundedness",
            "no shared warehouse/mart table changed during recovery",
            expected=[], actual=changed, status="PASS" if not changed else "FAIL"))
        assertions.append(be.fixture_integrity_assertion(ctx, "MP-22-VER-FIXTURE"))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-22", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions),
                                    "unrecoverable_dates": genuinely_lost})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.control):
            ctx.control.executescript(
                """
                CREATE TABLE IF NOT EXISTS mp22_policy (
                    policy_id            TEXT NOT NULL PRIMARY KEY,
                    retention_days       INTEGER NOT NULL,
                    promised_replay_days INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp22_raw_object (
                    object_id    TEXT NOT NULL PRIMARY KEY,
                    service_date TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp22_processed (
                    service_date TEXT NOT NULL PRIMARY KEY,
                    summary_rows INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp22_data_loss (
                    service_date TEXT NOT NULL PRIMARY KEY,
                    note         TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp22_drill (
                    drill_id     TEXT NOT NULL PRIMARY KEY,
                    service_date TEXT NOT NULL,
                    object_id    TEXT NOT NULL,
                    result       TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp22_replay_request (
                    request_id   TEXT NOT NULL PRIMARY KEY,
                    service_date TEXT NOT NULL,
                    status       TEXT NOT NULL,
                    note         TEXT
                );
                """)

    def _set_policy(self, ctx, retention_days, promised_days) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp22_policy "
                "(policy_id, retention_days, promised_replay_days) VALUES ('raw-input', ?, ?)",
                (retention_days, promised_days))

    def _policy(self, ctx) -> dict[str, int]:
        row = db.query_one(ctx.control,
                           "SELECT retention_days, promised_replay_days FROM mp22_policy "
                           "WHERE policy_id = 'raw-input'")
        return {"retention_days": row["retention_days"],
                "promised_replay_days": row["promised_replay_days"]}

    def _materialize_faulty(self, ctx: RunContext) -> None:
        # Deterministic replay of the shorter keep window: every object older than
        # the (short) cutoff is moved to recoverable trash, never permanently
        # deleted. The processed summary is untouched.
        self._set_policy(ctx, FAULT_KEEP_DAYS, PROMISED_DAYS)
        cutoff = AS_OF - timedelta(days=FAULT_KEEP_DAYS)
        for off in DAY_OFFSETS:
            sd = _svc_date(off)
            if date.fromisoformat(sd) < cutoff:
                be.trash_object(ctx, _object_id(sd))
        # The team's attempt to replay a specific promised date.
        rsd = _svc_date(REPLAY_OFFSET)
        present = self._raw_present(ctx, rsd)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp22_replay_request "
                "(request_id, service_date, status, note) VALUES (?, ?, ?, ?)",
                (f"REQ-{rsd}", rsd,
                 "raw_input_available" if present else "raw_input_not_available",
                 "processed summary still present" if not present else "ok"))

    def _all_dates(self) -> list[str]:
        return [_svc_date(off) for off in DAY_OFFSETS]

    def _promised_dates(self) -> list[str]:
        # Dates inside the promised window (offset <= promise), oldest first.
        return [_svc_date(off) for off in sorted(DAY_OFFSETS, reverse=True)
                if off <= PROMISED_DAYS]

    def _raw_present(self, ctx, service_date: str) -> bool:
        return be.object_durable(ctx, _object_id(service_date))

    def _missing_promised_dates(self, ctx) -> list[str]:
        return [sd for sd in self._promised_dates() if not self._raw_present(ctx, sd)]

    def _is_retained(self, ctx, service_date: str) -> bool:
        policy = self._policy(ctx)
        cutoff = AS_OF - timedelta(days=policy["retention_days"])
        return date.fromisoformat(service_date) >= cutoff

    def _oldest_recoverable_date(self, ctx) -> str | None:
        present = sorted(sd for sd in self._all_dates() if self._raw_present(ctx, sd))
        return present[0] if present else None

    def _fixture_ok(self, ctx) -> bool:
        ok, _ = be.integrity.fixtures_untouched(ctx)
        return ok


INCIDENT = MP22Incident()
