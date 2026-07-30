"""MP-25 — The target database is restored, but half an hour of ridership is gone.

Frozen mechanism (incident_taxonomy.md MP-25): the target database and the feed
position were restored to different points. The target came back from a 10:00
snapshot while the feed position said 10:30, so the pipeline resumed past the
10:00-10:30 window and those ridership events were never applied. "Database
restored" and "pipeline resumed" were judged separately.

The reader-facing title and the evidence describe only the symptom: a permanent
missing window of ridership after a restore. The frozen mechanism vocabulary
never appears under ``evidence/``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchE_common as be
from .contract import IncidentMetadata

SOURCE = "mp25_ridership_feed"
CONSUMER = "mp25_target_db"

MAX_OFFSET = 12                 # events at 10:00, 10:05, ... 11:00
BASE_TIME = datetime(2026, 4, 15, 10, 0, 0, tzinfo=timezone.utc)
STEP_MINUTES = 5

SINK_RESTORE_OFFSET = 0         # the target came back as of 10:00
FEED_RESUME_OFFSET = 6          # the feed position said 10:30

LEAK_TOKENS = ("restore point", "cursor", "consistent point", "gap",
               "offset mismatch", "reconcile", "reconciled", "reconciliation")


def _event_time(offset: int) -> str:
    return (BASE_TIME + timedelta(minutes=STEP_MINUTES * offset)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _value(offset: int) -> int:
    return be.det_int(f"ridership-evt-{offset}", mod=40) + 20


def _event_key(offset: int) -> str:
    return f"EVT-{offset:03d}"


class MP25Incident:
    metadata = IncidentMetadata(
        incident_id="MP-25",
        family="E. Recovery accidents",
        reader_title="After the target database was restored, half an hour of ridership is permanently missing",
        primary_invariant_id="MP-25-COVERAGE",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("restore_target_and_feed_to_different_points",),
        expected_detection_ids=("MP-25-COVERAGE-001", "MP-25-POSITION-001"),
        transfer_mappings=("Flink/Kafka coordinated checkpoints and transactions",),
        nearest_old_scenario="Concept-adjacent to snapshot/CDC discussion (old Q58).",
        old_scenario_difference=(
            "The earlier books discuss snapshots and change-data-capture as "
            "concepts; they have no experiment where a target and its feed "
            "position are restored to different points and leave a permanent "
            "missing window."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp25_events")
            ctx.control.execute("DELETE FROM mp25_sink")
            ctx.control.execute("DELETE FROM mp25_staging")
            ctx.control.execute("DELETE FROM mp25_boundary")
            for off in range(MAX_OFFSET + 1):
                ctx.control.execute(
                    "INSERT INTO mp25_events (offset, event_key, event_time, value) "
                    "VALUES (?, ?, ?, ?)", (off, _event_key(off), _event_time(off), _value(off)))
        # Consistent state: the target holds every event and the positions agree.
        self._apply_to_sink(ctx, range(MAX_OFFSET + 1))
        self._set_feed_position(ctx, MAX_OFFSET)
        self._set_checkpoint(ctx, MAX_OFFSET)

    def baseline_ok(self, ctx: RunContext) -> bool:
        cont = self._continuous_max(ctx)
        pos = self._feed_position(ctx)
        chk = self._checkpoint(ctx)
        return cont == pos == chk == MAX_OFFSET and self._missing_within_position(ctx) == []

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        fixtures_before = self._fixture_ok(ctx)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"restore_target_and_feed_to_different_points"})
        profile.setdefault("params", {})
        ctx.save_fault_profile(profile)

        self._materialize_faulty(ctx)

        missing = self._missing_within_position(ctx)
        assertions = [
            Assertion("MP-25-INJECT-PRE", "precondition",
                      "coverage is continuous before injection",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-25-INJECT-POST", "postcondition",
                      "events below the processed position are now missing",
                      expected=">0", actual=len(missing), status="PASS" if missing else "FAIL"),
            be.fixture_integrity_assertion(ctx, "MP-25-INJECT-FIXTURE"),
        ]
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-25", ctx.run_id, status,
                            fields={"missing_events": len(missing),
                                    "fixtures_unchanged": fixtures_before and self._fixture_ok(ctx)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._materialize_faulty(ctx)
        return StepReport("run", "MP-25", ctx.run_id, "executed",
                          fields={"missing_events": len(self._missing_within_position(ctx))})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        missing = self._missing_within_position(ctx)
        cont = self._continuous_max(ctx)
        pos = self._feed_position(ctx)
        three_point_mismatch = cont < pos
        detected = bool(missing) or three_point_mismatch

        be.write_quality_result(
            ctx, "MP-25-COVERAGE-001", "coverage",
            expected="every event the feed produced up to the processed position is present in the target",
            actual=f"{len(missing)} event(s) the feed produced are missing from the target, "
                   f"though the position is marked processed",
            status="FAIL" if missing else "PASS")
        be.write_quality_result(
            ctx, "MP-25-POSITION-001", "control_consistency",
            expected="the target's coverage is continuous up to the feed's processed position",
            actual=("the target is missing events below the feed's processed position"
                    if three_point_mismatch else "the target coverage matches the processed position"),
            status="FAIL" if three_point_mismatch else "PASS")

        assertions = [
            Assertion("MP-25-DET-COVERAGE", "coverage",
                      "coverage is continuous up to the processed position",
                      expected=0, actual=len(missing),
                      status="FAIL" if missing else "PASS", evidence="reports/detection.json"),
            Assertion("MP-25-DET-POSITION", "control_consistency",
                      "the target's continuous coverage reaches the processed position",
                      expected=False, actual=three_point_mismatch,
                      status="FAIL" if three_point_mismatch else "PASS",
                      evidence="reports/detection.json"),
        ]
        fields = {
            "incident_detected": detected,
            "target_snapshot_position": self._continuous_max(ctx),
            "feed_processed_position": pos,
            "processing_checkpoint": self._checkpoint(ctx),
            "missing_offsets": missing,
            "missing_window": [_event_time(missing[0]), _event_time(missing[-1])] if missing else None,
        }
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-25", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-25", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- evidence (read-only) -------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        pos = self._feed_position(ctx)
        sink = self._sink_offsets(ctx)

        cov = []
        for off in range(MAX_OFFSET + 1):
            cov.append({"offset": off, "event_time": _event_time(off),
                        "present_in_target": 1 if off in sink else 0,
                        "within_processed_position": 1 if off <= pos else 0})
        be.write_csv(tables / "coverage.csv", cov,
                     ["offset", "event_time", "present_in_target", "within_processed_position"])

        be.write_csv(tables / "positions.csv",
                     [{"name": "target_snapshot_position", "value": self._continuous_max(ctx)},
                      {"name": "feed_processed_position", "value": pos},
                      {"name": "processing_checkpoint", "value": self._checkpoint(ctx)}],
                     ["name", "value"])

        be.write_quality_results_csv(ctx)

        missing = self._missing_within_position(ctx)
        alert = be.base_alert(
            "MP-25",
            self.metadata.reader_title,
            reported_symptom=(
                "After the target database was restored from an earlier snapshot, a "
                "run of ridership events is permanently missing from it. The feed "
                "reports it continued without interruption, yet the target never "
                "received the events for that window."),
            time_pressure="A ridership total for the affected hour is due.",
            affected_product="ridership_target_db",
            extra={"missing_window":
                   [_event_time(missing[0]), _event_time(missing[-1])] if missing else None,
                   "missing_event_count": len(missing)})
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json",
                   be.sanitized_timeline(ctx, LEAK_TOKENS))
        index = be.write_evidence_index(ctx, "MP-25")
        return StepReport("evidence", "MP-25", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        missing = self._missing_within_position(ctx)
        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp25_consuming_paused"] = True
        ctx.save_fault_profile(profile)
        blast = {"target": "ridership_target_db", "missing_event_count": len(missing),
                 "missing_window": [_event_time(missing[0]), _event_time(missing[-1])] if missing else None,
                 "evidence_preserved": True}
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-25", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-25", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [
            t for t in profile.get("faulty_transforms", [])
            if t != "restore_target_and_feed_to_different_points"]
        profile.setdefault("params", {})["mp25_guardrail"] = "restore_target_and_feed_together"
        ctx.save_fault_profile(profile)
        # Find the last provably-consistent boundary: the highest continuous prefix
        # the target actually holds. Record it and the position the feed had
        # advanced past, so the bounded replay is fixed and idempotent.
        boundary = self._continuous_max(ctx)
        faulty_position = self._feed_position(ctx)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp25_boundary (id, sink_boundary, faulty_position) "
                "VALUES ('main', ?, ?)", (boundary, faulty_position))
        report = StepReport("repair", "MP-25", ctx.run_id, "repaired",
                            fields={"consistent_boundary": boundary,
                                    "bounded_interval": [boundary + 1, faulty_position],
                                    "guardrail": "the target and the feed position must be "
                                                 "restored together to one provable point"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (REPLAY bounded, idempotent-key reconciled) ------------------
    def recover(self, ctx: RunContext) -> StepReport:
        row = db.query_one(ctx.control,
                           "SELECT sink_boundary, faulty_position FROM mp25_boundary WHERE id = 'main'")
        boundary = row["sink_boundary"]
        faulty_position = row["faulty_position"]
        interval = list(range(boundary + 1, faulty_position + 1))
        applied, ignored = 0, 0
        with db.transaction(ctx.control):
            for off in interval:
                key = _event_key(off)
                ctx.control.execute(
                    "INSERT OR REPLACE INTO mp25_staging (offset, event_key, value) "
                    "VALUES (?, ?, ?)", (off, key, _value(off)))
        # Apply the staged events into the target with an idempotent key upsert.
        for off in interval:
            already = self._sink_has(ctx, off)
            self._apply_to_sink(ctx, [off])
            if already:
                ignored += 1
            else:
                applied += 1
        # The target now holds a continuous run; align the positions to one point.
        new_point = self._continuous_max(ctx)
        self._set_feed_position(ctx, new_point)
        self._set_checkpoint(ctx, new_point)
        be.record_outbox(ctx, f"MP-25-{ctx.run_id}-window-replayed", "missing_window_replayed",
                         {"target": "ridership_target_db", "interval": interval})
        manifest = {
            "mode": "REPLAY",
            "scope": {"source": SOURCE, "interval_offsets": interval,
                      "interval_window": [_event_time(interval[0]), _event_time(interval[-1])]
                      if interval else None},
            "write_mode": "upsert",
            "duplicate_protection": "idempotent event key; already-present events are ignored",
            "concurrency_fence": "consuming paused during the bounded replay",
            "downstream_descendants": ["ridership_target_db"],
            "counts": {"applied": applied, "ignored": ignored, "quarantined": 0},
            "validation": "coverage continuous up to the aligned position; applied/ignored/"
                          "quarantined counts close; zero duplicate side effects",
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-25", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        cont = self._continuous_max(ctx)
        pos = self._feed_position(ctx)
        chk = self._checkpoint(ctx)
        missing = self._missing_within_position(ctx)
        # correctness: every event present with the correct value.
        wrong = self._wrong_values(ctx)
        dup = self._duplicate_keys(ctx)
        covered = all(off in self._sink_offsets(ctx) for off in range(MAX_OFFSET + 1))
        quarantined = 0
        guardrail = ctx.load_fault_profile().get("params", {}).get("mp25_guardrail")

        assertions: list[Assertion] = []
        assertions.append(Assertion(
            "MP-25-VER-CORRECT", "correctness",
            "every feed event is present in the target with the correct value",
            expected=0, actual=len(wrong), status="PASS" if not wrong else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-25-VER-COVERAGE", "coverage",
            "coverage is continuous with no missing events up to the processed position",
            expected=0, actual=len(missing), status="PASS" if not missing else "FAIL"))
        assertions.append(Assertion(
            "MP-25-VER-UNIQUE", "uniqueness",
            "no event key is applied more than once (no double-apply)",
            expected=0, actual=dup, status="PASS" if dup == 0 else "FAIL"))
        assertions.append(Assertion(
            "MP-25-VER-CONTROL", "control_consistency",
            "the target's continuous coverage, feed position and checkpoint agree",
            expected=f"{MAX_OFFSET}/{MAX_OFFSET}/{MAX_OFFSET}", actual=f"{cont}/{pos}/{chk}",
            status="PASS" if cont == pos == chk == MAX_OFFSET else "FAIL"))
        assertions.append(Assertion(
            "MP-25-VER-HISTORY", "history",
            "every event in the interval is applied exactly once and none quarantined",
            expected=True, actual=covered and quarantined == 0,
            status="PASS" if covered and quarantined == 0 else "FAIL"))
        assertions.append(Assertion(
            "MP-25-VER-SIDEEFFECTS", "side_effects",
            "the replay notification is recorded exactly once",
            expected=1, actual=be.outbox_count(ctx, f"MP-25-{ctx.run_id}-window-replayed"),
            status="PASS" if be.outbox_count(ctx, f"MP-25-{ctx.run_id}-window-replayed") == 1 else "FAIL"))
        changed = be.shared_tables_changed(ctx)
        assertions.append(Assertion(
            "MP-25-VER-BOUNDED", "boundedness",
            "no shared warehouse/mart table changed during recovery",
            expected=[], actual=changed, status="PASS" if not changed else "FAIL"))
        assertions.append(Assertion(
            "MP-25-VER-GUARDRAIL", "control_consistency",
            "the coordinated-restore guardrail is in place",
            expected="restore_target_and_feed_together", actual=guardrail,
            status="PASS" if guardrail == "restore_target_and_feed_together" else "FAIL"))
        assertions.append(be.fixture_integrity_assertion(ctx, "MP-25-VER-FIXTURE"))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-25", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions),
                                    "continuous_max": cont, "feed_position": pos})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.control):
            ctx.control.executescript(
                """
                CREATE TABLE IF NOT EXISTS mp25_events (
                    offset     INTEGER NOT NULL PRIMARY KEY,
                    event_key  TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    value      INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp25_sink (
                    event_key TEXT NOT NULL PRIMARY KEY,
                    offset    INTEGER NOT NULL,
                    applied_value INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp25_staging (
                    offset    INTEGER NOT NULL PRIMARY KEY,
                    event_key TEXT NOT NULL,
                    value     INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp25_boundary (
                    id             TEXT NOT NULL PRIMARY KEY,
                    sink_boundary  INTEGER NOT NULL,
                    faulty_position INTEGER NOT NULL
                );
                """)

    def _materialize_faulty(self, ctx: RunContext) -> None:
        # The target came back as of 10:00; the feed position said 10:30. On
        # resume the pipeline applied only events strictly after the feed
        # position, leaving the 10:00-10:30 window permanently unapplied.
        resume_from = FEED_RESUME_OFFSET + 1
        applied = list(range(SINK_RESTORE_OFFSET + 1)) + list(range(resume_from, MAX_OFFSET + 1))
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp25_sink")
        self._apply_to_sink(ctx, applied)
        self._set_feed_position(ctx, FEED_RESUME_OFFSET)
        self._set_checkpoint(ctx, FEED_RESUME_OFFSET)

    def _apply_to_sink(self, ctx, offsets) -> None:
        with db.transaction(ctx.control):
            for off in offsets:
                ctx.control.execute(
                    "INSERT OR REPLACE INTO mp25_sink (event_key, offset, applied_value) "
                    "VALUES (?, ?, ?)", (_event_key(off), off, _value(off)))

    def _sink_offsets(self, ctx) -> set[int]:
        return {r["offset"] for r in db.query(ctx.control, "SELECT offset FROM mp25_sink")}

    def _sink_has(self, ctx, offset) -> bool:
        return db.scalar(ctx.control,
                         "SELECT COUNT(*) FROM mp25_sink WHERE offset = ?", (offset,)) > 0

    def _continuous_max(self, ctx) -> int:
        sink = self._sink_offsets(ctx)
        m = -1
        for off in range(MAX_OFFSET + 1):
            if off in sink:
                m = off
            else:
                break
        return m

    def _feed_position(self, ctx) -> int:
        v = db.scalar(ctx.control,
                      "SELECT cursor_value FROM source_cursor WHERE source_name = ?", (SOURCE,))
        return int(v) if v is not None else -1

    def _set_feed_position(self, ctx, offset) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO source_cursor "
                "(source_name, cursor_value, committed_position, updated_logical_time) "
                "VALUES (?, ?, ?, ?)",
                (SOURCE, str(offset), str(offset), ctx.clock.at_offset(0)))

    def _checkpoint(self, ctx) -> int:
        v = db.scalar(ctx.control,
                      "SELECT last_source_position FROM consumer_checkpoint "
                      "WHERE consumer_name = ? AND source_name = ?", (CONSUMER, SOURCE))
        return int(v) if v is not None else -1

    def _set_checkpoint(self, ctx, offset) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO consumer_checkpoint "
                "(consumer_name, source_name, last_source_position, last_publication) "
                "VALUES (?, ?, ?, ?)", (CONSUMER, SOURCE, str(offset), "mp25"))

    def _missing_within_position(self, ctx) -> list[int]:
        sink = self._sink_offsets(ctx)
        pos = self._feed_position(ctx)
        return [off for off in range(pos + 1) if off not in sink]

    def _wrong_values(self, ctx) -> list[int]:
        wrong = []
        applied = {r["offset"]: r["applied_value"] for r in db.query(
            ctx.control, "SELECT offset, applied_value FROM mp25_sink")}
        for off in range(MAX_OFFSET + 1):
            if applied.get(off) != _value(off):
                wrong.append(off)
        return wrong

    def _duplicate_keys(self, ctx) -> int:
        return db.scalar(ctx.control,
                         "SELECT COUNT(*) - COUNT(DISTINCT event_key) FROM mp25_sink")

    def _fixture_ok(self, ctx) -> bool:
        ok, _ = be.integrity.fixtures_untouched(ctx)
        return ok


INCIDENT = MP25Incident()
