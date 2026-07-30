"""MP-08 — The scheduled-trips dashboard is short by a fixed block of trips daily.

Frozen mechanism (incident_taxonomy.md MP-08): the ingest loop stops one request
short of the authoritative terminal condition, so it never fetches the final
response and the dashboard misses close to one response's worth of trips every
day even though every request returned HTTP 200.

The loop must terminate only on the API contract's terminal marker, and the
cursor must advance only after a full response has persisted. Reader-facing text
describes only the symptom: a fixed daily shortfall while every request
succeeded.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchB_common as bc
from . import _batchB_mp08_api as api
from .contract import IncidentMetadata

DATASET = "mp08_scheduled_trips"
SOURCE = "mp08_trips"
OUTBOX_ID = "MSG-mp08-recovered"
TOTAL_ROWS = 20
LEAK = ("pagination", "off-by-one", "off by one", "token", "page", "continuation", "terminate")


class MP08Incident:
    metadata = IncidentMetadata(
        incident_id="MP-08",
        family="B. Missing or late",
        reader_title="The scheduled-trips dashboard is short by a fixed block of trips every day",
        primary_invariant_id="MP-08-TERMINAL-CONDITION",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("enable_faulty_ingest_loop:mp08",),
        expected_detection_ids=("MP-08-COMPLETENESS-001", "MP-08-CONTROL-001"),
        transfer_mappings=("AIP-158 or a source-specific paginated API contract",),
        nearest_old_scenario="Adjacent to generic late/missing data topics.",
        old_scenario_difference=(
            "Not throttling, not a source-side delete, and not a station filter: a "
            "boundary bug in the ingest loop drops the final response so a fixed block "
            "of trips is missing while every request returned success."
        ),
        affected_tables=("mp08_trip",),
    )

    # -- schema / baseline ----------------------------------------------------
    def _endpoint(self) -> api.FakeTripsEndpoint:
        return api.FakeTripsEndpoint(TOTAL_ROWS)

    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp08_trip ("
                "trip_id TEXT NOT NULL PRIMARY KEY, route_id TEXT NOT NULL, "
                "service_date TEXT NOT NULL, scheduled_start_utc TEXT NOT NULL)")
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp08_ingest_state ("
                "state_id INTEGER NOT NULL PRIMARY KEY, reached_terminal INTEGER NOT NULL, "
                "trips_ingested INTEGER NOT NULL, declared_total INTEGER NOT NULL)")

    def _store_trips(self, ctx: RunContext, rows: list[dict[str, Any]], replace: bool) -> None:
        with db.transaction(ctx.warehouse):
            if replace:
                ctx.warehouse.execute("DELETE FROM mp08_trip")
            verb = "INSERT OR IGNORE" if not replace else "INSERT"
            ctx.warehouse.executemany(
                f"{verb} INTO mp08_trip (trip_id, route_id, service_date, scheduled_start_utc) "
                "VALUES (?, ?, ?, ?)",
                [(r["trip_id"], r["route_id"], r["service_date"], r["scheduled_start_utc"])
                 for r in rows])

    def _record_request_log(self, ctx: RunContext, log: list[dict[str, Any]]) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM api_request_log WHERE source_name = ?", (SOURCE,))
            for i, entry in enumerate(log):
                ctx.control.execute(
                    "INSERT INTO api_request_log (request_id, source_name, snapshot_id, "
                    "page_token, next_token, response_code, attempt, worker, logical_time) "
                    "VALUES (?, ?, 'SNAP-mp08', ?, ?, ?, 1, 'ingest', ?)",
                    (f"REQ-mp08-{i:03d}", SOURCE, entry["request_marker"], entry["next_marker"],
                     entry["response_code"], ctx.clock.at_offset(i)))

    def _set_state(self, ctx: RunContext, reached: bool, ingested: int) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp08_ingest_state")
            ctx.warehouse.execute(
                "INSERT INTO mp08_ingest_state (state_id, reached_terminal, trips_ingested, "
                "declared_total) VALUES (1, ?, ?, ?)", (int(reached), ingested, TOTAL_ROWS))

    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        t = api.consume_correct(self._endpoint())
        self._store_trips(ctx, t.rows, replace=True)
        self._record_request_log(ctx, t.request_log)
        self._set_state(ctx, t.reached_terminal, len(t.rows))
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-baseline", DATASET, "service-date",
                                  "PUBLISHED", "PUB-baseline")
        ctx.log.emit(logical_time=ctx.clock.now(), component="ingest",
                     event_type="trips_ingested", fields={"trips": len(t.rows)})

    def baseline_ok(self, ctx: RunContext) -> bool:
        st = db.query_one(ctx.warehouse,
                          "SELECT reached_terminal, trips_ingested, declared_total "
                          "FROM mp08_ingest_state")
        cnt = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp08_trip")
        return bool(st and st["reached_terminal"] == 1
                    and st["trips_ingested"] == st["declared_total"] == cnt)

    # -- faulty ingest --------------------------------------------------------
    def _build_faulty(self, ctx: RunContext) -> dict[str, Any]:
        t = api.consume_faulty(self._endpoint())
        self._store_trips(ctx, t.rows, replace=True)
        self._record_request_log(ctx, t.request_log)
        self._set_state(ctx, t.reached_terminal, len(t.rows))
        return {"trips_ingested": len(t.rows), "declared_total": TOTAL_ROWS,
                "reached_terminal": t.reached_terminal}

    def inject(self, ctx: RunContext) -> StepReport:
        before = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp08_trip")
        pre_ok = before == TOTAL_ROWS
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp08_faulty_loop"] = True
        ctx.save_fault_profile(profile)

        result = self._build_faulty(ctx)
        after = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp08_trip")

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, _ = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-08-INJECT-PRE", "precondition",
                      "baseline ingested the full declared trip set",
                      expected=TOTAL_ROWS, actual=before, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-08-INJECT-POST", "postcondition",
                      "faulty ingest is short of the declared trip count",
                      expected=f"<{TOTAL_ROWS}", actual=after,
                      status="PASS" if after < TOTAL_ROWS else "FAIL"),
            Assertion("MP-08-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.at_offset(60), component="incident",
                     event_type="fault_injected", fields={"trips_short": TOTAL_ROWS - after})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-08", ctx.run_id, status,
                            fields={**result, "fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        return StepReport("run", "MP-08", ctx.run_id, "executed", fields=self._build_faulty(ctx))

    # -- detect ---------------------------------------------------------------
    def _signals(self, ctx: RunContext) -> dict[str, Any]:
        ingested = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp08_trip")
        terminal_reached = db.scalar(
            ctx.control, "SELECT COUNT(*) FROM api_request_log "
            "WHERE source_name = ? AND next_token IS NULL", (SOURCE,)) > 0
        return {"trips_ingested": ingested, "declared_total": TOTAL_ROWS,
                "shortfall": TOTAL_ROWS - ingested, "terminal_reached": terminal_reached,
                "data_gap": ingested < TOTAL_ROWS, "control_gap": not terminal_reached}

    def detect(self, ctx: RunContext) -> StepReport:
        s = self._signals(ctx)
        bc.write_quality_result(
            ctx, "MP-08-COMPLETENESS-001", "completeness",
            expected=f"{s['declared_total']} trips per the full-endpoint control count",
            actual=f"{s['trips_ingested']} trips ingested",
            status="FAIL" if s["data_gap"] else "PASS")
        bc.write_quality_result(
            ctx, "MP-08-CONTROL-001", "control_consistency",
            expected="the request sequence reaches the source's terminal condition",
            actual=("the request sequence stopped while the source still signaled more"
                    if s["control_gap"] else "the request sequence reached the terminal condition"),
            status="FAIL" if s["control_gap"] else "PASS")

        assertions = [
            Assertion("MP-08-DET-COMPLETE", "completeness",
                      "ingested trips match the full-endpoint control count",
                      expected=s["declared_total"], actual=s["trips_ingested"],
                      status="FAIL" if s["data_gap"] else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-08-DET-CONTROL", "control_consistency",
                      "request sequence reaches the source terminal condition",
                      expected=True, actual=s["terminal_reached"],
                      status="FAIL" if s["control_gap"] else "PASS",
                      evidence="reports/detection.json"),
        ]
        detected = s["data_gap"] or s["control_gap"]
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-08", "run_id": ctx.run_id, "fields": s,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-08", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=s, assertions=assertions)

    # -- evidence -------------------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        s = self._signals(ctx)

        bc.write_csv(tables / "trip_coverage.csv", [{
            "source_name": "scheduled_trips_endpoint",
            "trips_ingested": s["trips_ingested"],
            "trips_expected": s["declared_total"],
            "shortfall": s["shortfall"],
        }], ["source_name", "trips_ingested", "trips_expected", "shortfall"])

        # Request sequence with neutral column names (no mechanism vocabulary).
        seq = db.query(ctx.control,
                       "SELECT request_id, response_code, next_token FROM api_request_log "
                       "WHERE source_name = ? ORDER BY logical_time, request_id", (SOURCE,))
        rows = []
        for i, r in enumerate(seq, start=1):
            rows.append({"request_seq": i, "response_code": r["response_code"],
                         "chain_continues": "yes" if r["next_token"] is not None else "no",
                         "followed_by_next_request": "yes" if i < len(seq) else "no"})
        bc.write_csv(tables / "request_sequence.csv", rows,
                     ["request_seq", "response_code", "chain_continues", "followed_by_next_request"])

        bc.write_csv(tables / "row_counts.csv",
                     [{"dataset": "trips_ingested", "rows": s["trips_ingested"]},
                      {"dataset": "endpoint_control_total", "rows": s["declared_total"]}],
                     ["dataset", "rows"])
        bc.write_csv(tables / "quality_results.csv", bc.quality_results_rows(ctx),
                     ["check_id", "scope_key", "dimension", "expected", "actual", "status"])

        alert = bc.base_alert(
            "MP-08", self.metadata.reader_title,
            reported_symptom=(
                "The scheduled-trips dashboard is short by almost exactly one request's worth "
                "of trips every day. Every request to the source returned HTTP 200 and the "
                "ingest job reported success."),
            time_pressure="The operations dashboard is reviewed each morning.",
            affected_product="scheduled_trips_dashboard",
            extra={"observed": {"trips_ingested": s["trips_ingested"],
                                "trips_expected": s["declared_total"],
                                "shortfall": s["shortfall"]}})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", bc.sanitized_timeline(ctx, LEAK))
        write_json(ctx.paths.evidence_index, bc.evidence_index("MP-08", ctx.run_id, ev))
        return StepReport("evidence", "MP-08", ctx.run_id, "collected",
                          fields={"shortfall": s["shortfall"]})

    # -- contain / repair / recover ------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        bc.set_dataset_state(ctx, DATASET, "HELD", from_state="PUBLISHED")
        blast = {"held_dataset": DATASET, "trips_short": self._signals(ctx)["shortfall"],
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.now(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-08", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-08", ctx.run_id, "contained", fields=blast)

    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp08_faulty_loop"] = False
        profile["params"]["mp08_terminal_rule"] = "stop_only_on_null_continuation_marker"
        ctx.save_fault_profile(profile)
        report = StepReport("repair", "MP-08", ctx.run_id, "repaired",
                            fields={"change": "terminate the ingest loop only on the API's "
                                             "authoritative terminal marker; advance the cursor "
                                             "only after a full response persists"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    def recover(self, ctx: RunContext) -> StepReport:
        t = api.consume_correct(self._endpoint())
        self._store_trips(ctx, t.rows, replace=False)   # re-ingest the dropped final response
        self._record_request_log(ctx, t.request_log)
        self._set_state(ctx, t.reached_terminal, db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp08_trip"))
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-recover", DATASET, "service-date",
                                  "PUBLISHED", "PUB-recover")
        bc.record_outbox(ctx, OUTBOX_ID, "mp08.recovered",
                         {"dataset": DATASET, "trips": len(t.rows)})
        manifest = {
            "mode": "REPLAY",
            "scope": {"source": SOURCE, "datasets": ["mp08_trip"]},
            "source": "re-ingest the missing final response to the terminal condition",
            "write_mode": "append",
            "duplicate_protection": "primary key on trip_id; INSERT OR IGNORE",
            "validation": "request chain reaches the terminal marker; count equals control total",
            "trips_after": db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp08_trip"),
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.now(), component="recovery",
                     event_type="replay_completed", fields={"trips": manifest["trips_after"]})
        return StepReport("recover", "MP-08", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        expected_ids = {r["trip_id"] for r in api.consume_correct(self._endpoint()).rows}
        got_ids = {r["trip_id"] for r in db.query(ctx.warehouse, "SELECT trip_id FROM mp08_trip")}
        count = len(got_ids)
        terminal_reached = db.scalar(
            ctx.control, "SELECT COUNT(*) FROM api_request_log "
            "WHERE source_name = ? AND next_token IS NULL", (SOURCE,)) > 0
        dup = db.scalar(ctx.warehouse,
                        "SELECT COUNT(*) - COUNT(DISTINCT trip_id) FROM mp08_trip")
        changed = bc.shared_tables_changed(ctx)

        assertions = [
            Assertion("MP-08-VER-CORRECT", "correctness",
                      "ingested trip ids equal the full endpoint set",
                      expected=len(expected_ids), actual=count,
                      status="PASS" if got_ids == expected_ids else "FAIL",
                      evidence="reports/verification.json"),
            Assertion("MP-08-VER-COMPLETE", "completeness",
                      "ingested count equals the full-endpoint control total",
                      expected=TOTAL_ROWS, actual=count,
                      status="PASS" if count == TOTAL_ROWS else "FAIL"),
            Assertion("MP-08-VER-UNIQUE", "uniqueness",
                      "no duplicate trip_id", expected=0, actual=dup,
                      status="PASS" if dup == 0 else "FAIL"),
            Assertion("MP-08-VER-CONTROL", "control_consistency",
                      "request sequence reaches the source terminal condition",
                      expected=True, actual=terminal_reached,
                      status="PASS" if terminal_reached else "FAIL"),
            Assertion("MP-08-VER-BOUNDED", "boundedness",
                      "no shared warehouse table changed since the faulty run",
                      expected=[], actual=changed, status="PASS" if not changed else "FAIL"),
            Assertion("MP-08-VER-IDEMPOTENT", "idempotency",
                      "independent full traversal equals persisted trips",
                      expected=True, actual=(expected_ids == got_ids),
                      status="PASS" if expected_ids == got_ids else "FAIL"),
            Assertion("MP-08-VER-SIDEEFFECT", "side_effects",
                      "exactly one recovery notification emitted",
                      expected=1, actual=bc.outbox_count(ctx, OUTBOX_ID),
                      status="PASS" if bc.outbox_count(ctx, OUTBOX_ID) == 1 else "FAIL"),
        ]
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion("MP-08-VER-FIXTURE", "fixture_integrity",
                                    "run landing inputs match checked-in fixtures",
                                    expected=True, actual=fx_ok,
                                    status="PASS" if fx_ok else "FAIL",
                                    evidence=str(fx_detail.get("status"))))
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-08", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP08Incident()
