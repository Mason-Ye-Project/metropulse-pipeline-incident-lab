"""MP-10 — After one bad record, later normal trips never enter the warehouse.

Frozen mechanism (incident_taxonomy.md MP-10): a single record with an impossible
far-future timestamp is committed as the incremental progress marker. Because the
marker jumps far ahead, every subsequent normal row falls below it and is never
selected again, so later trips never enter the warehouse while each run still
succeeds with zero new rows.

The marker must advance only from validated records inside a sane forward bound,
using a composite (timestamp, stable id) so ties are not lost. Reader-facing text
describes only the symptom and never prints the impossible value; out-of-range
timestamps are masked so the reader must discover the anomaly.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchB_common as bc
from .contract import IncidentMetadata

DATASET = "mp10_trip_warehouse"
SOURCE = "mp10_trips"
OUTBOX_ID = "MSG-mp10-recovered"
# A sane forward bound for accepting a record's timestamp. A checked-in constant,
# never the host clock. Anything beyond this is not a plausible service instant.
FORWARD_BOUND = "2026-05-01T00:00:00Z"
INITIAL_MARKER = "2026-04-01T00:00:00Z"
RANGE_MASK = "(outside accepted range)"
LEAK = ("cursor", "watermark", "future", "2099", "poison", "monotonic")


def source_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for i, h in enumerate((6, 7, 8, 9)):
        rows.append({"stable_id": f"TRIP_A{i + 1:02d}", "updated_at": f"2026-04-13T{h:02d}:00:00Z",
                     "service_date": "2026-04-13", "batch": "A"})
    for i, h in enumerate((6, 7, 8)):
        rows.append({"stable_id": f"TRIP_B{i + 1:02d}", "updated_at": f"2026-04-14T{h:02d}:00:00Z",
                     "service_date": "2026-04-14", "batch": "B"})
    # The single bad record: an impossible far-future timestamp.
    rows.append({"stable_id": "TRIP_B99", "updated_at": "2099-01-01T00:00:00Z",
                 "service_date": "2026-04-14", "batch": "B"})
    for i, h in enumerate((6, 7, 8, 9)):
        rows.append({"stable_id": f"TRIP_C{i + 1:02d}", "updated_at": f"2026-04-15T{h:02d}:00:00Z",
                     "service_date": "2026-04-15", "batch": "C"})
    return rows


def _normal_rows() -> list[dict[str, Any]]:
    return [r for r in source_rows() if r["updated_at"] <= FORWARD_BOUND]


def _batch_a_max() -> str:
    return max(r["updated_at"] for r in source_rows() if r["batch"] == "A")


class MP10Incident:
    metadata = IncidentMetadata(
        incident_id="MP-10",
        family="B. Missing or late",
        reader_title="After one bad record, later normal trips never enter the warehouse although the job keeps succeeding",
        primary_invariant_id="MP-10-PROGRESS-BOUND",
        core_evidence_kind="REAL_SQLITE",
        recovery_mode="REPLAY",
        fault_operations=("commit_unbounded_progress_marker:mp10",),
        expected_detection_ids=("MP-10-FRESHNESS-001", "MP-10-CONTROL-001"),
        transfer_mappings=("dbt / warehouse incremental model bookmarks",),
        nearest_old_scenario="Adjacent to a past-dated late-data scenario.",
        old_scenario_difference=(
            "Not a past-dated late arrival: an impossible forward-dated value pollutes "
            "the control-plane progress marker, so later valid rows are never selected "
            "even though the job keeps succeeding."
        ),
        affected_tables=("mp10_sink",),
    )

    # -- schema / baseline ----------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp10_source_row ("
                "stable_id TEXT NOT NULL PRIMARY KEY, updated_at TEXT NOT NULL, "
                "service_date TEXT NOT NULL, batch TEXT NOT NULL)")
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp10_sink ("
                "stable_id TEXT NOT NULL PRIMARY KEY, updated_at TEXT NOT NULL, "
                "service_date TEXT NOT NULL)")
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp10_quarantine ("
                "stable_id TEXT NOT NULL PRIMARY KEY, updated_at TEXT NOT NULL, "
                "service_date TEXT NOT NULL, reason TEXT NOT NULL)")
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp10_progress ("
                "seq INTEGER NOT NULL PRIMARY KEY, marker_value TEXT NOT NULL, "
                "within_bound INTEGER NOT NULL, batch TEXT NOT NULL, rows_selected INTEGER NOT NULL)")

    def _read_marker(self, ctx: RunContext) -> str:
        val = db.scalar(ctx.control,
                        "SELECT cursor_value FROM source_cursor WHERE source_name = ?", (SOURCE,))
        return val if val is not None else INITIAL_MARKER

    def _commit_marker(self, ctx: RunContext, value: str) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO source_cursor (source_name, cursor_value, "
                "committed_position, updated_logical_time) VALUES (?, ?, ?, ?)",
                (SOURCE, value, value, ctx.clock.at_offset(0)))

    def _record_progress(self, ctx: RunContext, seq: int, marker: str, batch: str, selected: int) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "INSERT OR REPLACE INTO mp10_progress (seq, marker_value, within_bound, batch, rows_selected) "
                "VALUES (?, ?, ?, ?, ?)",
                (seq, marker, int(marker <= FORWARD_BOUND), batch, selected))

    def _insert_sink(self, ctx: RunContext, rows: list[dict[str, Any]]) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.executemany(
                "INSERT OR IGNORE INTO mp10_sink (stable_id, updated_at, service_date) "
                "VALUES (?, ?, ?)",
                [(r["stable_id"], r["updated_at"], r["service_date"]) for r in rows])

    def _run_batch(self, ctx: RunContext, batch: str, seq: int, faulty: bool) -> dict[str, Any]:
        marker = self._read_marker(ctx)
        candidates = sorted([r for r in source_rows() if r["batch"] == batch
                             and r["updated_at"] > marker],
                            key=lambda r: (r["updated_at"], r["stable_id"]))
        if faulty:
            accepted = candidates                      # no forward bound
            new_marker = max((r["updated_at"] for r in candidates), default=marker)
        else:
            accepted = [r for r in candidates if r["updated_at"] <= FORWARD_BOUND]
            quarantined = [r for r in candidates if r["updated_at"] > FORWARD_BOUND]
            with db.transaction(ctx.warehouse):
                for r in quarantined:
                    ctx.warehouse.execute(
                        "INSERT OR REPLACE INTO mp10_quarantine (stable_id, updated_at, service_date, reason) "
                        "VALUES (?, ?, ?, 'timestamp beyond accepted forward bound')",
                        (r["stable_id"], r["updated_at"], r["service_date"]))
            new_marker = max((r["updated_at"] for r in accepted), default=marker)
        self._insert_sink(ctx, accepted)
        self._commit_marker(ctx, new_marker)
        self._record_progress(ctx, seq, new_marker, batch, len(accepted))
        return {"batch": batch, "selected": len(accepted), "new_marker": new_marker}

    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp10_source_row")
            db.insert_rows(ctx.warehouse, "mp10_source_row",
                           ("stable_id", "updated_at", "service_date", "batch"),
                           [(r["stable_id"], r["updated_at"], r["service_date"], r["batch"])
                            for r in source_rows()])
            ctx.warehouse.execute("DELETE FROM mp10_sink")
            ctx.warehouse.execute("DELETE FROM mp10_quarantine")
            ctx.warehouse.execute("DELETE FROM mp10_progress")
        self._commit_marker(ctx, INITIAL_MARKER)
        self._run_batch(ctx, "A", seq=1, faulty=False)   # healthy incremental run
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-baseline", DATASET, "incremental",
                                  "PUBLISHED", "PUB-baseline")
        ctx.log.emit(logical_time=ctx.clock.now(), component="ingest",
                     event_type="incremental_run", fields={"batch": "A"})

    def baseline_ok(self, ctx: RunContext) -> bool:
        marker = self._read_marker(ctx)
        sink = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp10_sink")
        out_of_range = db.scalar(ctx.warehouse,
                                 "SELECT COUNT(*) FROM mp10_sink WHERE updated_at > ?", (FORWARD_BOUND,))
        batch_a = sum(1 for r in source_rows() if r["batch"] == "A")
        return marker <= FORWARD_BOUND and sink == batch_a and out_of_range == 0

    # -- faulty ---------------------------------------------------------------
    def _apply_faulty(self, ctx: RunContext) -> dict[str, Any]:
        # Two scheduled runs under the faulty rule: batch B commits the bad marker,
        # then batch C selects nothing because the marker jumped far ahead.
        b = self._run_batch(ctx, "B", seq=2, faulty=True)
        c = self._run_batch(ctx, "C", seq=3, faulty=True)
        return {"batch_b_selected": b["selected"], "batch_c_selected": c["selected"],
                "marker_after": self._read_marker(ctx)}

    def inject(self, ctx: RunContext) -> StepReport:
        before_marker = self._read_marker(ctx)
        pre_ok = before_marker <= FORWARD_BOUND
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp10_unbounded_marker"] = True
        ctx.save_fault_profile(profile)

        result = self._apply_faulty(ctx)
        marker_after = self._read_marker(ctx)
        c_in_sink = db.scalar(ctx.warehouse,
                              "SELECT COUNT(*) FROM mp10_sink WHERE service_date = '2026-04-15'")
        post_ok = (marker_after > FORWARD_BOUND) and (c_in_sink == 0)

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, _ = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-10-INJECT-PRE", "precondition",
                      "baseline progress marker is within the accepted forward bound",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-10-INJECT-POST", "postcondition",
                      "marker jumped past the bound and later trips are absent",
                      expected=True, actual=post_ok, status="PASS" if post_ok else "FAIL"),
            Assertion("MP-10-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.at_offset(60), component="incident",
                     event_type="fault_injected", fields={"later_trips_selected": result["batch_c_selected"]})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-10", ctx.run_id, status,
                            fields={"batch_c_selected": result["batch_c_selected"],
                                    "fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        # The "next scheduled run" for the latest batch still selects nothing.
        res = self._run_batch(ctx, "C", seq=3, faulty=True)
        return StepReport("run", "MP-10", ctx.run_id, "executed",
                          fields={"batch": "C", "rows_selected": res["selected"]})

    # -- detect ---------------------------------------------------------------
    def _signals(self, ctx: RunContext) -> dict[str, Any]:
        marker = self._read_marker(ctx)
        source_dates = sorted({r["service_date"] for r in _normal_rows()})
        sink_dates = {r["service_date"] for r in db.query(
            ctx.warehouse, "SELECT DISTINCT service_date FROM mp10_sink")}
        missing_dates = [d for d in source_dates if d not in sink_dates]
        latest_source = source_dates[-1]
        latest_in_sink = latest_source in sink_dates
        marker_in_bound = marker <= FORWARD_BOUND
        return {"marker_in_bound": marker_in_bound, "missing_service_dates": missing_dates,
                "latest_source_date": latest_source, "latest_in_warehouse": latest_in_sink,
                "data_gap": bool(missing_dates), "control_gap": not marker_in_bound}

    def detect(self, ctx: RunContext) -> StepReport:
        s = self._signals(ctx)
        bc.write_quality_result(
            ctx, "MP-10-FRESHNESS-001", "freshness",
            expected=f"warehouse covers the latest source service day {s['latest_source_date']}",
            actual=("the latest source service day never entered the warehouse"
                    if not s["latest_in_warehouse"] else "warehouse is current"),
            status="FAIL" if s["data_gap"] else "PASS")
        bc.write_quality_result(
            ctx, "MP-10-CONTROL-001", "control_consistency",
            expected="the committed progress marker stays within the accepted forward bound",
            actual=("the committed progress marker is beyond the accepted forward bound"
                    if s["control_gap"] else "the progress marker is within bound"),
            status="FAIL" if s["control_gap"] else "PASS")

        assertions = [
            Assertion("MP-10-DET-FRESH", "freshness",
                      "latest source service day is present in the warehouse",
                      expected=True, actual=s["latest_in_warehouse"],
                      status="FAIL" if s["data_gap"] else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-10-DET-CONTROL", "control_consistency",
                      "committed progress marker is within the accepted forward bound",
                      expected=True, actual=s["marker_in_bound"],
                      status="FAIL" if s["control_gap"] else "PASS",
                      evidence="reports/detection.json"),
        ]
        detected = s["data_gap"] or s["control_gap"]
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-10", "run_id": ctx.run_id, "fields": s,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-10", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=s, assertions=assertions)

    # -- evidence (masks out-of-range timestamps) -----------------------------
    def _display_ts(self, ts: str) -> str:
        return ts if ts <= FORWARD_BOUND else RANGE_MASK

    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        # Source vs warehouse coverage per service day.
        cov_rows = []
        for date in sorted({r["service_date"] for r in _normal_rows()}):
            src = sum(1 for r in _normal_rows() if r["service_date"] == date)
            wh = db.scalar(ctx.warehouse,
                           "SELECT COUNT(*) FROM mp10_sink WHERE service_date = ?", (date,))
            cov_rows.append({"service_date": date, "source_rows": src, "warehouse_rows": wh,
                             "gap": src - wh})
        bc.write_csv(tables / "source_vs_warehouse.csv", cov_rows,
                     ["service_date", "source_rows", "warehouse_rows", "gap"])

        # Progress-marker history; the impossible value is masked.
        prog = db.query(ctx.warehouse,
                        "SELECT seq, marker_value, within_bound, rows_selected FROM mp10_progress "
                        "ORDER BY seq")
        bc.write_csv(tables / "progress_marker_history.csv",
                     [{"step": p["seq"], "progress_marker": self._display_ts(p["marker_value"]),
                       "marker_in_accepted_range": "yes" if p["within_bound"] else "no",
                       "rows_selected": p["rows_selected"]} for p in prog],
                     ["step", "progress_marker", "marker_in_accepted_range", "rows_selected"])

        # Raw source rows with out-of-range timestamps masked so the reader can
        # spot the one anomalous record without being handed the answer.
        src_rows = db.query(ctx.warehouse,
                            "SELECT stable_id, service_date, updated_at FROM mp10_source_row "
                            "ORDER BY updated_at, stable_id")
        bc.write_csv(tables / "source_rows.csv",
                     [{"stable_id": r["stable_id"], "service_date": r["service_date"],
                       "updated_at": self._display_ts(r["updated_at"]),
                       "updated_at_in_range": "yes" if r["updated_at"] <= FORWARD_BOUND else "no"}
                      for r in src_rows],
                     ["stable_id", "service_date", "updated_at", "updated_at_in_range"])
        bc.write_csv(tables / "quality_results.csv", bc.quality_results_rows(ctx),
                     ["check_id", "scope_key", "dimension", "expected", "actual", "status"])

        s = self._signals(ctx)
        alert = bc.base_alert(
            "MP-10", self.metadata.reader_title,
            reported_symptom=(
                "After one bad record was ingested, later normal trips stopped entering the "
                "warehouse entirely. Each incremental run still reports success and selects "
                "zero new rows, so the latest service day is missing."),
            time_pressure="Downstream reports assume the warehouse is current each morning.",
            affected_product="trip_warehouse",
            extra={"observed": {"missing_service_dates": s["missing_service_dates"],
                                "latest_source_date": s["latest_source_date"]}})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", bc.sanitized_timeline(ctx, LEAK))
        write_json(ctx.paths.evidence_index, bc.evidence_index("MP-10", ctx.run_id, ev))
        return StepReport("evidence", "MP-10", ctx.run_id, "collected",
                          fields={"missing_service_dates": s["missing_service_dates"]})

    # -- contain / repair / recover ------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        bc.set_dataset_state(ctx, DATASET, "HELD", from_state="PUBLISHED")
        s = self._signals(ctx)
        blast = {"held_dataset": DATASET, "missing_service_dates": s["missing_service_dates"],
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.now(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-10", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-10", ctx.run_id, "contained", fields=blast)

    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp10_unbounded_marker"] = False
        profile["params"]["mp10_marker_rule"] = "forward_bound_and_composite_marker"
        ctx.save_fault_profile(profile)
        report = StepReport("repair", "MP-10", ctx.run_id, "repaired",
                            fields={"change": "roll the progress marker back to the last validated "
                                             "value, quarantine the impossible forward-dated row, and "
                                             "re-read a bounded interval with a composite (updated_at, "
                                             "stable_id) marker"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    def recover(self, ctx: RunContext) -> StepReport:
        # REPLAY a bounded interval from the last validated marker. Rebuild the sink
        # from every validated row, quarantine the impossible record, and commit the
        # last validated marker with a composite (updated_at, stable_id) rule.
        last_validated = _batch_a_max()
        self._commit_marker(ctx, last_validated)
        candidates = sorted([r for r in source_rows() if r["updated_at"] > last_validated],
                            key=lambda r: (r["updated_at"], r["stable_id"]))
        accepted = [r for r in candidates if r["updated_at"] <= FORWARD_BOUND]
        quarantined = [r for r in candidates if r["updated_at"] > FORWARD_BOUND]
        all_valid = _normal_rows()
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp10_sink")
            ctx.warehouse.execute("DELETE FROM mp10_quarantine")
            for r in all_valid:
                ctx.warehouse.execute(
                    "INSERT INTO mp10_sink (stable_id, updated_at, service_date) VALUES (?, ?, ?)",
                    (r["stable_id"], r["updated_at"], r["service_date"]))
            for r in quarantined:
                ctx.warehouse.execute(
                    "INSERT INTO mp10_quarantine (stable_id, updated_at, service_date, reason) "
                    "VALUES (?, ?, ?, 'timestamp beyond accepted forward bound')",
                    (r["stable_id"], r["updated_at"], r["service_date"]))
        final_marker = max(r["updated_at"] for r in all_valid)
        self._commit_marker(ctx, final_marker)
        self._record_progress(ctx, seq=100, marker=final_marker, batch="recover", selected=len(accepted))
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-recover", DATASET, "incremental",
                                  "PUBLISHED", "PUB-recover")
        bc.record_outbox(ctx, OUTBOX_ID, "mp10.recovered",
                         {"dataset": DATASET, "accepted": len(all_valid), "quarantined": len(quarantined)})
        manifest = {
            "mode": "REPLAY",
            "scope": {"source": SOURCE, "interval": f"({last_validated}, {FORWARD_BOUND}]",
                      "datasets": ["mp10_sink"]},
            "source": "bounded re-read from the last validated marker with a composite key",
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key on stable_id; deterministic rebuild",
            "validation": "accepted + quarantined = candidates; marker within forward bound",
            "accepted": len(all_valid), "quarantined": len(quarantined),
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.now(), component="recovery",
                     event_type="replay_completed",
                     fields={"accepted": len(all_valid), "quarantined": len(quarantined)})
        return StepReport("recover", "MP-10", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        valid_ids = {r["stable_id"] for r in _normal_rows()}
        sink_ids = {r["stable_id"] for r in db.query(ctx.warehouse, "SELECT stable_id FROM mp10_sink")}
        out_of_range_in_sink = db.scalar(ctx.warehouse,
                                         "SELECT COUNT(*) FROM mp10_sink WHERE updated_at > ?",
                                         (FORWARD_BOUND,))
        quarantined = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp10_quarantine")
        total_source = len(source_rows())
        accounting_closes = (len(sink_ids) + quarantined) == total_source
        marker = self._read_marker(ctx)
        latest_source = max(r["service_date"] for r in _normal_rows())
        latest_in_sink = db.scalar(ctx.warehouse,
                                   "SELECT COUNT(*) FROM mp10_sink WHERE service_date = ?",
                                   (latest_source,)) > 0
        dup = db.scalar(ctx.warehouse, "SELECT COUNT(*) - COUNT(DISTINCT stable_id) FROM mp10_sink")
        changed = bc.shared_tables_changed(ctx)

        assertions = [
            Assertion("MP-10-VER-CORRECT", "correctness",
                      "warehouse holds exactly the validated rows and none out of range",
                      expected={"ids": len(valid_ids), "out_of_range": 0},
                      actual={"ids": len(sink_ids), "out_of_range": out_of_range_in_sink},
                      status="PASS" if (sink_ids == valid_ids and out_of_range_in_sink == 0) else "FAIL",
                      evidence="reports/verification.json"),
            Assertion("MP-10-VER-COMPLETE", "completeness",
                      "accepted + quarantined accounts for every source row",
                      expected=total_source, actual=len(sink_ids) + quarantined,
                      status="PASS" if accounting_closes else "FAIL"),
            Assertion("MP-10-VER-UNIQUE", "uniqueness",
                      "no duplicate stable_id in the warehouse", expected=0, actual=dup,
                      status="PASS" if dup == 0 else "FAIL"),
            Assertion("MP-10-VER-FRESH", "freshness",
                      "latest source service day is present and the marker is within bound",
                      expected=True, actual=(latest_in_sink and marker <= FORWARD_BOUND),
                      status="PASS" if (latest_in_sink and marker <= FORWARD_BOUND) else "FAIL"),
            Assertion("MP-10-VER-HISTORY", "history",
                      "the impossible forward-dated row is quarantined, not warehoused",
                      expected=1, actual=quarantined,
                      status="PASS" if (quarantined == 1 and out_of_range_in_sink == 0) else "FAIL"),
            Assertion("MP-10-VER-CONTROL", "control_consistency",
                      "committed marker equals the last validated timestamp within bound",
                      expected=max(r["updated_at"] for r in _normal_rows()), actual=marker,
                      status="PASS" if marker == max(r["updated_at"] for r in _normal_rows()) else "FAIL"),
            Assertion("MP-10-VER-BOUNDED", "boundedness",
                      "no shared warehouse table changed since the faulty run",
                      expected=[], actual=changed, status="PASS" if not changed else "FAIL"),
            Assertion("MP-10-VER-IDEMPOTENT", "idempotency",
                      "recomputed validated id set equals persisted sink",
                      expected=True, actual=(valid_ids == sink_ids),
                      status="PASS" if valid_ids == sink_ids else "FAIL"),
            Assertion("MP-10-VER-SIDEEFFECT", "side_effects",
                      "exactly one recovery notification emitted",
                      expected=1, actual=bc.outbox_count(ctx, OUTBOX_ID),
                      status="PASS" if bc.outbox_count(ctx, OUTBOX_ID) == 1 else "FAIL"),
        ]
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion("MP-10-VER-FIXTURE", "fixture_integrity",
                                    "run landing inputs match checked-in fixtures",
                                    expected=True, actual=fx_ok,
                                    status="PASS" if fx_ok else "FAIL",
                                    evidence=str(fx_detail.get("status"))))
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-10", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP10Incident()
