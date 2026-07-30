"""MP-06 — The 02:00 stop-event archive is present but its final records are missing.

Frozen mechanism (incident_taxonomy.md MP-06): the archive was cut off at the
tail during upload/copy. The consumer checked only that the object existed and
the reader returned the decodable leading records, so a partial artifact was
published as if complete. Object existence is not artifact completeness — the
end-of-stream marker, the size/CRC check, and the producer's declared
bytes/records/last-sequence must agree.

Reader-facing text never names the compression format or the cut. It describes
only the symptom: the object exists, holds most records, is only slightly smaller
than the producer declared, and its final batch of stop events is absent.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchB_common as bc
from . import _batchB_mp06_archive as arc
from .contract import IncidentMetadata

DATASET = "mp06_stop_event_archive"
DELIVERY_ID = "DLV-mp06-stop-batch"
OBJECT_NAME = "stop_event_batch_02_00"
OUTBOX_ID = "MSG-mp06-recovered"
FAULT_FLAG = "mp06_tail_cut"


class MP06Incident:
    metadata = IncidentMetadata(
        incident_id="MP-06",
        family="B. Missing or late",
        reader_title="The 02:00 stop-event archive is present but its final records are missing",
        primary_invariant_id="MP-06-ARTIFACT-COMPLETE",
        core_evidence_kind="REAL_SQLITE",
        recovery_mode="REPLAY",
        fault_operations=("truncate_completion_marker:mp06_tail_cut",),
        expected_detection_ids=("MP-06-COMPLETENESS-001", "MP-06-CONTROL-001"),
        transfer_mappings=(
            "Object-store upload checksums and atomic completion markers",
        ),
        nearest_old_scenario="Adjacent to a source outage or a downstream partial write.",
        old_scenario_difference=(
            "Not an upstream outage and not a downstream partial write: the object "
            "arrived and holds most records, but the artifact itself is short at its "
            "end and only its leading records were readable."
        ),
        affected_tables=("mp06_stop_event",),
    )

    # -- schema / baseline ----------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp06_stop_event ("
                "stop_event_id TEXT NOT NULL PRIMARY KEY, trip_id TEXT NOT NULL, "
                "station_id TEXT NOT NULL, event_type TEXT NOT NULL, "
                "occurred_at_utc TEXT NOT NULL, source_sequence INTEGER NOT NULL)")
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp06_archive_state ("
                "state_id INTEGER NOT NULL PRIMARY KEY, active_variant TEXT NOT NULL, "
                "object_bytes INTEGER NOT NULL, end_marker_reached INTEGER NOT NULL, "
                "integrity_ok INTEGER NOT NULL, records_readable INTEGER NOT NULL, "
                "decoded_bytes INTEGER NOT NULL)")
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp06_producer_manifest ("
                "manifest_id INTEGER NOT NULL PRIMARY KEY, expected_records INTEGER NOT NULL, "
                "expected_bytes INTEGER NOT NULL, last_sequence INTEGER NOT NULL, "
                "expected_crc32 INTEGER NOT NULL)")

    def _expected(self) -> dict[str, Any]:
        records = arc.stop_event_records()
        full = arc.build_complete_gzip(records)
        res = arc.read_archive(full)
        return {
            "records": records,
            "full_bytes": full,
            "expected_records": len(records),
            "expected_bytes": len(full),
            "last_sequence": records[-1]["source_sequence"],
            "expected_crc32": res.computed_crc32,
        }

    def _write_events(self, ctx: RunContext, records: list[dict[str, Any]]) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp06_stop_event")
            db.insert_rows(
                ctx.warehouse, "mp06_stop_event",
                ("stop_event_id", "trip_id", "station_id", "event_type",
                 "occurred_at_utc", "source_sequence"),
                [(r["stop_event_id"], r["trip_id"], r["station_id"], r["event_type"],
                  r["occurred_at_utc"], r["source_sequence"]) for r in records])

    def _set_archive_state(self, ctx: RunContext, variant: str, res: "arc.ArchiveReadResult",
                           object_bytes: int) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp06_archive_state")
            ctx.warehouse.execute(
                "INSERT INTO mp06_archive_state (state_id, active_variant, object_bytes, "
                "end_marker_reached, integrity_ok, records_readable, decoded_bytes) "
                "VALUES (1, ?, ?, ?, ?, ?, ?)",
                (variant, object_bytes, int(res.end_marker_reached), int(res.integrity_ok),
                 len(res.records), res.decoded_bytes))

    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        exp = self._expected()
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp06_producer_manifest")
            ctx.warehouse.execute(
                "INSERT INTO mp06_producer_manifest (manifest_id, expected_records, "
                "expected_bytes, last_sequence, expected_crc32) VALUES (1, ?, ?, ?, ?)",
                (exp["expected_records"], exp["expected_bytes"], exp["last_sequence"],
                 exp["expected_crc32"]))
        # Producer-declared completeness in the control plane.
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO ingestion_manifest "
                "(delivery_id, source_name, object_path, checksum, record_count, byte_count, "
                " completeness_token, commit_state) VALUES (?, ?, ?, ?, ?, ?, 'COMPLETE', 'COMMITTED')",
                (DELIVERY_ID, "mp06_stop_event", OBJECT_NAME, format(exp["expected_crc32"], "08x"),
                 exp["expected_records"], exp["expected_bytes"]))
        # Clean state: complete archive read, all records present.
        res = arc.read_archive(exp["full_bytes"])
        self._write_events(ctx, res.records)
        self._set_archive_state(ctx, "complete", res, exp["expected_bytes"])
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-baseline", DATASET, "batch-0200",
                                  "PUBLISHED", "PUB-baseline")
        ctx.log.emit(logical_time=ctx.clock.now(), component="ingest",
                     event_type="archive_ingested",
                     fields={"object": OBJECT_NAME, "records": len(res.records)})

    def baseline_ok(self, ctx: RunContext) -> bool:
        exp = db.scalar(ctx.warehouse, "SELECT expected_records FROM mp06_producer_manifest")
        got = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp06_stop_event")
        integ = db.scalar(ctx.warehouse, "SELECT integrity_ok FROM mp06_archive_state")
        return got == exp and integ == 1

    # -- faulty materialization ----------------------------------------------
    def _build_faulty(self, ctx: RunContext) -> dict[str, Any]:
        exp = self._expected()
        partial = arc.truncate_tail(exp["full_bytes"])
        res = arc.read_archive(partial)
        self._write_events(ctx, res.records)
        self._set_archive_state(ctx, "partial", res, len(partial))
        return {"readable": len(res.records), "expected": exp["expected_records"],
                "object_bytes": len(partial), "expected_bytes": exp["expected_bytes"]}

    def inject(self, ctx: RunContext) -> StepReport:
        exp_records = db.scalar(ctx.warehouse, "SELECT expected_records FROM mp06_producer_manifest")
        before = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp06_stop_event")
        pre_ok = (before == exp_records)
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})[FAULT_FLAG] = True
        ctx.save_fault_profile(profile)

        result = self._build_faulty(ctx)
        after = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp06_stop_event")

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-06-INJECT-PRE", "precondition",
                      "baseline holds the full declared record set",
                      expected=exp_records, actual=before,
                      status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-06-INJECT-POST", "postcondition",
                      "faulty object is short of the declared record count",
                      expected=f"<{exp_records}", actual=after,
                      status="PASS" if after < exp_records else "FAIL"),
            Assertion("MP-06-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.at_offset(60), component="incident",
                     event_type="fault_injected", fields={"records_short": exp_records - after})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-06", ctx.run_id, status,
                            fields={"records_readable": result["readable"],
                                    "records_expected": result["expected"],
                                    "object_bytes": result["object_bytes"],
                                    "expected_bytes": result["expected_bytes"],
                                    "fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        result = self._build_faulty(ctx)
        return StepReport("run", "MP-06", ctx.run_id, "executed", fields=result)

    # -- detect ---------------------------------------------------------------
    def _signals(self, ctx: RunContext) -> dict[str, Any]:
        man = db.query_one(ctx.warehouse,
                           "SELECT expected_records, expected_bytes, last_sequence "
                           "FROM mp06_producer_manifest")
        st = db.query_one(ctx.warehouse,
                          "SELECT object_bytes, integrity_ok, records_readable, end_marker_reached "
                          "FROM mp06_archive_state")
        readable = db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp06_stop_event")
        last_seq = db.scalar(ctx.warehouse, "SELECT MAX(source_sequence) FROM mp06_stop_event") or 0
        data_gap = readable < man["expected_records"]
        control_gap = (st["integrity_ok"] == 0 or st["object_bytes"] != man["expected_bytes"]
                       or readable != man["expected_records"])
        return {"expected_records": man["expected_records"], "readable": readable,
                "last_sequence_readable": last_seq, "expected_last_sequence": man["last_sequence"],
                "object_bytes": st["object_bytes"], "expected_bytes": man["expected_bytes"],
                "integrity_ok": bool(st["integrity_ok"]), "data_gap": data_gap,
                "control_gap": control_gap}

    def detect(self, ctx: RunContext) -> StepReport:
        s = self._signals(ctx)
        bc.write_quality_result(
            ctx, "MP-06-COMPLETENESS-001", "completeness",
            expected=f"{s['expected_records']} stop events per producer manifest",
            actual=f"{s['readable']} stop events present",
            status="FAIL" if s["data_gap"] else "PASS")
        bc.write_quality_result(
            ctx, "MP-06-CONTROL-001", "control_consistency",
            expected="object size and integrity check agree with the producer manifest",
            actual=("object integrity check fails and its size is below the declared size"
                    if s["control_gap"] else "object matches the producer manifest"),
            status="FAIL" if s["control_gap"] else "PASS")

        assertions = [
            Assertion("MP-06-DET-COMPLETE", "completeness",
                      "present stop-event count matches the producer manifest",
                      expected=s["expected_records"], actual=s["readable"],
                      status="FAIL" if s["data_gap"] else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-06-DET-CONTROL", "control_consistency",
                      "object size and integrity check agree with the producer manifest",
                      expected=False, actual=s["control_gap"],
                      status="FAIL" if s["control_gap"] else "PASS",
                      evidence="reports/detection.json"),
        ]
        detected = s["data_gap"] or s["control_gap"]
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-06", "run_id": ctx.run_id, "fields": s,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-06", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=s, assertions=assertions)

    # -- evidence (read-only) -------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        s = self._signals(ctx)

        bc.write_csv(tables / "archive_object.csv", [{
            "object_name": OBJECT_NAME,
            "object_bytes": s["object_bytes"],
            "producer_declared_bytes": s["expected_bytes"],
            "records_readable": s["readable"],
            "producer_declared_records": s["expected_records"],
            "last_record_sequence_readable": s["last_sequence_readable"],
            "producer_last_sequence": s["expected_last_sequence"],
            "integrity_check": "PASS" if s["integrity_ok"] else "FAIL",
        }], ["object_name", "object_bytes", "producer_declared_bytes", "records_readable",
             "producer_declared_records", "last_record_sequence_readable",
             "producer_last_sequence", "integrity_check"])

        present = db.query(ctx.warehouse,
                           "SELECT stop_event_id, trip_id, station_id, event_type, "
                           "occurred_at_utc, source_sequence FROM mp06_stop_event "
                           "ORDER BY source_sequence, stop_event_id")
        bc.write_csv(tables / "stop_events_present.csv", db.rows_to_dicts(present),
                     ["source_sequence", "stop_event_id", "trip_id", "station_id",
                      "event_type", "occurred_at_utc"])

        man = db.rows_to_dicts(db.query(ctx.control,
            "SELECT delivery_id, source_name, record_count, byte_count, commit_state "
            "FROM ingestion_manifest WHERE delivery_id = ? ORDER BY delivery_id", (DELIVERY_ID,)))
        for row in man:
            row["declared_records"] = row.pop("record_count")
            row["declared_bytes"] = row.pop("byte_count")
        bc.write_csv(tables / "producer_manifest.csv", man,
                     ["delivery_id", "source_name", "declared_records", "declared_bytes",
                      "commit_state"])

        bc.write_csv(tables / "row_counts.csv",
                     [{"dataset": "stop_events_present", "rows": s["readable"]},
                      {"dataset": "producer_declared", "rows": s["expected_records"]}],
                     ["dataset", "rows"])
        bc.write_csv(tables / "quality_results.csv", bc.quality_results_rows(ctx),
                     ["check_id", "scope_key", "dimension", "expected", "actual", "status"])

        alert = bc.base_alert(
            "MP-06", self.metadata.reader_title,
            reported_symptom=(
                "The 02:00 stop-event archive object exists and holds most records, but the "
                "final batch of stop events is absent and the object is only slightly smaller "
                "than the size the producer declared. The daily job reported success."),
            time_pressure="The morning service report depends on the 02:00 batch.",
            affected_product="stop_events_present",
            extra={"observed": {"records_present": s["readable"],
                                "producer_declared_records": s["expected_records"],
                                "object_bytes": s["object_bytes"],
                                "producer_declared_bytes": s["expected_bytes"],
                                "integrity_check": "FAIL" if not s["integrity_ok"] else "PASS"}})
        write_json(ev / "alert.json", alert)
        leak = ("truncat", "gzip", "crc", "trailer", "incomplet", "prefix", "tail")
        write_json(ev / "timeline.json", bc.sanitized_timeline(ctx, leak))
        write_json(ctx.paths.evidence_index, bc.evidence_index("MP-06", ctx.run_id, ev))
        return StepReport("evidence", "MP-06", ctx.run_id, "collected",
                          fields={"records_short": s["expected_records"] - s["readable"]})

    # -- contain / repair / recover ------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        bc.set_dataset_state(ctx, DATASET, "HELD", from_state="PUBLISHED")
        blast = {"held_dataset": DATASET, "records_short": self._signals(ctx)["expected_records"]
                 - self._signals(ctx)["readable"], "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.now(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-06", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-06", ctx.run_id, "contained", fields=blast)

    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})[FAULT_FLAG] = False
        profile.setdefault("params", {})["mp06_completeness_gate"] = "verify_end_marker_and_manifest"
        ctx.save_fault_profile(profile)
        report = StepReport("repair", "MP-06", ctx.run_id, "repaired",
                            fields={"change": "verify the artifact's end-of-stream marker, "
                                             "size and record checksum against the producer "
                                             "manifest before publishing"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    def recover(self, ctx: RunContext) -> StepReport:
        exp = self._expected()
        res = arc.read_archive(exp["full_bytes"])
        if not res.integrity_ok:
            raise RuntimeError("re-fetched artifact did not validate")
        self._write_events(ctx, res.records)
        self._set_archive_state(ctx, "complete", res, exp["expected_bytes"])
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-recover", DATASET, "batch-0200",
                                  "PUBLISHED", "PUB-recover")
        bc.record_outbox(ctx, OUTBOX_ID, "mp06.recovered",
                         {"dataset": DATASET, "records": len(res.records)})
        manifest = {
            "mode": "REPLAY",
            "scope": {"batch": OBJECT_NAME, "datasets": ["mp06_stop_event"]},
            "source": "complete artifact re-fetched from the producer",
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key on stop_event_id; deterministic rebuild",
            "validation": "end-of-stream marker + size + record checksum vs producer manifest",
            "records_restored": len(res.records),
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.now(), component="recovery",
                     event_type="replay_completed", fields={"records": len(res.records)})
        return StepReport("recover", "MP-06", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        exp = self._expected()
        expected_ids = {r["stop_event_id"] for r in exp["records"]}
        rows = db.query(ctx.warehouse, "SELECT stop_event_id, source_sequence FROM mp06_stop_event")
        got_ids = {r["stop_event_id"] for r in rows}
        st = db.query_one(ctx.warehouse,
                          "SELECT object_bytes, integrity_ok FROM mp06_archive_state")
        last_seq = db.scalar(ctx.warehouse, "SELECT MAX(source_sequence) FROM mp06_stop_event") or 0
        dup = db.scalar(ctx.warehouse,
                        "SELECT COUNT(*) - COUNT(DISTINCT stop_event_id) FROM mp06_stop_event")
        changed = bc.shared_tables_changed(ctx)
        # independent recompute straight from the re-read complete artifact
        recomputed = {r["stop_event_id"] for r in arc.read_archive(exp["full_bytes"]).records}

        assertions = [
            Assertion("MP-06-VER-CORRECT", "correctness",
                      "restored stop events equal the full declared set",
                      expected=len(expected_ids), actual=len(got_ids),
                      status="PASS" if got_ids == expected_ids else "FAIL",
                      evidence="reports/verification.json"),
            Assertion("MP-06-VER-COMPLETE", "completeness",
                      "the final declared sequence is present",
                      expected=exp["last_sequence"], actual=last_seq,
                      status="PASS" if last_seq == exp["last_sequence"] else "FAIL"),
            Assertion("MP-06-VER-UNIQUE", "uniqueness",
                      "no duplicate stop_event_id", expected=0, actual=dup,
                      status="PASS" if dup == 0 else "FAIL"),
            Assertion("MP-06-VER-CONTROL", "control_consistency",
                      "object size matches producer bytes and integrity check passes",
                      expected=exp["expected_bytes"],
                      actual={"object_bytes": st["object_bytes"], "integrity_ok": st["integrity_ok"]},
                      status="PASS" if (st["object_bytes"] == exp["expected_bytes"]
                                        and st["integrity_ok"] == 1) else "FAIL"),
            Assertion("MP-06-VER-BOUNDED", "boundedness",
                      "no shared warehouse table changed since the faulty run",
                      expected=[], actual=changed, status="PASS" if not changed else "FAIL"),
            Assertion("MP-06-VER-IDEMPOTENT", "idempotency",
                      "independent recompute equals persisted records",
                      expected=True, actual=(recomputed == got_ids),
                      status="PASS" if recomputed == got_ids else "FAIL"),
            Assertion("MP-06-VER-SIDEEFFECT", "side_effects",
                      "exactly one recovery notification emitted",
                      expected=1, actual=bc.outbox_count(ctx, OUTBOX_ID),
                      status="PASS" if bc.outbox_count(ctx, OUTBOX_ID) == 1 else "FAIL"),
        ]
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion("MP-06-VER-FIXTURE", "fixture_integrity",
                                    "run landing inputs match checked-in fixtures",
                                    expected=True, actual=fx_ok,
                                    status="PASS" if fx_ok else "FAIL",
                                    evidence=str(fx_detail.get("status"))))
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-06", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP06Incident()
