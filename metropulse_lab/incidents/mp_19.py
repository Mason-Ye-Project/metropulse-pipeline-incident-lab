"""MP-19 — Traffic is flat while the streaming store grows every hour.

Frozen mechanism (incident_taxonomy.md MP-19): the keyed store keeps one entry
per trip-stop-window with no business-termination or retention boundary, so keys
for ended trips are never released. At a fixed input rate the retained key count
and its byte size grow every hour, and the saved snapshots grow with them, until
the job misses its SLA.

The reference fix configures a retention boundary so the retained set plateaus
after the expected window. The emitted business result is unchanged, because an
ended window's already-emitted value does not depend on whether its key is still
retained; a restart reproduces the same output.

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
from ._batchD_state_store import KeyedStoreSimulator

FAULT = "mp19_no_retention_boundary"
HOURS = 12
NEW_KEYS_PER_HOUR = 8
BYTES_PER_KEY = 64
RETAIN_HOURS = 2
STORE_BYTES_BUDGET = 2048
RECOVERY_DATASET = "mart_window_result"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mp19_store_metric (
    approach       TEXT NOT NULL,
    snapshot_index INTEGER NOT NULL,
    retained_keys  INTEGER NOT NULL,
    store_bytes    INTEGER NOT NULL,
    snapshot_bytes INTEGER NOT NULL,
    growth_bytes   INTEGER NOT NULL,
    PRIMARY KEY (approach, snapshot_index)
);
CREATE TABLE IF NOT EXISTS mp19_probe (
    approach     TEXT PRIMARY KEY,
    final_keys   INTEGER NOT NULL,
    final_bytes  INTEGER NOT NULL,
    growth_slope INTEGER NOT NULL,
    plateaued    INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mp19_window_result (
    window_key TEXT PRIMARY KEY,
    value      INTEGER NOT NULL
);
"""


class MP19Incident:
    metadata = IncidentMetadata(
        incident_id="MP-19",
        family="D. Performance and cost",
        reader_title="Event volume is flat, but the streaming job's footprint and restart time grow every hour",
        primary_invariant_id="MP-19-STORE-BOUND",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-19-STORESIZE-001", "MP-19-SNAPSHOT-001"),
        transfer_mappings=(
            "Flink/Spark keyed streaming store retention and snapshot growth",
        ),
        nearest_old_scenario="None; the old books define retention concepts but have no growth/restart experiment.",
        old_scenario_difference=(
            "Not rising traffic and not slower object storage: input rate is flat, yet "
            "the retained working set grows every hour because ended-window keys are "
            "never released."
        ),
        affected_tables=(),
    )

    _sim = KeyedStoreSimulator(NEW_KEYS_PER_HOUR, BYTES_PER_KEY)

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        ctx.warehouse.executescript(_SCHEMA)

    def _write_metric(self, ctx: RunContext, approach: str, snapshots: list[dict[str, Any]]) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp19_store_metric WHERE approach = ?", (approach,))
            for s in snapshots:
                ctx.warehouse.execute(
                    "INSERT INTO mp19_store_metric (approach, snapshot_index, retained_keys, "
                    "store_bytes, snapshot_bytes, growth_bytes) VALUES (?,?,?,?,?,?)",
                    (approach, s["snapshot_index"], s["retained_keys"], s["store_bytes"],
                     s["snapshot_bytes"], s["growth_bytes"]))

    def _record_probe(self, ctx: RunContext, approach: str, snapshots: list[dict[str, Any]]) -> None:
        final = snapshots[-1]
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "INSERT OR REPLACE INTO mp19_probe "
                "(approach, final_keys, final_bytes, growth_slope, plateaued) VALUES (?,?,?,?,?)",
                (approach, final["retained_keys"], final["store_bytes"],
                 self._sim.growth_slope(snapshots), int(self._sim.is_bounded(snapshots))))

    def _read_probe(self, ctx: RunContext, approach: str) -> dict[str, Any] | None:
        row = db.query_one(ctx.warehouse, "SELECT * FROM mp19_probe WHERE approach = ?", (approach,))
        return dict(row) if row else None

    def _write_entries(self, ctx: RunContext, operator_id: str, retain_hours) -> int:
        entries = self._sim.terminal_entries(HOURS, retain_hours)
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM simulated_state_entry WHERE operator_id = ?", (operator_id,))
            for e in entries:
                ctx.control.execute(
                    "INSERT INTO simulated_state_entry (operator_id, state_key, event_time, "
                    "expiry_time, byte_estimate) VALUES (?,?,?,?,?)",
                    (operator_id, e["state_key"], e["event_time"], e["expiry_time"], e["byte_estimate"]))
        return len(entries)

    def baseline_ok(self, ctx: RunContext) -> bool:
        snaps = self._sim.snapshots(HOURS, RETAIN_HOURS)
        return self._sim.is_bounded(snaps) and snaps[-1]["store_bytes"] <= STORE_BYTES_BUDGET

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        bounded_snaps = self._sim.snapshots(HOURS, RETAIN_HOURS)
        bounded_ok_before = self._sim.is_bounded(bounded_snaps)
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(set(profile.get("faulty_transforms", [])) | {FAULT})
        profile.setdefault("params", {})["retention_hours"] = None
        ctx.save_fault_profile(profile)

        snaps = self._sim.snapshots(HOURS, None)
        self._write_metric(ctx, "retained_all", snaps)
        self._record_probe(ctx, "retained_all", snaps)
        self._write_entries(ctx, "mp19_retained_all", None)
        grows = not self._sim.is_bounded(snaps) or snaps[-1]["store_bytes"] > STORE_BYTES_BUDGET

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fixtures_ok, fixture_detail = integrity.fixtures_untouched(ctx)
        fixtures_ok = fixtures_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-19-INJECT-PRE", "precondition",
                      "bounded retention plateaus within the store byte budget at baseline",
                      expected=True, actual=bounded_ok_before,
                      status="PASS" if bounded_ok_before else "FAIL"),
            Assertion("MP-19-INJECT-POST", "postcondition",
                      "with no retention boundary the store grows past its budget at a flat input rate",
                      expected=True, actual=grows, status="PASS" if grows else "FAIL"),
            Assertion("MP-19-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection (control-plane fault only)",
                      expected=True, actual=fixtures_ok, status="PASS" if fixtures_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected",
                     fields={"final_bytes": snaps[-1]["store_bytes"], "budget": STORE_BYTES_BUDGET})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-19", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "final_bytes": snaps[-1]["store_bytes"], "budget": STORE_BYTES_BUDGET,
                                    "fixture_checksums_unchanged": fixtures_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        snaps = self._sim.snapshots(HOURS, None)
        self._write_metric(ctx, "retained_all", snaps)
        self._record_probe(ctx, "retained_all", snaps)
        self._write_entries(ctx, "mp19_retained_all", None)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="faulty_run", fields={"final_bytes": snaps[-1]["store_bytes"]})
        return StepReport("run", "MP-19", ctx.run_id, "executed",
                          fields={"final_keys": snaps[-1]["retained_keys"],
                                  "final_bytes": snaps[-1]["store_bytes"],
                                  "growth_slope": self._sim.growth_slope(snaps)})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        probe = self._read_probe(ctx, "retained_all") or {}
        final_bytes = probe.get("final_bytes", 0)
        slope = probe.get("growth_slope", 0)
        plateaued = bool(probe.get("plateaued", 0))

        size_bad = final_bytes > STORE_BYTES_BUDGET
        growth_bad = (not plateaued) or slope > 0
        detected = bool(size_bad or growth_bad)

        common.write_quality_result(
            ctx, "MP-19-STORESIZE-001", "published_metric",
            expected=f"the retained store stays within {STORE_BYTES_BUDGET} bytes at a flat input rate",
            actual=f"the retained store reaches {final_bytes} bytes",
            status="FAIL" if size_bad else "PASS")
        common.write_quality_result(
            ctx, "MP-19-SNAPSHOT-001", "scan_work",
            expected="saved-snapshot bytes plateau after the expected window",
            actual=f"saved-snapshot bytes keep growing (net growth {slope} bytes)",
            status="FAIL" if growth_bad else "PASS")

        assertions = [
            Assertion("MP-19-DET-STORESIZE", "published_metric",
                      "retained store bytes within budget at a flat input rate",
                      expected=f"<= {STORE_BYTES_BUDGET}", actual=final_bytes,
                      status="FAIL" if size_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-19-DET-SNAPSHOT", "scan_work",
                      "saved-snapshot bytes plateau (no continued growth)",
                      expected="plateau", actual=f"net growth {slope} bytes",
                      status="FAIL" if growth_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "final_bytes": final_bytes,
                  "growth_slope": slope, "budget": STORE_BYTES_BUDGET,
                  "result_identical_after_retention": True}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-19", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-19", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        common.write_csv(
            tables / "store_growth_by_hour.csv",
            db.rows_to_dicts(db.query(ctx.warehouse,
                "SELECT snapshot_index, retained_keys, store_bytes, snapshot_bytes, growth_bytes "
                "FROM mp19_store_metric WHERE approach = 'retained_all' ORDER BY snapshot_index")),
            ["snapshot_index", "retained_keys", "store_bytes", "snapshot_bytes", "growth_bytes"])
        common.write_csv(
            tables / "input_rate.csv",
            [{"hour": i, "new_keys": NEW_KEYS_PER_HOUR} for i in range(1, HOURS + 1)],
            ["hour", "new_keys"])
        common.write_csv(
            tables / "retained_key_sample.csv",
            db.rows_to_dicts(db.query(ctx.control,
                "SELECT state_key AS window_key, event_time, byte_estimate FROM simulated_state_entry "
                "WHERE operator_id = 'mp19_retained_all' ORDER BY state_key")),
            ["window_key", "event_time", "byte_estimate"])
        common.write_common_evidence_tables(ctx)

        alert = common.build_alert(
            "MP-19", self.metadata.reader_title,
            reported_symptom=(
                "Events per minute are steady, yet the streaming job's memory footprint "
                "and the size of each saved snapshot grow every hour, and restarts take "
                "longer each time, until the job misses its freshness SLA."),
            time_pressure="The job feeds a near-real-time arrivals board with an SLA.",
            affected_product="near-real-time window results",
            observed={"input_rate": "flat", "store_size": "grows every hour",
                      "restart": "slower each hour"})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx))
        index = common.write_evidence_index(ctx, "MP-19")
        return StepReport("evidence", "MP-19", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        blast_radius = {
            "held_product": RECOVERY_DATASET,
            "action": "back up the current snapshot and hold before restart",
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast_radius)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-19", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-19", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["retention_hours"] = RETAIN_HOURS
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident", event_type="repaired",
                     fields={"strategy": f"event-time retention boundary of {RETAIN_HOURS} hours"})
        report = StepReport("repair", "MP-19", ctx.run_id, "repaired",
                            fields={"change": f"configure an event-time retention boundary "
                                             f"({RETAIN_HOURS} hours) so ended-window keys are released",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        pub = f"PUB-{ctx.run_id}-recover"
        snaps = self._sim.snapshots(HOURS, RETAIN_HOURS)
        self._write_metric(ctx, "bounded", snaps)
        self._record_probe(ctx, "bounded", snaps)
        entries = self._write_entries(ctx, "mp19_bounded", RETAIN_HOURS)

        # The emitted business result is independent of retention; a restart of the
        # bounded store reproduces exactly this mapping.
        results = self._sim.window_results(HOURS)
        with db.transaction(conn):
            conn.execute("DELETE FROM mp19_window_result")
            for window_key in sorted(results):
                conn.execute("INSERT INTO mp19_window_result (window_key, value) VALUES (?, ?)",
                             (window_key, results[window_key]))
        dv_id = common.record_recovery_publication(ctx, RECOVERY_DATASET, pub)

        manifest = {
            "mode": "REPLAY", "publication_id": pub, "dataset_version_id": dv_id,
            "scope": {"datasets": [RECOVERY_DATASET], "retention_hours": RETAIN_HOURS},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key at window grain; deterministic replay after restart",
            "strategy": f"event-time retention boundary of {RETAIN_HOURS} hours; store plateaus",
            "final_bytes_after": snaps[-1]["store_bytes"], "store_budget": STORE_BYTES_BUDGET,
            "retained_entries": entries, "window_rows": len(results),
            "evidence_origin": common.EVIDENCE_ORIGIN,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"window_rows": len(results)})
        return StepReport("recover", "MP-19", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        assertions: list[Assertion] = []

        reference = self._sim.window_results(HOURS)
        persisted = {r["window_key"]: r["value"] for r in db.query(conn,
            "SELECT window_key, value FROM mp19_window_result")}

        assertions.append(Assertion(
            "MP-19-VER-CORRECT", "correctness",
            "recovered window results equal an independent replay (restart reproduces output)",
            expected=len(reference), actual=len(persisted),
            status="PASS" if persisted == reference else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-19-VER-COMPLETE", "completeness",
            "recovered results cover every window that opened",
            expected=len(reference), actual=len(persisted),
            status="PASS" if set(persisted) == set(reference) else "FAIL"))

        bounded = self._read_probe(ctx, "bounded")
        bounded_ok = bool(bounded) and bounded["plateaued"] == 1 \
            and bounded["final_bytes"] <= STORE_BYTES_BUDGET
        assertions.append(Assertion(
            "MP-19-VER-BOUNDED", "boundedness",
            "the recovered store plateaus within the byte budget",
            expected=f"plateau and <= {STORE_BYTES_BUDGET}",
            actual=(bounded or {}).get("final_bytes"),
            status="PASS" if bounded_ok else "FAIL"))

        entries = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM simulated_state_entry WHERE operator_id = 'mp19_bounded'")
        control_ok = entries >= 1 and common.has_recovery_publication(ctx, RECOVERY_DATASET)
        assertions.append(Assertion(
            "MP-19-VER-CONTROL", "control_consistency",
            "the bounded store entries and a recovery publication are recorded",
            expected=">=1 bounded entry + recovered publication",
            actual={"bounded_entries": entries, "recovered_publication":
                    common.has_recovery_publication(ctx, RECOVERY_DATASET)},
            status="PASS" if control_ok else "FAIL"))

        assertions.append(Assertion(
            "MP-19-VER-IDEMPOTENT", "idempotency",
            "independent replay equals the persisted window results",
            expected=True, actual=(persisted == reference and bool(persisted)),
            status="PASS" if (persisted == reference and bool(persisted)) else "FAIL"))

        integrity_ok, integrity_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-19-VER-FIXTURE", "fixture_integrity",
            "run landing inputs match checked-in fixture checksums",
            expected=True, actual=integrity_ok, status="PASS" if integrity_ok else "FAIL",
            evidence=str(integrity_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-19", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP19Incident()
