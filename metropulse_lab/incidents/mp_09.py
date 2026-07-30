"""MP-09 — West-side station ridership is about a quarter low though every file is valid.

Frozen mechanism (incident_taxonomy.md MP-09): the pipeline validated the
integrity of each file that arrived but never proved the batch was complete. Four
station-feed parts were expected; only three were delivered. Single-file
integrity is not batch completeness, so the west-side zone is entirely absent
while the job succeeds and every delivered file checks out.

A completeness gate must hold the batch until the expected delivery set is
present, then verify checksums and publish atomically. Reader-facing text
describes only the symptom: a west-side ridership gap while every delivered file
is valid.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .. import db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchB_common as bc
from .contract import IncidentMetadata

DATASET = "mp09_station_gate_counts"
OUTBOX_ID = "MSG-mp09-recovered"

# Four station-feed parts, one zone each. The west-side zone is the one withheld.
DELIVERIES = (
    ("PART_1", "R1", "Z-CENTRAL", ("STN_001", "STN_002", "STN_003")),
    ("PART_2", "R2", "Z-NORTH", ("STN_004", "STN_005", "STN_006")),
    ("PART_3", "R3", "Z-EAST", ("STN_007", "STN_008", "STN_009")),
    ("PART_4", "R4", "Z-WEST", ("STN_010", "STN_011", "STN_012")),
)
WEST_DELIVERY = "PART_4"
SERVICE_DATE = "2026-04-15"
LEAK = ("shard", "completeness", "manifest", "set difference", "set-difference",
        "incomplet", "missing file")


def _delivery_events(delivery_id: str, route_id: str, stations: tuple[str, ...]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for station in stations:
        for k in range(4):
            rows.append({
                "gate_event_id": f"MGATE_{station}_{k}",
                "device_id": f"GATE_DEV_{station}",
                "station_id": station,
                "zone_code": next(z for _, r, z, s in DELIVERIES if station in s),
                "route_id": route_id,
                "service_date": SERVICE_DATE,
                "passenger_count": 5 + (k % 3),
                "source_delivery": delivery_id,
            })
    return rows


def _delivery_checksum(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(sorted(rows, key=lambda r: r["gate_event_id"]),
                         sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _all_deliveries() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for delivery_id, route_id, zone, stations in DELIVERIES:
        rows = _delivery_events(delivery_id, route_id, stations)
        out[delivery_id] = {"delivery_id": delivery_id, "route_id": route_id, "zone_code": zone,
                            "stations": stations, "rows": rows,
                            "checksum": _delivery_checksum(rows)}
    return out


class MP09Incident:
    metadata = IncidentMetadata(
        incident_id="MP-09",
        family="B. Missing or late",
        reader_title="West-side station ridership is about a quarter low though every delivered file is valid",
        primary_invariant_id="MP-09-BATCH-COMPLETE",
        core_evidence_kind="REAL_SQLITE",
        recovery_mode="REPLAY",
        fault_operations=("omit_expected_source_partition:mp09_west",),
        expected_detection_ids=("MP-09-COVERAGE-001", "MP-09-CONTROL-001"),
        transfer_mappings=("Object-storage and table-format delivery manifests",),
        nearest_old_scenario="Adjacent to a downstream partial-write scenario.",
        old_scenario_difference=(
            "Not a corrupt file skipped by the parser and not a real station closure: "
            "each delivered file is valid, but the batch is short one expected delivery "
            "and single-file integrity was mistaken for batch completeness."
        ),
        affected_tables=("mp09_gate_count",),
    )

    # -- schema / baseline ----------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp09_gate_count ("
                "gate_event_id TEXT NOT NULL PRIMARY KEY, device_id TEXT NOT NULL, "
                "station_id TEXT NOT NULL, zone_code TEXT NOT NULL, route_id TEXT NOT NULL, "
                "service_date TEXT NOT NULL, passenger_count INTEGER NOT NULL, "
                "source_delivery TEXT NOT NULL)")
            ctx.warehouse.execute(
                "CREATE TABLE IF NOT EXISTS mp09_expected_delivery ("
                "delivery_id TEXT NOT NULL PRIMARY KEY, zone_code TEXT NOT NULL, "
                "route_id TEXT NOT NULL, checksum TEXT NOT NULL)")

    def _process(self, ctx: RunContext, delivery_ids: list[str]) -> None:
        deliveries = _all_deliveries()
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp09_gate_count")
            for did in delivery_ids:
                d = deliveries[did]
                db.insert_rows(
                    ctx.warehouse, "mp09_gate_count",
                    ("gate_event_id", "device_id", "station_id", "zone_code", "route_id",
                     "service_date", "passenger_count", "source_delivery"),
                    [(r["gate_event_id"], r["device_id"], r["station_id"], r["zone_code"],
                      r["route_id"], r["service_date"], r["passenger_count"], r["source_delivery"])
                     for r in d["rows"]])
        # Actual object inventory: only the delivered parts are recorded, each valid.
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM object_inventory WHERE object_id LIKE 'PART_%'")
            for did in delivery_ids:
                d = deliveries[did]
                ctx.control.execute(
                    "INSERT OR REPLACE INTO object_inventory (object_id, run_relative_path, "
                    "checksum, lifecycle_state, active_writer) VALUES (?, ?, ?, 'RECEIVED', NULL)",
                    (did, f"landing/gate_parts/{did}.csv", d["checksum"]))

    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        deliveries = _all_deliveries()
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute("DELETE FROM mp09_expected_delivery")
            for did, d in sorted(deliveries.items()):
                ctx.warehouse.execute(
                    "INSERT INTO mp09_expected_delivery (delivery_id, zone_code, route_id, checksum) "
                    "VALUES (?, ?, ?, ?)", (did, d["zone_code"], d["route_id"], d["checksum"]))
        self._process(ctx, [did for did, *_ in DELIVERIES])
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-baseline", DATASET, SERVICE_DATE,
                                  "PUBLISHED", "PUB-baseline")
        ctx.log.emit(logical_time=ctx.clock.now(), component="ingest",
                     event_type="batch_published", fields={"parts": len(DELIVERIES)})

    def baseline_ok(self, ctx: RunContext) -> bool:
        zones = db.scalar(ctx.warehouse, "SELECT COUNT(DISTINCT zone_code) FROM mp09_gate_count")
        received = db.scalar(ctx.control, "SELECT COUNT(*) FROM object_inventory WHERE object_id LIKE 'PART_%'")
        return zones == len(DELIVERIES) and received == len(DELIVERIES)

    # -- faulty ---------------------------------------------------------------
    def _build_faulty(self, ctx: RunContext) -> dict[str, Any]:
        present = [did for did, *_ in DELIVERIES if did != WEST_DELIVERY]
        self._process(ctx, present)
        return {"delivered": len(present), "expected": len(DELIVERIES),
                "withheld": WEST_DELIVERY}

    def inject(self, ctx: RunContext) -> StepReport:
        before_zones = db.scalar(ctx.warehouse, "SELECT COUNT(DISTINCT zone_code) FROM mp09_gate_count")
        pre_ok = before_zones == len(DELIVERIES)
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp09_withhold_delivery"] = WEST_DELIVERY
        ctx.save_fault_profile(profile)

        result = self._build_faulty(ctx)
        after_zones = db.scalar(ctx.warehouse, "SELECT COUNT(DISTINCT zone_code) FROM mp09_gate_count")

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, _ = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-09-INJECT-PRE", "precondition",
                      "baseline covers every expected zone",
                      expected=len(DELIVERIES), actual=before_zones,
                      status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-09-INJECT-POST", "postcondition",
                      "a zone is uncovered after withholding one delivery",
                      expected=f"<{len(DELIVERIES)}", actual=after_zones,
                      status="PASS" if after_zones < len(DELIVERIES) else "FAIL"),
            Assertion("MP-09-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.at_offset(60), component="incident",
                     event_type="fault_injected", fields={"withheld": WEST_DELIVERY})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-09", ctx.run_id, status,
                            fields={**result, "fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        return StepReport("run", "MP-09", ctx.run_id, "executed", fields=self._build_faulty(ctx))

    # -- detect ---------------------------------------------------------------
    def _signals(self, ctx: RunContext) -> dict[str, Any]:
        expected_stations = {s for _, _, _, stations in DELIVERIES for s in stations}
        covered = {r["station_id"] for r in db.query(
            ctx.warehouse, "SELECT DISTINCT station_id FROM mp09_gate_count")}
        missing_stations = sorted(expected_stations - covered)
        expected_deliveries = {d for d, *_ in DELIVERIES}
        received = {r["object_id"] for r in db.query(
            ctx.control, "SELECT object_id FROM object_inventory WHERE object_id LIKE 'PART_%'")}
        missing_deliveries = sorted(expected_deliveries - received)
        # accounting identity: expected == processed(received) + quarantined
        quarantined = 0
        accounting_closes = len(expected_deliveries) == len(received) + quarantined
        return {"expected_stations": len(expected_stations), "covered_stations": len(covered),
                "missing_stations": missing_stations,
                "expected_deliveries": len(expected_deliveries), "received_deliveries": len(received),
                "missing_deliveries": missing_deliveries, "accounting_closes": accounting_closes,
                "data_gap": bool(missing_stations),
                "control_gap": bool(missing_deliveries) or not accounting_closes}

    def detect(self, ctx: RunContext) -> StepReport:
        s = self._signals(ctx)
        bc.write_quality_result(
            ctx, "MP-09-COVERAGE-001", "coverage",
            expected=f"{s['expected_stations']} stations covered across every zone",
            actual=f"{s['covered_stations']} stations covered; a zone is absent",
            status="FAIL" if s["data_gap"] else "PASS")
        bc.write_quality_result(
            ctx, "MP-09-CONTROL-001", "control_consistency",
            expected="every expected delivery is present (expected = received + quarantined)",
            actual=(f"{s['received_deliveries']} of {s['expected_deliveries']} expected deliveries present"
                    if s["control_gap"] else "all expected deliveries present"),
            status="FAIL" if s["control_gap"] else "PASS")

        assertions = [
            Assertion("MP-09-DET-COVERAGE", "completeness",
                      "every expected station is covered",
                      expected=s["expected_stations"], actual=s["covered_stations"],
                      status="FAIL" if s["data_gap"] else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-09-DET-CONTROL", "control_consistency",
                      "expected delivery set equals the received set",
                      expected=False, actual=s["control_gap"],
                      status="FAIL" if s["control_gap"] else "PASS",
                      evidence="reports/detection.json"),
        ]
        detected = s["data_gap"] or s["control_gap"]
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-09", "run_id": ctx.run_id, "fields": s,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-09", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=s, assertions=assertions)

    # -- evidence -------------------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        s = self._signals(ctx)

        # Station coverage with per-station ridership.
        covered = {r["station_id"]: r["ridership"] for r in db.query(
            ctx.warehouse, "SELECT station_id, SUM(passenger_count) AS ridership "
            "FROM mp09_gate_count GROUP BY station_id")}
        station_rows = []
        for _, route_id, zone, stations in DELIVERIES:
            for station in stations:
                station_rows.append({"station_id": station, "zone_code": zone, "route_id": route_id,
                                     "ridership_present": covered.get(station, 0),
                                     "covered": "yes" if station in covered else "no"})
        station_rows.sort(key=lambda r: r["station_id"])
        bc.write_csv(tables / "station_coverage.csv", station_rows,
                     ["station_id", "zone_code", "route_id", "ridership_present", "covered"])

        # Delivery inventory: expected set versus what actually arrived.
        expected = db.rows_to_dicts(db.query(ctx.warehouse,
            "SELECT delivery_id, zone_code, route_id, checksum FROM mp09_expected_delivery "
            "ORDER BY delivery_id"))
        received = {r["object_id"]: r["checksum"] for r in db.query(
            ctx.control, "SELECT object_id, checksum FROM object_inventory WHERE object_id LIKE 'PART_%'")}
        inv_rows = []
        for e in expected:
            did = e["delivery_id"]
            got = did in received
            inv_rows.append({"delivery_id": did, "zone_code": e["zone_code"], "route_id": e["route_id"],
                             "expected": "yes", "received": "yes" if got else "no",
                             "checksum_valid": "yes" if (got and received[did] == e["checksum"])
                             else ("no" if got else "n/a")})
        bc.write_csv(tables / "delivery_inventory.csv", inv_rows,
                     ["delivery_id", "zone_code", "route_id", "expected", "received", "checksum_valid"])

        # Zone-level rollup.
        zone_rows = []
        for zone in sorted({z for _, _, z, _ in DELIVERIES}):
            covered_here = db.scalar(ctx.warehouse,
                "SELECT COUNT(DISTINCT station_id) FROM mp09_gate_count WHERE zone_code = ?", (zone,))
            expected_here = sum(len(st) for _, _, z, st in DELIVERIES if z == zone)
            ridership = db.scalar(ctx.warehouse,
                "SELECT COALESCE(SUM(passenger_count), 0) FROM mp09_gate_count WHERE zone_code = ?", (zone,))
            zone_rows.append({"zone_code": zone, "stations_expected": expected_here,
                              "stations_covered": covered_here, "ridership": ridership})
        bc.write_csv(tables / "zone_coverage.csv", zone_rows,
                     ["zone_code", "stations_expected", "stations_covered", "ridership"])
        bc.write_csv(tables / "quality_results.csv", bc.quality_results_rows(ctx),
                     ["check_id", "scope_key", "dimension", "expected", "actual", "status"])

        alert = bc.base_alert(
            "MP-09", self.metadata.reader_title,
            reported_symptom=(
                "West-side station ridership is about a quarter lower than usual, yet every file "
                "that arrived is checksum-valid and the ingest job reported success."),
            time_pressure="A zone ridership summary is due at the next service review.",
            affected_product="station_gate_counts",
            extra={"observed": {"expected_deliveries": s["expected_deliveries"],
                                "received_deliveries": s["received_deliveries"],
                                "uncovered_stations": s["missing_stations"]}})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", bc.sanitized_timeline(ctx, LEAK))
        write_json(ctx.paths.evidence_index, bc.evidence_index("MP-09", ctx.run_id, ev))
        return StepReport("evidence", "MP-09", ctx.run_id, "collected",
                          fields={"uncovered_stations": len(s["missing_stations"])})

    # -- contain / repair / recover ------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        bc.set_dataset_state(ctx, DATASET, "HELD", from_state="PUBLISHED")
        s = self._signals(ctx)
        blast = {"held_dataset": DATASET, "uncovered_stations": s["missing_stations"],
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.now(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-09", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-09", ctx.run_id, "contained", fields=blast)

    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile.setdefault("params", {}).pop("mp09_withhold_delivery", None)
        profile["params"]["mp09_gate"] = "hold_until_expected_set_present_then_verify_and_publish"
        ctx.save_fault_profile(profile)
        report = StepReport("repair", "MP-09", ctx.run_id, "repaired",
                            fields={"change": "hold the batch until the expected delivery set is "
                                             "present, verify every checksum, then publish atomically"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    def recover(self, ctx: RunContext) -> StepReport:
        # REPLAY once the withheld delivery is present: reprocess the full set.
        self._process(ctx, [did for did, *_ in DELIVERIES])
        bc.upsert_dataset_version(ctx, f"DV-{DATASET}-recover", DATASET, SERVICE_DATE,
                                  "PUBLISHED", "PUB-recover")
        bc.record_outbox(ctx, OUTBOX_ID, "mp09.recovered",
                         {"dataset": DATASET, "deliveries": len(DELIVERIES)})
        manifest = {
            "mode": "REPLAY",
            "scope": {"service_date": SERVICE_DATE, "datasets": ["mp09_gate_count"]},
            "source": "reprocess the complete expected delivery set",
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key on gate_event_id; deterministic rebuild",
            "validation": "expected = received + quarantined; every zone covered; checksums valid",
            "deliveries_present": len(DELIVERIES),
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.now(), component="recovery",
                     event_type="replay_completed", fields={"deliveries": len(DELIVERIES)})
        return StepReport("recover", "MP-09", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        s = self._signals(ctx)
        deliveries = _all_deliveries()
        expected_total = sum(r["passenger_count"] for d in deliveries.values() for r in d["rows"])
        got_total = db.scalar(ctx.warehouse, "SELECT COALESCE(SUM(passenger_count), 0) FROM mp09_gate_count")
        dup = db.scalar(ctx.warehouse,
                        "SELECT COUNT(*) - COUNT(DISTINCT gate_event_id) FROM mp09_gate_count")
        changed = bc.shared_tables_changed(ctx)
        checksums_valid = all(
            db.scalar(ctx.control, "SELECT checksum FROM object_inventory WHERE object_id = ?", (did,))
            == deliveries[did]["checksum"] for did, *_ in DELIVERIES)

        assertions = [
            Assertion("MP-09-VER-CORRECT", "correctness",
                      "total ridership equals the full expected batch",
                      expected=expected_total, actual=got_total,
                      status="PASS" if got_total == expected_total else "FAIL",
                      evidence="reports/verification.json"),
            Assertion("MP-09-VER-COMPLETE", "completeness",
                      "every expected station is covered and accounting closes",
                      expected=s["expected_stations"],
                      actual={"covered": s["covered_stations"], "accounting_closes": s["accounting_closes"]},
                      status="PASS" if (s["covered_stations"] == s["expected_stations"]
                                        and s["accounting_closes"]) else "FAIL"),
            Assertion("MP-09-VER-UNIQUE", "uniqueness",
                      "no duplicate gate_event_id", expected=0, actual=dup,
                      status="PASS" if dup == 0 else "FAIL"),
            Assertion("MP-09-VER-CONTROL", "control_consistency",
                      "all expected deliveries present with valid checksums",
                      expected=len(DELIVERIES), actual=s["received_deliveries"],
                      status="PASS" if (s["received_deliveries"] == len(DELIVERIES)
                                        and checksums_valid) else "FAIL"),
            Assertion("MP-09-VER-BOUNDED", "boundedness",
                      "no shared warehouse table changed since the faulty run",
                      expected=[], actual=changed, status="PASS" if not changed else "FAIL"),
            Assertion("MP-09-VER-IDEMPOTENT", "idempotency",
                      "recomputed ridership total equals persisted total",
                      expected=expected_total, actual=got_total,
                      status="PASS" if got_total == expected_total else "FAIL"),
            Assertion("MP-09-VER-SIDEEFFECT", "side_effects",
                      "exactly one recovery notification emitted",
                      expected=1, actual=bc.outbox_count(ctx, OUTBOX_ID),
                      status="PASS" if bc.outbox_count(ctx, OUTBOX_ID) == 1 else "FAIL"),
        ]
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion("MP-09-VER-FIXTURE", "fixture_integrity",
                                    "run landing inputs match checked-in fixtures",
                                    expected=True, actual=fx_ok,
                                    status="PASS" if fx_ok else "FAIL",
                                    evidence=str(fx_detail.get("status"))))
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-09", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP09Incident()
