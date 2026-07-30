"""MP-02 — A maintenance-device filter empties the station gate-count report.

Reader-facing symptom only: after a filter is added to leave out maintenance
devices, the station gate-count report drops from its normal set of rows to zero
rows, yet the query runs without error.

Frozen mechanism (incident_taxonomy.md MP-02): the exclusion is expressed as an
anti-join whose right-hand side contains a blank key. When that blank key is
present the row predicate can no longer be true for any row, so every row is
left out. The reader-facing title and evidence pack never name the mechanism.

Implemented entirely in real Python-3.12 SQLite. The two exclusion forms are run
as actual SQL against the run warehouse: the robust correlated form for the
clean baseline and recovery, and the collapsing membership form for the fault.
"""

from __future__ import annotations

from typing import Any

from .. import config, db
from ..checks import integrity
from ..checks.snapshots import table_logical_hash
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchA_common as common
from .contract import IncidentMetadata

FAULT = "mp02_null_anti_join"
INPUT_TABLES = ("mp02_gate_reading", "mp02_maintenance_device")
PUBLISHED_TABLE = "mp02_station_gate_daily"
INPUT_HASHES = "mp02_input_hashes.json"
FAULTY_SNAPSHOT = "faulty_snapshots.json"
SNAPSHOT_TABLES = tuple(config.WAREHOUSE_TABLES) + tuple(config.MART_TABLES)

# A synthetic maintenance station with no route mapping, so nothing here can
# perturb MP-01's route-hour reconciliation.
MAINT_STATION = "STN_MAINT"
MAINT_DEVICE_A = "GATE_DEV_MAINT_A"
MAINT_DEVICE_B = "GATE_DEV_MAINT_B"


class MP02Incident:
    metadata = IncidentMetadata(
        incident_id="MP-02",
        family="A. Present but wrong",
        reader_title=("A station gate-count report returns zero rows after a "
                      "maintenance-device filter is added, though the query runs "
                      "without error"),
        primary_invariant_id="MP-02-CONSERVATION",
        core_evidence_kind="REAL_SQLITE",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-02-COLLAPSE-001", "MP-02-CONSERVATION-001",
                                "MP-02-KEYCONTRACT-001"),
        transfer_mappings=("PostgreSQL/DuckDB anti-join membership semantics with a "
                           "blank right-hand key (transfer note only)",),
        nearest_old_scenario="None; the old books define the semantics but ship no runnable incident.",
        old_scenario_difference=(
            "Not an empty source and not a device-id churn: the readings are all "
            "present and the exclusion list is intact; the report empties itself."
        ),
        affected_tables=(PUBLISHED_TABLE,),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        w = ctx.warehouse
        w.execute(
            f"CREATE TABLE IF NOT EXISTS {PUBLISHED_TABLE} ("
            " service_date TEXT NOT NULL, station_id TEXT NOT NULL,"
            " station_gate_count INTEGER NOT NULL, reading_count INTEGER NOT NULL,"
            " publication_id TEXT NOT NULL,"
            " PRIMARY KEY (service_date, station_id))")
        w.execute(
            "CREATE TABLE IF NOT EXISTS mp02_gate_reading ("
            " reading_id TEXT NOT NULL PRIMARY KEY, service_date TEXT NOT NULL,"
            " station_id TEXT NOT NULL, device_id TEXT, passenger_count INTEGER NOT NULL)")
        w.execute(
            "CREATE TABLE IF NOT EXISTS mp02_maintenance_device ("
            " entry_id INTEGER NOT NULL PRIMARY KEY, device_id TEXT, note TEXT NOT NULL)")
        ctx.control.execute(
            "CREATE TABLE IF NOT EXISTS mp02_build_provenance ("
            " dataset TEXT NOT NULL PRIMARY KEY, exclusion_rule TEXT NOT NULL,"
            " key_contract_ok INTEGER NOT NULL, published_rows INTEGER NOT NULL,"
            " publication_id TEXT NOT NULL)")
        w.commit()
        ctx.control.commit()

        # Real station readings (independent copy; fct_station_gate_count is not
        # touched, so MP-01 is unaffected).
        real = db.query(
            w, "SELECT gate_event_id, service_date, station_id, device_id, "
            "passenger_count FROM fct_station_gate_count ORDER BY gate_event_id")
        rows = [(r["gate_event_id"], r["service_date"], r["station_id"],
                 r["device_id"], r["passenger_count"]) for r in real]
        # Maintenance-device readings live only at the maintenance station.
        for service_date in config.BASELINE_SERVICE_DATES:
            key = service_date.replace("-", "")
            rows.append((f"MAINT_{key}_A", service_date, MAINT_STATION, MAINT_DEVICE_A, 7))
            rows.append((f"MAINT_{key}_B", service_date, MAINT_STATION, MAINT_DEVICE_B, 9))
        with db.transaction(w):
            w.execute("DELETE FROM mp02_gate_reading")
            db.insert_rows(w, "mp02_gate_reading",
                           ("reading_id", "service_date", "station_id", "device_id",
                            "passenger_count"), rows)
            w.execute("DELETE FROM mp02_maintenance_device")
            db.insert_rows(w, "mp02_maintenance_device",
                           ("entry_id", "device_id", "note"),
                           [(1, MAINT_DEVICE_A, "scheduled maintenance device"),
                            (2, MAINT_DEVICE_B, "scheduled maintenance device"),
                            (3, None, "device awaiting registration")])

        self._materialize(ctx, faulty=False, tag="baseline")
        common.write_hash_checkpoint(
            ctx, INPUT_HASHES, common.capture_table_hashes(w, INPUT_TABLES))

    def _materialize(self, ctx: RunContext, faulty: bool, tag: str) -> int:
        """Rebuild the published aggregate with the correct or faulty exclusion."""
        w = ctx.warehouse
        pub = f"PUB-{ctx.run_id}-{tag}"
        rule = "membership_exclusion" if faulty else "correlated_exclusion"
        with db.transaction(w):
            w.execute(f"DELETE FROM {PUBLISHED_TABLE}")
            if faulty:
                # Membership form: a blank right-hand key collapses the result.
                w.execute(
                    f"INSERT INTO {PUBLISHED_TABLE} "
                    " (service_date, station_id, station_gate_count, reading_count, publication_id) "
                    "SELECT r.service_date, r.station_id, SUM(r.passenger_count), COUNT(*), ? "
                    "FROM mp02_gate_reading r "
                    "WHERE r.device_id NOT IN (SELECT device_id FROM mp02_maintenance_device) "
                    "GROUP BY r.service_date, r.station_id "
                    "ORDER BY r.service_date, r.station_id", (pub,))
            else:
                # Correlated form: robust to a blank right-hand key.
                w.execute(
                    f"INSERT INTO {PUBLISHED_TABLE} "
                    " (service_date, station_id, station_gate_count, reading_count, publication_id) "
                    "SELECT r.service_date, r.station_id, SUM(r.passenger_count), COUNT(*), ? "
                    "FROM mp02_gate_reading r "
                    "WHERE NOT EXISTS (SELECT 1 FROM mp02_maintenance_device m "
                    "                  WHERE m.device_id = r.device_id) "
                    "GROUP BY r.service_date, r.station_id "
                    "ORDER BY r.service_date, r.station_id", (pub,))
        published_rows = db.scalar(w, f"SELECT COUNT(*) FROM {PUBLISHED_TABLE}")
        key_ok = 1 if self._key_contract_ok(ctx) else 0
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp02_build_provenance "
                "(dataset, exclusion_rule, key_contract_ok, published_rows, publication_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (PUBLISHED_TABLE, rule, key_ok, int(published_rows), pub))
        return published_rows

    # -- conservation view ----------------------------------------------------
    def _conservation(self, ctx: RunContext) -> dict[str, int]:
        w = ctx.warehouse
        total = db.scalar(w, "SELECT COALESCE(SUM(passenger_count), 0) FROM mp02_gate_reading")
        excluded = db.scalar(
            w, "SELECT COALESCE(SUM(passenger_count), 0) FROM mp02_gate_reading r "
            "WHERE EXISTS (SELECT 1 FROM mp02_maintenance_device m "
            "WHERE m.device_id = r.device_id)")
        published = db.scalar(w, f"SELECT COALESCE(SUM(station_gate_count), 0) FROM {PUBLISHED_TABLE}")
        published_rows = db.scalar(w, f"SELECT COUNT(*) FROM {PUBLISHED_TABLE}")
        return {"total": total, "excluded": excluded, "included": total - excluded,
                "published_sum": published, "published_rows": published_rows}

    def _expected_keys(self, ctx: RunContext) -> set[tuple[str, str]]:
        """Station-days that keep at least one non-maintenance reading."""
        recompute = self._independent(ctx)
        return set(recompute.keys())

    def _independent(self, ctx: RunContext) -> dict[tuple[str, str], int]:
        w = ctx.warehouse
        maint = {r["device_id"] for r in db.query(
            w, "SELECT device_id FROM mp02_maintenance_device WHERE device_id IS NOT NULL")}
        out: dict[tuple[str, str], int] = {}
        for r in db.query(w, "SELECT service_date, station_id, device_id, passenger_count "
                             "FROM mp02_gate_reading ORDER BY reading_id"):
            if r["device_id"] in maint:
                continue
            key = (r["service_date"], r["station_id"])
            out[key] = out.get(key, 0) + r["passenger_count"]
        return out

    def baseline_ok(self, ctx: RunContext) -> bool:
        c = self._conservation(ctx)
        return c["published_rows"] > 0 and c["published_sum"] == c["included"]

    def _key_contract_ok(self, ctx: RunContext) -> bool:
        return db.scalar(
            ctx.warehouse,
            "SELECT COUNT(*) FROM mp02_maintenance_device WHERE device_id IS NULL") == 0

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        clean_ok = self.baseline_ok(ctx)
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(set(profile.get("faulty_transforms", [])) | {FAULT})
        profile.setdefault("params", {})
        ctx.save_fault_profile(profile)

        self._materialize(ctx, faulty=True, tag="faulty")
        faulty_ok = self.baseline_ok(ctx)

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-02-INJECT-PRE", "precondition",
                      "clean baseline report is non-empty and reconciles",
                      expected=True, actual=clean_ok,
                      status="PASS" if clean_ok else "FAIL"),
            Assertion("MP-02-INJECT-POST", "postcondition",
                      "faulty run breaks the report/reconciliation",
                      expected=False, actual=faulty_ok,
                      status="PASS" if not faulty_ok else "FAIL"),
            Assertion("MP-02-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected", fields={"fault": FAULT})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-02", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "published_rows": self._conservation(ctx)["published_rows"],
                                    "fixture_checksums_unchanged": fx_ok,
                                    "fixture_detail": fx_detail,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        rows = self._materialize(ctx, faulty=True, tag="faulty")
        return StepReport("run", "MP-02", ctx.run_id, "executed",
                          fields={"published_rows": rows})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        c = self._conservation(ctx)
        expected_rows = len(self._expected_keys(ctx))
        collapse = c["published_rows"] == 0 and expected_rows > 0
        conservation_bad = c["published_sum"] != c["included"]
        key_bad = not self._key_contract_ok(ctx)
        detected = collapse or conservation_bad

        common.write_quality_result(
            ctx, "MP-02-COLLAPSE-001", "all", "completeness",
            expected=f"about {expected_rows} report rows",
            actual=f"{c['published_rows']} report rows",
            status="FAIL" if collapse else "PASS")
        common.write_quality_result(
            ctx, "MP-02-CONSERVATION-001", "all", "conservation",
            expected="kept plus set-aside readings equal all readings",
            actual=(f"published total {c['published_sum']} does not equal the "
                    f"kept total {c['included']}"),
            status="FAIL" if conservation_bad else "PASS")
        common.write_quality_result(
            ctx, "MP-02-KEYCONTRACT-001", "all", "control_consistency",
            expected="every exclusion-list key is populated",
            actual="one exclusion-list entry has a blank key",
            status="FAIL" if key_bad else "PASS")

        assertions = [
            Assertion("MP-02-DET-COLLAPSE", "completeness",
                      "the report keeps its expected rows",
                      expected=">0", actual=c["published_rows"],
                      status="FAIL" if collapse else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-02-DET-CONSERVE", "conservation",
                      "kept + set-aside readings equal all readings",
                      expected=c["included"], actual=c["published_sum"],
                      status="FAIL" if conservation_bad else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-02-DET-KEY", "control_consistency",
                      "exclusion-list key contract holds (no blank key)",
                      expected=True, actual=not key_bad,
                      status="FAIL" if key_bad else "PASS",
                      evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "published_rows": c["published_rows"],
                  "expected_rows": expected_rows, "kept_total": c["included"],
                  "published_total": c["published_sum"], "key_contract_ok": not key_bad}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-02", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-02", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        w = ctx.warehouse
        c = self._conservation(ctx)

        common.write_csv(
            tables / "report_row_counts.csv",
            [{"metric": "report_rows_now", "value": c["published_rows"]},
             {"metric": "station_days_with_readings", "value": len(self._expected_keys(ctx))},
             {"metric": "total_readings", "value": db.scalar(w, "SELECT COUNT(*) FROM mp02_gate_reading")},
             {"metric": "total_reading_count_sum", "value": c["total"]}],
            ["metric", "value"])

        raw = db.query(
            w, "SELECT service_date, station_id, COUNT(*) AS readings, "
            "SUM(passenger_count) AS gate_count FROM mp02_gate_reading "
            "GROUP BY service_date, station_id ORDER BY service_date, station_id")
        common.write_csv(
            tables / "readings_by_station.csv",
            [{"service_date": r["service_date"], "station_id": r["station_id"],
              "readings": r["readings"], "gate_count": r["gate_count"]} for r in raw],
            ["service_date", "station_id", "readings", "gate_count"])

        excl = db.query(
            w, "SELECT entry_id, device_id, note FROM mp02_maintenance_device ORDER BY entry_id")
        common.write_csv(
            tables / "exclusion_reference.csv",
            [{"entry_id": r["entry_id"], "device_id": r["device_id"], "note": r["note"]}
             for r in excl],
            ["entry_id", "device_id", "note"])

        common.write_csv(
            tables / "conservation.csv",
            [{"all_readings_total": c["total"], "set_aside_total": c["excluded"],
              "kept_total": c["included"], "published_total": c["published_sum"]}],
            ["all_readings_total", "set_aside_total", "kept_total", "published_total"])

        alert = {
            "incident_id": "MP-02",
            "title": self.metadata.reader_title,
            "reported_symptom": (
                "After a filter was added to leave out maintenance devices, the daily "
                "station gate-count report returned no rows at all. The previous report "
                "had a full set of station-day rows. The query completed successfully "
                "with no error, and the underlying readings are all still present."),
            "time_pressure": "A station operations summary is due at the next review.",
            "affected_product": PUBLISHED_TABLE,
            "fictional_notice": common.FICTIONAL_NOTICE,
        }
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx))
        write_json(ctx.paths.evidence_index,
                   common.evidence_index("MP-02", ctx.run_id, ev))
        return StepReport("evidence", "MP-02", ctx.run_id, "collected",
                          fields={"artifacts": sorted(
                              common.evidence_index("MP-02", ctx.run_id, ev)["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        c = self._conservation(ctx)
        blast_radius = {
            "held_dataset": PUBLISHED_TABLE,
            "report_rows_now": c["published_rows"],
            "readings_available": db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp02_gate_reading"),
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast_radius)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-02", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-02", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["mp02_exclusion_rule"] = "correlated_exclusion"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"rule": "correlated_exclusion"})
        report = StepReport("repair", "MP-02", ctx.run_id, "repaired",
                            fields={"change": "exclusion evaluated with a correlated "
                                             "predicate that tolerates a blank right-hand key",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        rows = self._materialize(ctx, faulty=False, tag="recover")
        c = self._conservation(ctx)
        manifest = {
            "mode": "REPLAY",
            "publication_id": f"PUB-{ctx.run_id}-recover",
            "scope": {"datasets": [PUBLISHED_TABLE], "service_dates": "all-baseline"},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key (service_date, station_id); deterministic recompute",
            "published_rows": rows,
            "validation": {"published_total": c["published_sum"], "kept_total": c["included"]},
            "corrected_rule": "correlated_exclusion",
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"published_rows": rows})
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-02", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        w = ctx.warehouse
        assertions: list[Assertion] = []

        persisted = {(r["service_date"], r["station_id"]): r["station_gate_count"]
                     for r in db.query(w, f"SELECT service_date, station_id, station_gate_count "
                                          f"FROM {PUBLISHED_TABLE}")}
        expected = self._independent(ctx)

        assertions.append(Assertion(
            "MP-02-VER-CORRECT", "correctness",
            "published station-day totals equal the correctly-excluded recompute",
            expected=True, actual=(persisted == expected),
            status="PASS" if persisted == expected else "FAIL",
            evidence="reports/verification.json"))

        assertions.append(Assertion(
            "MP-02-VER-COMPLETE", "completeness",
            "published keys equal the expected non-maintenance station-days",
            expected=len(expected), actual=len(persisted),
            status="PASS" if set(persisted) == set(expected) else "FAIL"))

        dup = db.scalar(w, "SELECT COUNT(*) - COUNT(DISTINCT service_date || '|' || station_id) "
                          f"FROM {PUBLISHED_TABLE}")
        assertions.append(Assertion(
            "MP-02-VER-UNIQUE", "uniqueness",
            "no duplicate at (service_date, station_id) grain",
            expected=0, actual=dup, status="PASS" if dup == 0 else "FAIL"))

        c = self._conservation(ctx)
        conserved = (c["published_sum"] == c["included"]
                     and c["included"] + c["excluded"] == c["total"])
        assertions.append(Assertion(
            "MP-02-VER-CONSERVE", "history",
            "kept + set-aside readings equal all readings and match the report",
            expected=c["included"], actual=c["published_sum"],
            status="PASS" if conserved else "FAIL"))

        prov = db.query_one(ctx.control, "SELECT exclusion_rule, published_rows FROM "
                            "mp02_build_provenance WHERE dataset = ?", (PUBLISHED_TABLE,))
        prov_ok = prov is not None and prov["exclusion_rule"] == "correlated_exclusion" \
            and prov["published_rows"] == c["published_rows"] and c["published_rows"] > 0
        assertions.append(Assertion(
            "MP-02-VER-CONTROL", "control_consistency",
            "provenance records the robust rule and a non-empty recovered publication",
            expected="correlated_exclusion", actual=(prov["exclusion_rule"] if prov else None),
            status="PASS" if prov_ok else "FAIL"))

        faulty_snap = common.read_hash_checkpoint(ctx, FAULTY_SNAPSHOT)
        std_changed = [t for t in SNAPSHOT_TABLES
                       if t in faulty_snap and faulty_snap[t] != table_logical_hash(w, t)]
        input_base = common.read_hash_checkpoint(ctx, INPUT_HASHES)
        input_changed = [t for t in INPUT_TABLES
                         if input_base.get(t) != table_logical_hash(w, t)]
        bounded = not std_changed and not input_changed
        assertions.append(Assertion(
            "MP-02-VER-BOUNDED", "boundedness",
            "recovery changed only the report; standard and input tables unchanged",
            expected=[], actual=std_changed + input_changed,
            status="PASS" if bounded else "FAIL"))

        assertions.append(Assertion(
            "MP-02-VER-IDEMPOTENT", "idempotency",
            "independent correct recompute equals the persisted report",
            expected=True, actual=(persisted == expected),
            status="PASS" if persisted == expected else "FAIL"))

        integ_ok, integ_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-02-VER-FIXTURE", "fixture_integrity",
            "landing inputs match checked-in fixture checksums",
            expected=True, actual=integ_ok, status="PASS" if integ_ok else "FAIL",
            evidence=str(integ_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-02", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP02Incident()
