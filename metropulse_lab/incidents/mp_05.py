"""MP-05 — A batch of vehicle positions resolves to a date long before service.

Reader-facing symptom only: a batch of vehicle-position timestamps resolves to a
date decades before the service ever existed. The raw values are non-empty and
parse without error.

Frozen mechanism (incident_taxonomy.md MP-05): the raw integer is interpreted
with the wrong time unit relative to its versioned source contract; both the
schema shape and the parser "succeed", but the resolved instant is wrong. The
reader-facing title and evidence pack never name the mechanism.

The parser is pure Python driven by a CHECKED-IN, versioned source contract
(fixtures/incidents/mp_05/). It never reads the host clock. The values are
synthetic lab fixtures, not a real incident.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from .. import config, db, paths
from ..checks import integrity
from ..checks.snapshots import table_logical_hash
from ..clock import format_utc, parse_utc
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchA_common as common
from .contract import IncidentMetadata

FAULT = "mp05_wrong_time_unit"
INPUT_HASHES = "mp05_input_hashes.json"
FAULTY_SNAPSHOT = "faulty_snapshots.json"
SNAPSHOT_TABLES = tuple(config.WAREHOUSE_TABLES) + tuple(config.MART_TABLES)
PUBLISHED_TABLE = "mp05_vehicle_position"
CONTRACT_UNIT = "epoch_seconds"
WRONG_UNIT = "epoch_milliseconds"

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _load_fixture() -> dict[str, Any]:
    path = paths.fixtures_root() / "incidents" / "mp_05" / "epoch_fixture.json"
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _to_instant(raw: int, unit: str) -> datetime:
    """Unit-aware pure-Python interpretation of a raw integer time value."""
    if unit == "epoch_seconds":
        whole_seconds = raw
    elif unit == "epoch_milliseconds":
        whole_seconds = raw // 1000
    else:
        raise ValueError(f"unknown time unit: {unit}")
    return _EPOCH + timedelta(seconds=whole_seconds)


def _resolve(raw: int, unit: str) -> str:
    return format_utc(_to_instant(raw, unit))


def _in_window(observed_utc: str, contract: dict[str, Any]) -> bool:
    instant = parse_utc(observed_utc)
    return parse_utc(contract["valid_from_utc"]) <= instant < parse_utc(contract["valid_to_utc"])


class MP05Incident:
    metadata = IncidentMetadata(
        incident_id="MP-05",
        family="A. Present but wrong",
        reader_title=("A batch of vehicle-position timestamps resolves to a date long "
                      "before the service existed, though the raw values parse cleanly"),
        primary_invariant_id="MP-05-RANGE",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-05-RANGE-001", "MP-05-COVERAGE-001",
                                "MP-05-PARSEPROV-001"),
        transfer_mappings=("PostgreSQL Unix-epoch conversion; source unit stays governed "
                           "by the MetroPulse contract (transfer note only)",),
        nearest_old_scenario="None; not the old late-data scenario — the event time is misread, not late.",
        old_scenario_difference=(
            "Not a drifting device clock and not late delivery: the values arrived on "
            "time and parse, but are interpreted against the wrong contract unit."
        ),
        affected_tables=(PUBLISHED_TABLE,),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        w = ctx.warehouse
        w.execute(
            "CREATE TABLE IF NOT EXISTS mp05_position_raw ("
            " position_event_id TEXT NOT NULL PRIMARY KEY, vehicle_id TEXT NOT NULL,"
            " epoch_raw INTEGER NOT NULL)")
        w.execute(
            f"CREATE TABLE IF NOT EXISTS {PUBLISHED_TABLE} ("
            " position_event_id TEXT NOT NULL PRIMARY KEY, vehicle_id TEXT NOT NULL,"
            " observed_at_utc TEXT NOT NULL, in_window INTEGER NOT NULL,"
            " publication_id TEXT NOT NULL)")
        ctx.control.execute(
            "CREATE TABLE IF NOT EXISTS mp05_source_contract ("
            " source_name TEXT NOT NULL PRIMARY KEY, unit TEXT NOT NULL,"
            " unit_version INTEGER NOT NULL, valid_from_utc TEXT NOT NULL,"
            " valid_to_utc TEXT NOT NULL)")
        ctx.control.execute(
            "CREATE TABLE IF NOT EXISTS mp05_parse_provenance ("
            " dataset TEXT NOT NULL PRIMARY KEY, parse_unit TEXT NOT NULL,"
            " unit_version INTEGER NOT NULL, range_gate_failures INTEGER NOT NULL,"
            " publication_id TEXT NOT NULL)")
        w.commit()
        ctx.control.commit()

        fixture = _load_fixture()
        contract = fixture["source_contract"]
        with db.transaction(w):
            w.execute("DELETE FROM mp05_position_raw")
            db.insert_rows(w, "mp05_position_raw",
                           ("position_event_id", "vehicle_id", "epoch_raw"),
                           [(r["position_event_id"], r["vehicle_id"], r["epoch_raw"])
                            for r in fixture["position_batch"]])
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp05_source_contract")
            ctx.control.execute(
                "INSERT INTO mp05_source_contract "
                "(source_name, unit, unit_version, valid_from_utc, valid_to_utc) "
                "VALUES (?, ?, ?, ?, ?)",
                (contract["source_name"], contract["unit"], contract["unit_version"],
                 contract["valid_from_utc"], contract["valid_to_utc"]))

        self._materialize(ctx, faulty=False, tag="baseline")
        common.write_hash_checkpoint(ctx, INPUT_HASHES, {
            "mp05_position_raw": table_logical_hash(w, "mp05_position_raw"),
            "mp05_source_contract": table_logical_hash(ctx.control, "mp05_source_contract"),
        })

    def _contract(self, ctx: RunContext) -> dict[str, Any]:
        row = db.query_one(ctx.control, "SELECT unit, unit_version, valid_from_utc, "
                           "valid_to_utc FROM mp05_source_contract")
        return dict(row)

    def _materialize(self, ctx: RunContext, faulty: bool, tag: str) -> int:
        w = ctx.warehouse
        contract = self._contract(ctx)
        unit = WRONG_UNIT if faulty else contract["unit"]
        pub = f"PUB-{ctx.run_id}-{tag}"
        rows = []
        failures = 0
        for r in db.query(w, "SELECT position_event_id, vehicle_id, epoch_raw "
                             "FROM mp05_position_raw ORDER BY position_event_id"):
            observed = _resolve(r["epoch_raw"], unit)
            ok = _in_window(observed, contract)
            if not ok:
                failures += 1
            rows.append((r["position_event_id"], r["vehicle_id"], observed,
                         1 if ok else 0, pub))
        with db.transaction(w):
            w.execute(f"DELETE FROM {PUBLISHED_TABLE}")
            db.insert_rows(w, PUBLISHED_TABLE,
                           ("position_event_id", "vehicle_id", "observed_at_utc",
                            "in_window", "publication_id"), rows)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp05_parse_provenance "
                "(dataset, parse_unit, unit_version, range_gate_failures, publication_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (PUBLISHED_TABLE, unit, contract["unit_version"], failures, pub))
        return len(rows)

    # -- analysis -------------------------------------------------------------
    def _expected_dates(self, ctx: RunContext) -> set[str]:
        """Service dates implied by the batch under the contract unit."""
        contract = self._contract(ctx)
        out = set()
        for r in db.query(ctx.warehouse, "SELECT epoch_raw FROM mp05_position_raw"):
            out.add(_resolve(r["epoch_raw"], contract["unit"])[:10])
        return out

    def _published_dates(self, ctx: RunContext) -> set[str]:
        return {r["observed_at_utc"][:10] for r in db.query(
            ctx.warehouse, f"SELECT observed_at_utc FROM {PUBLISHED_TABLE} WHERE in_window = 1")}

    def _out_of_window(self, ctx: RunContext) -> int:
        return db.scalar(ctx.warehouse, f"SELECT COUNT(*) FROM {PUBLISHED_TABLE} WHERE in_window = 0")

    def baseline_ok(self, ctx: RunContext) -> bool:
        return self._out_of_window(ctx) == 0 and self._published_dates(ctx) == self._expected_dates(ctx)

    def _parse_unit(self, ctx: RunContext) -> str | None:
        row = db.query_one(ctx.control, "SELECT parse_unit FROM mp05_parse_provenance "
                           "WHERE dataset = ?", (PUBLISHED_TABLE,))
        return row["parse_unit"] if row else None

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
            Assertion("MP-05-INJECT-PRE", "precondition",
                      "clean baseline positions all fall inside the valid window",
                      expected=True, actual=clean_ok, status="PASS" if clean_ok else "FAIL"),
            Assertion("MP-05-INJECT-POST", "postcondition",
                      "faulty run pushes the batch outside the valid window",
                      expected=False, actual=faulty_ok, status="PASS" if not faulty_ok else "FAIL"),
            Assertion("MP-05-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected", fields={"fault": FAULT})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-05", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "out_of_window": self._out_of_window(ctx),
                                    "fixture_checksums_unchanged": fx_ok,
                                    "fixture_detail": fx_detail,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        rows = self._materialize(ctx, faulty=True, tag="faulty")
        return StepReport("run", "MP-05", ctx.run_id, "executed",
                          fields={"positions": rows, "out_of_window": self._out_of_window(ctx)})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        out_of_window = self._out_of_window(ctx)
        expected = self._expected_dates(ctx)
        published = self._published_dates(ctx)
        coverage_bad = published != expected
        parse_unit = self._parse_unit(ctx)
        contract = self._contract(ctx)
        control_bad = parse_unit != contract["unit"] or out_of_window > 0
        detected = out_of_window > 0 or coverage_bad

        common.write_quality_result(
            ctx, "MP-05-RANGE-001", "all", "range",
            expected="every position resolves inside the valid window",
            actual=f"{out_of_window} positions resolve outside the valid window",
            status="FAIL" if out_of_window else "PASS")
        common.write_quality_result(
            ctx, "MP-05-COVERAGE-001", "all", "completeness",
            expected=f"{len(expected)} operating days covered",
            actual=f"{len(published)} operating days covered",
            status="FAIL" if coverage_bad else "PASS")
        common.write_quality_result(
            ctx, "MP-05-PARSEPROV-001", "all", "control_consistency",
            expected="parse rule matches the source contract and the range gate passes",
            actual="the parse rule disagrees with the source contract",
            status="FAIL" if control_bad else "PASS")

        assertions = [
            Assertion("MP-05-DET-RANGE", "range", "all positions inside the valid window",
                      expected=0, actual=out_of_window,
                      status="FAIL" if out_of_window else "PASS", evidence="reports/detection.json"),
            Assertion("MP-05-DET-COVERAGE", "completeness",
                      "operating-day coverage is intact",
                      expected=len(expected), actual=len(published),
                      status="FAIL" if coverage_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-05-DET-CONTROL", "control_consistency",
                      "parse provenance matches the source contract and the range gate passes",
                      expected=contract["unit"], actual=parse_unit,
                      status="FAIL" if control_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "out_of_window": out_of_window,
                  "expected_days": len(expected), "covered_days": len(published)}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-05", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-05", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        w = ctx.warehouse
        contract = self._contract(ctx)

        # Raw value + window flag only. The out-of-window resolved instants are NOT
        # printed (they would hand the reader the answer); the raw magnitude and the
        # window bounds are the facts a junior collects.
        checks = db.query(
            w, f"SELECT r.position_event_id, r.vehicle_id, r.epoch_raw, p.in_window "
            f"FROM mp05_position_raw r JOIN {PUBLISHED_TABLE} p "
            f"ON p.position_event_id = r.position_event_id ORDER BY r.position_event_id")
        common.write_csv(
            tables / "batch_window_check.csv",
            [{"position_event_id": r["position_event_id"], "vehicle_id": r["vehicle_id"],
              "raw_value": r["epoch_raw"], "within_valid_window": r["in_window"]}
             for r in checks],
            ["position_event_id", "vehicle_id", "raw_value", "within_valid_window"])

        common.write_csv(
            tables / "valid_window.csv",
            [{"window_start": contract["valid_from_utc"], "window_end": contract["valid_to_utc"]}],
            ["window_start", "window_end"])

        expected = sorted(self._expected_dates(ctx))
        common.write_csv(
            tables / "operating_day_coverage.csv",
            [{"expected_operating_day": d,
              "positions_landing_on_day": db.scalar(
                  w, f"SELECT COUNT(*) FROM {PUBLISHED_TABLE} "
                     f"WHERE in_window = 1 AND substr(observed_at_utc, 1, 10) = ?", (d,))}
             for d in expected],
            ["expected_operating_day", "positions_landing_on_day"])

        total = db.scalar(w, "SELECT COUNT(*) FROM mp05_position_raw")
        within = db.scalar(w, f"SELECT COUNT(*) FROM {PUBLISHED_TABLE} WHERE in_window = 1")
        common.write_csv(
            tables / "batch_summary.csv",
            [{"total_positions": total, "within_valid_window": within,
              "outside_valid_window": total - within}],
            ["total_positions", "within_valid_window", "outside_valid_window"])

        alert = {
            "incident_id": "MP-05",
            "title": self.metadata.reader_title,
            "reported_symptom": (
                "A batch of vehicle positions carries timestamps that fall far outside "
                "the valid window; they resolve to a date long before the service ever "
                "existed. The raw values are all present, are numeric, and were accepted "
                "by the parser without error, so the batch covers none of the expected "
                "operating days."),
            "time_pressure": "Live vehicle-position coverage feeds an operations view.",
            "affected_product": PUBLISHED_TABLE,
            "fictional_notice": common.FICTIONAL_NOTICE,
        }
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx))
        write_json(ctx.paths.evidence_index, common.evidence_index("MP-05", ctx.run_id, ev))
        return StepReport("evidence", "MP-05", ctx.run_id, "collected",
                          fields={"out_of_window": self._out_of_window(ctx)})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        blast_radius = {
            "held_dataset": PUBLISHED_TABLE,
            "positions_outside_window": self._out_of_window(ctx),
            "covered_operating_days": len(self._published_dates(ctx)),
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast_radius)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-05", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-05", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["mp05_parse_unit"] = CONTRACT_UNIT
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"parse_rule": "source_contract"})
        report = StepReport("repair", "MP-05", ctx.run_id, "repaired",
                            fields={"change": "raw time values are interpreted with the unit "
                                             "declared by the versioned source contract, then "
                                             "range-validated before commit",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        # Reparse under the contract unit and range-validate BEFORE committing.
        contract = self._contract(ctx)
        staged = []
        for r in db.query(ctx.warehouse, "SELECT position_event_id, vehicle_id, epoch_raw "
                          "FROM mp05_position_raw ORDER BY position_event_id"):
            observed = _resolve(r["epoch_raw"], contract["unit"])
            staged.append((r["position_event_id"], observed, _in_window(observed, contract)))
        rejected = [pid for pid, _obs, ok in staged if not ok]
        rows = self._materialize(ctx, faulty=False, tag="recover")
        manifest = {
            "mode": "REPLAY",
            "publication_id": f"PUB-{ctx.run_id}-recover",
            "scope": {"datasets": [PUBLISHED_TABLE], "positions": rows},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key position_event_id; deterministic recompute",
            "validation": {"range_gate_rejections_before_commit": len(rejected),
                           "out_of_window_after": self._out_of_window(ctx)},
            "corrected_parse_unit": CONTRACT_UNIT,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"positions": rows})
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-05", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        w = ctx.warehouse
        contract = self._contract(ctx)
        assertions: list[Assertion] = []

        expected = {r["position_event_id"]: _resolve(r["epoch_raw"], contract["unit"])
                    for r in db.query(w, "SELECT position_event_id, epoch_raw FROM mp05_position_raw")}
        persisted = {r["position_event_id"]: r["observed_at_utc"]
                     for r in db.query(w, f"SELECT position_event_id, observed_at_utc FROM {PUBLISHED_TABLE}")}
        assertions.append(Assertion(
            "MP-05-VER-CORRECT", "correctness",
            "resolved timestamps equal the contract-unit interpretation",
            expected=True, actual=(expected == persisted),
            status="PASS" if expected == persisted else "FAIL", evidence="reports/verification.json"))

        out_of_window = self._out_of_window(ctx)
        assertions.append(Assertion(
            "MP-05-VER-RANGE", "range", "every position resolves inside the valid window",
            expected=0, actual=out_of_window, status="PASS" if out_of_window == 0 else "FAIL"))

        covered = self._published_dates(ctx)
        expected_days = self._expected_dates(ctx)
        total = db.scalar(w, "SELECT COUNT(*) FROM mp05_position_raw")
        published_rows = db.scalar(w, f"SELECT COUNT(*) FROM {PUBLISHED_TABLE}")
        assertions.append(Assertion(
            "MP-05-VER-COMPLETE", "completeness",
            "all positions present and operating-day coverage restored",
            expected=f"{total} positions / {len(expected_days)} days",
            actual=f"{published_rows} positions / {len(covered)} days",
            status="PASS" if published_rows == total and covered == expected_days else "FAIL"))

        dup = db.scalar(w, f"SELECT COUNT(*) - COUNT(DISTINCT position_event_id) FROM {PUBLISHED_TABLE}")
        assertions.append(Assertion(
            "MP-05-VER-UNIQUE", "uniqueness", "no duplicate position_event_id",
            expected=0, actual=dup, status="PASS" if dup == 0 else "FAIL"))

        assertions.append(Assertion(
            "MP-05-VER-CONSERVE", "history",
            "no position dropped or added during recovery",
            expected=total, actual=published_rows, status="PASS" if published_rows == total else "FAIL"))

        parse_unit = self._parse_unit(ctx)
        prov = db.query_one(ctx.control, "SELECT range_gate_failures FROM mp05_parse_provenance "
                            "WHERE dataset = ?", (PUBLISHED_TABLE,))
        prov_ok = parse_unit == contract["unit"] and prov is not None and prov["range_gate_failures"] == 0
        assertions.append(Assertion(
            "MP-05-VER-CONTROL", "control_consistency",
            "parse provenance matches the contract unit and the range gate is clean",
            expected=contract["unit"], actual=parse_unit, status="PASS" if prov_ok else "FAIL"))

        faulty_snap = common.read_hash_checkpoint(ctx, FAULTY_SNAPSHOT)
        std_changed = [t for t in SNAPSHOT_TABLES
                       if t in faulty_snap and faulty_snap[t] != table_logical_hash(w, t)]
        base = common.read_hash_checkpoint(ctx, INPUT_HASHES)
        input_changed = []
        if base.get("mp05_position_raw") != table_logical_hash(w, "mp05_position_raw"):
            input_changed.append("mp05_position_raw")
        if base.get("mp05_source_contract") != table_logical_hash(ctx.control, "mp05_source_contract"):
            input_changed.append("mp05_source_contract")
        bounded = not std_changed and not input_changed
        assertions.append(Assertion(
            "MP-05-VER-BOUNDED", "boundedness",
            "recovery changed only the position output; standard, raw and contract tables unchanged",
            expected=[], actual=std_changed + input_changed,
            status="PASS" if bounded else "FAIL"))

        assertions.append(Assertion(
            "MP-05-VER-IDEMPOTENT", "idempotency",
            "independent contract-unit reparse equals the persisted output",
            expected=True, actual=(expected == persisted),
            status="PASS" if expected == persisted else "FAIL"))

        integ_ok, integ_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-05-VER-FIXTURE", "fixture_integrity",
            "landing inputs match checked-in fixture checksums",
            expected=True, actual=integ_ok, status="PASS" if integ_ok else "FAIL",
            evidence=str(integ_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-05", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP05Incident()
