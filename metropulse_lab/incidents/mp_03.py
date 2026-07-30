"""MP-03 — A few early-hours gate readings land under the neighbouring day.

Reader-facing symptom only: a small number of gate readings taken in the early
hours are grouped under the adjacent day's report, and the effect is larger on
one particular changeover night.

Frozen mechanism (incident_taxonomy.md MP-03): the transform buckets each event
by its UTC calendar date instead of converting to the configured local offset
and applying the operational-day boundary. The reader-facing title and evidence
pack never name the mechanism.

The local-offset schedule is a CHECKED-IN lab fixture
(fixtures/incidents/mp_03/); the bucket function is pure Python and never reads
the host time zone, zoneinfo, or any session setting. The dates are synthetic
test fixtures, NOT a real city incident.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from .. import config, db, paths
from ..checks import integrity
from ..checks.snapshots import table_logical_hash
from ..clock import parse_utc
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchA_common as common
from .contract import IncidentMetadata

FAULT = "mp03_utc_calendar_bucket"
CORRECT_RULE = "local_offset_operating_boundary"
FAULTY_RULE = "raw_calendar_date"
INPUT_HASHES = "mp03_input_hashes.json"
FAULTY_SNAPSHOT = "faulty_snapshots.json"
SNAPSHOT_TABLES = tuple(config.WAREHOUSE_TABLES) + tuple(config.MART_TABLES)
PUBLISHED_TABLE = "mp03_reading_service_day"


def _load_fixture() -> dict[str, Any]:
    path = paths.fixtures_root() / "incidents" / "mp_03" / "bucketing_fixture.json"
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _offset_seconds(instant, periods: list[dict[str, Any]]) -> int:
    for p in periods:
        if parse_utc(p["from_utc"]) <= instant < parse_utc(p["to_utc"]):
            return int(p["offset_seconds"])
    raise ValueError("no offset period covers instant")


def _correct_day(observed_at_utc: str, fixture: dict[str, Any]) -> str:
    instant = parse_utc(observed_at_utc)
    off = _offset_seconds(instant, fixture["offset_periods"])
    local = instant + timedelta(seconds=off)
    boundary = local - timedelta(seconds=int(fixture["operating_day_cutoff_seconds"]))
    return boundary.strftime("%Y-%m-%d")


def _faulty_day(observed_at_utc: str) -> str:
    return observed_at_utc[:10]


class MP03Incident:
    metadata = IncidentMetadata(
        incident_id="MP-03",
        family="A. Present but wrong",
        reader_title=("A handful of early-hours gate readings are grouped under the "
                      "adjacent day, and the shift is larger on one changeover night"),
        primary_invariant_id="MP-03-OPERATING-DAY",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-03-RECONCILE-001", "MP-03-BUCKET-001",
                                "MP-03-PROVENANCE-001"),
        transfer_mappings=("PostgreSQL AT TIME ZONE / Airflow data-interval boundaries "
                           "(transfer note only)",),
        nearest_old_scenario="None; the old books list zones only as a candidate, with no runnable boundary incident.",
        old_scenario_difference=(
            "Not late-arriving data and not a wrong device clock: the events are on "
            "time; only the day each is grouped under is decided by the wrong rule."
        ),
        affected_tables=(PUBLISHED_TABLE,),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        w = ctx.warehouse
        w.execute(
            "CREATE TABLE IF NOT EXISTS mp03_gate_reading ("
            " reading_id TEXT NOT NULL PRIMARY KEY, station_id TEXT NOT NULL,"
            " observed_at_utc TEXT NOT NULL, passenger_count INTEGER NOT NULL)")
        w.execute(
            f"CREATE TABLE IF NOT EXISTS {PUBLISHED_TABLE} ("
            " reading_id TEXT NOT NULL PRIMARY KEY, assigned_service_date TEXT NOT NULL,"
            " passenger_count INTEGER NOT NULL, publication_id TEXT NOT NULL)")
        ctx.control.execute(
            "CREATE TABLE IF NOT EXISTS mp03_operating_calendar ("
            " service_date TEXT NOT NULL PRIMARY KEY, reading_count INTEGER NOT NULL,"
            " passenger_total INTEGER NOT NULL)")
        ctx.control.execute(
            "CREATE TABLE IF NOT EXISTS mp03_bucket_provenance ("
            " dataset TEXT NOT NULL PRIMARY KEY, rule_id TEXT NOT NULL,"
            " rule_version INTEGER NOT NULL, publication_id TEXT NOT NULL)")
        w.commit()
        ctx.control.commit()

        fixture = _load_fixture()
        readings = fixture["readings"]
        with db.transaction(w):
            w.execute("DELETE FROM mp03_gate_reading")
            db.insert_rows(w, "mp03_gate_reading",
                           ("reading_id", "station_id", "observed_at_utc", "passenger_count"),
                           [(r["reading_id"], r["station_id"], r["observed_at_utc"],
                             r["passenger_count"]) for r in readings])

        # Authoritative operating calendar (correct rule), the control reconciliation source.
        calendar: dict[str, list[int]] = {}
        for r in readings:
            day = _correct_day(r["observed_at_utc"], fixture)
            agg = calendar.setdefault(day, [0, 0])
            agg[0] += 1
            agg[1] += r["passenger_count"]
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp03_operating_calendar")
            db.insert_rows(ctx.control, "mp03_operating_calendar",
                           ("service_date", "reading_count", "passenger_total"),
                           [(d, c, t) for d, (c, t) in sorted(calendar.items())])

        self._materialize(ctx, faulty=False, tag="baseline")
        common.write_hash_checkpoint(ctx, INPUT_HASHES, {
            "mp03_gate_reading": table_logical_hash(w, "mp03_gate_reading"),
            "mp03_operating_calendar": table_logical_hash(ctx.control, "mp03_operating_calendar"),
        })

    def _materialize(self, ctx: RunContext, faulty: bool, tag: str) -> int:
        w = ctx.warehouse
        fixture = _load_fixture()
        pub = f"PUB-{ctx.run_id}-{tag}"
        rule = FAULTY_RULE if faulty else CORRECT_RULE
        rows = []
        for r in db.query(w, "SELECT reading_id, observed_at_utc, passenger_count "
                             "FROM mp03_gate_reading ORDER BY reading_id"):
            day = _faulty_day(r["observed_at_utc"]) if faulty \
                else _correct_day(r["observed_at_utc"], fixture)
            rows.append((r["reading_id"], day, r["passenger_count"], pub))
        with db.transaction(w):
            w.execute(f"DELETE FROM {PUBLISHED_TABLE}")
            db.insert_rows(w, PUBLISHED_TABLE,
                           ("reading_id", "assigned_service_date", "passenger_count",
                            "publication_id"), rows)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp03_bucket_provenance "
                "(dataset, rule_id, rule_version, publication_id) VALUES (?, ?, ?, ?)",
                (PUBLISHED_TABLE, rule, 2, pub))
        return len(rows)

    # -- analysis helpers -----------------------------------------------------
    def _misassigned(self, ctx: RunContext) -> list[dict[str, Any]]:
        """Readings whose published day differs from the operating-calendar day."""
        fixture = _load_fixture()
        out = []
        for r in db.query(ctx.warehouse,
                          f"SELECT p.reading_id, g.observed_at_utc, p.assigned_service_date, "
                          f"p.passenger_count FROM {PUBLISHED_TABLE} p "
                          f"JOIN mp03_gate_reading g ON g.reading_id = p.reading_id "
                          f"ORDER BY p.reading_id"):
            correct = _correct_day(r["observed_at_utc"], fixture)
            if r["assigned_service_date"] != correct:
                out.append({"reading_id": r["reading_id"],
                            "observed_at": r["observed_at_utc"],
                            "assigned": r["assigned_service_date"],
                            "expected": correct,
                            "passenger_count": r["passenger_count"]})
        return out

    def _published_by_day(self, ctx: RunContext) -> dict[str, tuple[int, int]]:
        out: dict[str, tuple[int, int]] = {}
        for r in db.query(ctx.warehouse,
                          f"SELECT assigned_service_date AS d, COUNT(*) AS c, "
                          f"SUM(passenger_count) AS t FROM {PUBLISHED_TABLE} "
                          f"GROUP BY assigned_service_date ORDER BY assigned_service_date"):
            out[r["d"]] = (r["c"], r["t"])
        return out

    def _calendar(self, ctx: RunContext) -> dict[str, tuple[int, int]]:
        return {r["service_date"]: (r["reading_count"], r["passenger_total"])
                for r in db.query(ctx.control, "SELECT service_date, reading_count, "
                                  "passenger_total FROM mp03_operating_calendar")}

    def baseline_ok(self, ctx: RunContext) -> bool:
        return len(self._misassigned(ctx)) == 0 and self._published_by_day(ctx) == self._calendar(ctx)

    def _rule_id(self, ctx: RunContext) -> str | None:
        row = db.query_one(ctx.control, "SELECT rule_id FROM mp03_bucket_provenance "
                           "WHERE dataset = ?", (PUBLISHED_TABLE,))
        return row["rule_id"] if row else None

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
            Assertion("MP-03-INJECT-PRE", "precondition",
                      "clean baseline reconciles to the operating calendar",
                      expected=True, actual=clean_ok, status="PASS" if clean_ok else "FAIL"),
            Assertion("MP-03-INJECT-POST", "postcondition",
                      "faulty run misassigns readings vs the operating calendar",
                      expected=False, actual=faulty_ok, status="PASS" if not faulty_ok else "FAIL"),
            Assertion("MP-03-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected", fields={"fault": FAULT})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-03", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "misassigned": len(self._misassigned(ctx)),
                                    "fixture_checksums_unchanged": fx_ok,
                                    "fixture_detail": fx_detail,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        rows = self._materialize(ctx, faulty=True, tag="faulty")
        return StepReport("run", "MP-03", ctx.run_id, "executed",
                          fields={"assigned_rows": rows,
                                  "misassigned": len(self._misassigned(ctx))})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        mis = self._misassigned(ctx)
        published = self._published_by_day(ctx)
        calendar = self._calendar(ctx)
        recon_bad = published != calendar
        rule = self._rule_id(ctx)
        control_bad = rule != CORRECT_RULE
        detected = bool(mis) or recon_bad

        common.write_quality_result(
            ctx, "MP-03-RECONCILE-001", "all", "reconciliation",
            expected="published per-day distribution equals the operating calendar",
            actual=f"{sum(1 for d in set(published)|set(calendar) if published.get(d)!=calendar.get(d))} days differ",
            status="FAIL" if recon_bad else "PASS")
        common.write_quality_result(
            ctx, "MP-03-BUCKET-001", "all", "correctness",
            expected="every reading grouped under its calendar day",
            actual=f"{len(mis)} readings grouped under a different day",
            status="FAIL" if mis else "PASS")
        common.write_quality_result(
            ctx, "MP-03-PROVENANCE-001", "all", "control_consistency",
            expected="grouping rule recorded as the sanctioned rule",
            actual=f"grouping rule recorded as '{rule}'",
            status="FAIL" if control_bad else "PASS")

        assertions = [
            Assertion("MP-03-DET-RECON", "reconciliation",
                      "published day distribution matches the operating calendar",
                      expected=0, actual=len(mis),
                      status="FAIL" if recon_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-03-DET-BUCKET", "correctness",
                      "no reading grouped under the wrong day",
                      expected=0, actual=len(mis),
                      status="FAIL" if mis else "PASS", evidence="reports/detection.json"),
            Assertion("MP-03-DET-CONTROL", "control_consistency",
                      "grouping-rule provenance is the sanctioned rule",
                      expected=CORRECT_RULE, actual=rule,
                      status="FAIL" if control_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "misassigned": len(mis),
                  "reconciliation_days_off": sum(1 for d in set(published) | set(calendar)
                                                 if published.get(d) != calendar.get(d))}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-03", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-03", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        fixture = _load_fixture()

        rows = db.query(ctx.warehouse,
                        f"SELECT p.reading_id, g.observed_at_utc, p.assigned_service_date, "
                        f"p.passenger_count FROM {PUBLISHED_TABLE} p "
                        f"JOIN mp03_gate_reading g ON g.reading_id = p.reading_id "
                        f"ORDER BY p.reading_id")
        assignment = []
        for r in rows:
            correct = _correct_day(r["observed_at_utc"], fixture)
            assignment.append({
                "reading_id": r["reading_id"], "observed_at": r["observed_at_utc"],
                "grouped_day": r["assigned_service_date"], "reference_day": correct,
                "matches": 1 if r["assigned_service_date"] == correct else 0})
        common.write_csv(tables / "reading_grouping.csv", assignment,
                         ["reading_id", "observed_at", "grouped_day", "reference_day", "matches"])

        published = self._published_by_day(ctx)
        calendar = self._calendar(ctx)
        recon = []
        for d in sorted(set(published) | set(calendar)):
            pc, pt = published.get(d, (0, 0))
            cc, ct = calendar.get(d, (0, 0))
            recon.append({"report_day": d, "grouped_readings": pc, "grouped_passengers": pt,
                          "reference_readings": cc, "reference_passengers": ct,
                          "reading_difference": pc - cc})
        common.write_csv(tables / "per_day_reconciliation.csv", recon,
                         ["report_day", "grouped_readings", "grouped_passengers",
                          "reference_readings", "reference_passengers", "reading_difference"])

        mis = self._misassigned(ctx)
        by_ref: dict[str, int] = {}
        for m in mis:
            by_ref[m["expected"]] = by_ref.get(m["expected"], 0) + 1
        common.write_csv(
            tables / "misgrouping_by_reference_day.csv",
            [{"reference_day": d, "misgrouped_readings": n} for d, n in sorted(by_ref.items())],
            ["reference_day", "misgrouped_readings"])

        alert = {
            "incident_id": "MP-03",
            "title": self.metadata.reader_title,
            "reported_symptom": (
                "A small number of gate readings recorded in the early hours are grouped "
                "under the neighbouring day's report instead of the day the reference "
                "calendar assigns them to. The count of misgrouped readings is noticeably "
                "larger on one particular changeover night. The events themselves arrived "
                "on time and parsed cleanly."),
            "time_pressure": "A daily station report is published each morning.",
            "affected_product": PUBLISHED_TABLE,
            "fictional_notice": (common.FICTIONAL_NOTICE + " The dates here are synthetic "
                                 "test fixtures, not a real city incident."),
        }
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx, include_logical_time=False))
        write_json(ctx.paths.evidence_index, common.evidence_index("MP-03", ctx.run_id, ev))
        return StepReport("evidence", "MP-03", ctx.run_id, "collected",
                          fields={"misgrouped": len(mis)})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        mis = self._misassigned(ctx)
        blast_radius = {
            "held_dataset": PUBLISHED_TABLE,
            "misgrouped_readings": len(mis),
            "affected_reference_days": sorted({m["expected"] for m in mis}),
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields={"misgrouped": len(mis)})
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-03", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-03", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["mp03_bucket_rule"] = CORRECT_RULE
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"rule": CORRECT_RULE})
        report = StepReport("repair", "MP-03", ctx.run_id, "repaired",
                            fields={"change": "each reading converted to the configured local "
                                             "offset and grouped by the operating-day boundary "
                                             "before the calendar date is taken",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        rows = self._materialize(ctx, faulty=False, tag="recover")
        mis = len(self._misassigned(ctx))
        manifest = {
            "mode": "REPLAY",
            "publication_id": f"PUB-{ctx.run_id}-recover",
            "scope": {"datasets": [PUBLISHED_TABLE], "readings": rows},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key reading_id; deterministic recompute",
            "validation": {"misgrouped_after": mis},
            "corrected_rule": CORRECT_RULE,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"reassigned": rows})
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-03", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        w = ctx.warehouse
        assertions: list[Assertion] = []

        mis = self._misassigned(ctx)
        assertions.append(Assertion(
            "MP-03-VER-CORRECT", "correctness",
            "every reading grouped under its operating-calendar day",
            expected=0, actual=len(mis), status="PASS" if not mis else "FAIL",
            evidence="reports/verification.json"))

        total_readings = db.scalar(w, "SELECT COUNT(*) FROM mp03_gate_reading")
        assigned = db.scalar(w, f"SELECT COUNT(*) FROM {PUBLISHED_TABLE}")
        calendar_days = set(self._calendar(ctx))
        published_days = set(self._published_by_day(ctx))
        assertions.append(Assertion(
            "MP-03-VER-COMPLETE", "completeness",
            "all readings assigned and every calendar day covered",
            expected=f"{total_readings} readings / {len(calendar_days)} days",
            actual=f"{assigned} readings / {len(published_days)} days",
            status="PASS" if assigned == total_readings and published_days == calendar_days else "FAIL"))

        dup = db.scalar(w, f"SELECT COUNT(*) - COUNT(DISTINCT reading_id) FROM {PUBLISHED_TABLE}")
        assertions.append(Assertion(
            "MP-03-VER-UNIQUE", "uniqueness", "no duplicate reading_id",
            expected=0, actual=dup, status="PASS" if dup == 0 else "FAIL"))

        pub_total = db.scalar(w, f"SELECT COALESCE(SUM(passenger_count), 0) FROM {PUBLISHED_TABLE}")
        raw_total = db.scalar(w, "SELECT COALESCE(SUM(passenger_count), 0) FROM mp03_gate_reading")
        recon_ok = self._published_by_day(ctx) == self._calendar(ctx)
        assertions.append(Assertion(
            "MP-03-VER-CONSERVE", "history",
            "passengers preserved and per-day distribution equals the calendar",
            expected=raw_total, actual=pub_total,
            status="PASS" if pub_total == raw_total and recon_ok else "FAIL"))

        rule = self._rule_id(ctx)
        assertions.append(Assertion(
            "MP-03-VER-CONTROL", "control_consistency",
            "grouping-rule provenance records the sanctioned rule",
            expected=CORRECT_RULE, actual=rule, status="PASS" if rule == CORRECT_RULE else "FAIL"))

        faulty_snap = common.read_hash_checkpoint(ctx, FAULTY_SNAPSHOT)
        std_changed = [t for t in SNAPSHOT_TABLES
                       if t in faulty_snap and faulty_snap[t] != table_logical_hash(w, t)]
        base = common.read_hash_checkpoint(ctx, INPUT_HASHES)
        input_changed = []
        if base.get("mp03_gate_reading") != table_logical_hash(w, "mp03_gate_reading"):
            input_changed.append("mp03_gate_reading")
        if base.get("mp03_operating_calendar") != table_logical_hash(ctx.control, "mp03_operating_calendar"):
            input_changed.append("mp03_operating_calendar")
        bounded = not std_changed and not input_changed
        assertions.append(Assertion(
            "MP-03-VER-BOUNDED", "boundedness",
            "recovery changed only the grouping; standard, input and calendar tables unchanged",
            expected=[], actual=std_changed + input_changed,
            status="PASS" if bounded else "FAIL"))

        fixture = _load_fixture()
        recomputed = {r["reading_id"]: _correct_day(r["observed_at_utc"], fixture)
                      for r in db.query(w, "SELECT reading_id, observed_at_utc FROM mp03_gate_reading")}
        persisted = {r["reading_id"]: r["assigned_service_date"]
                     for r in db.query(w, f"SELECT reading_id, assigned_service_date FROM {PUBLISHED_TABLE}")}
        assertions.append(Assertion(
            "MP-03-VER-IDEMPOTENT", "idempotency",
            "independent recompute equals the persisted grouping",
            expected=True, actual=(recomputed == persisted),
            status="PASS" if recomputed == persisted else "FAIL"))

        integ_ok, integ_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-03-VER-FIXTURE", "fixture_integrity",
            "landing inputs match checked-in fixture checksums",
            expected=True, actual=integ_ok, status="PASS" if integ_ok else "FAIL",
            evidence=str(integ_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-03", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP03Incident()
