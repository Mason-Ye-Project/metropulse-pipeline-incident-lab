"""MP-04 — A monthly route distance total comes out negative.

Reader-facing symptom only: every daily per-vehicle distance is non-negative,
yet the monthly route-distance total is negative.

Frozen mechanism (incident_taxonomy.md MP-04): the monthly aggregate is summed
through a narrow signed 32-bit intermediate that rolls past its maximum and
flips sign. The reader-facing title and evidence pack never name the mechanism.

The narrow accumulator is an EXPLICIT, LABELED pure-Python simulator that runs
beside the mathematically-correct wide-integer (Python int) truth. SQLite is
never claimed to have rolled over; the roll-over lives only in the labeled
Python accumulator below.
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

FAULT = "mp04_narrow_accumulator"
NARROW_WIDTH = "signed_32bit"          # the labeled narrow simulator
WIDE_WIDTH = "wide_integer"            # the correct Python-int truth
INPUT_TABLES = ("mp04_daily_distance",)
INPUT_HASHES = "mp04_input_hashes.json"
FAULTY_SNAPSHOT = "faulty_snapshots.json"
SNAPSHOT_TABLES = tuple(config.WAREHOUSE_TABLES) + tuple(config.MART_TABLES)
PUBLISHED_TABLE = "mp04_monthly_route_distance"

MONTH = "2026-04"
DAYS = 30
# Per-route daily magnitude (lab values). R1/R2 sums exceed the narrow maximum;
# R3/R4 stay inside it, so the fault is visibly route-specific.
ROUTE_DAILY_BASE = {"R1": 91_000_000, "R2": 72_000_000, "R3": 58_000_000, "R4": 40_000_000}

# --- the explicit, labeled narrow signed-32-bit accumulator simulator -------
_NARROW_MODULUS = 2 ** 32
_NARROW_OFFSET = 2 ** 31


def _narrow_wrap(value: int) -> int:
    """Fold an integer into the signed 32-bit range (two's-complement)."""
    return ((value + _NARROW_OFFSET) % _NARROW_MODULUS) - _NARROW_OFFSET


def _accumulate_narrow(values: list[int]) -> int:
    """Sum through the labeled narrow accumulator (can roll past its maximum)."""
    acc = 0
    for v in values:
        acc = _narrow_wrap(acc + v)
    return acc


def _accumulate_wide(values: list[int]) -> int:
    """The correct wide-integer truth (Python int; unbounded)."""
    total = 0
    for v in values:
        total += v
    return total


class MP04Incident:
    metadata = IncidentMetadata(
        incident_id="MP-04",
        family="A. Present but wrong",
        reader_title=("A monthly route-distance total is negative even though every "
                      "daily per-vehicle distance is non-negative"),
        primary_invariant_id="MP-04-RANGE",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-04-RANGE-001", "MP-04-CONSERVATION-001",
                                "MP-04-TYPEPROV-001"),
        transfer_mappings=("PostgreSQL integer/bigint range and overflow-on-out-of-range "
                           "behaviour (transfer note only)",),
        nearest_old_scenario="None; the old books have no numeric range incident and use no money fields.",
        old_scenario_difference=(
            "Not a reversed odometer and not a correction record: the daily detail is "
            "monotone and non-negative; only the aggregate intermediate loses range."
        ),
        affected_tables=(PUBLISHED_TABLE,),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        w = ctx.warehouse
        w.execute(
            "CREATE TABLE IF NOT EXISTS mp04_daily_distance ("
            " service_date TEXT NOT NULL, route_id TEXT NOT NULL, vehicle_id TEXT NOT NULL,"
            " distance_m INTEGER NOT NULL,"
            " PRIMARY KEY (service_date, route_id, vehicle_id))")
        w.execute(
            f"CREATE TABLE IF NOT EXISTS {PUBLISHED_TABLE} ("
            " month TEXT NOT NULL, route_id TEXT NOT NULL, total_distance_m INTEGER NOT NULL,"
            " publication_id TEXT NOT NULL, PRIMARY KEY (month, route_id))")
        ctx.control.execute(
            "CREATE TABLE IF NOT EXISTS mp04_type_provenance ("
            " dataset TEXT NOT NULL PRIMARY KEY, accumulator_width TEXT NOT NULL,"
            " publication_id TEXT NOT NULL)")
        w.commit()
        ctx.control.commit()

        rows = []
        for route, base in ROUTE_DAILY_BASE.items():
            vehicle = config.ROUTE_VEHICLE[route]
            for i in range(DAYS):
                service_date = f"{MONTH}-{i + 1:02d}"
                distance = base + (i * 7919) % 1000  # deterministic, non-negative
                rows.append((service_date, route, vehicle, distance))
        with db.transaction(w):
            w.execute("DELETE FROM mp04_daily_distance")
            db.insert_rows(w, "mp04_daily_distance",
                           ("service_date", "route_id", "vehicle_id", "distance_m"), rows)

        self._materialize(ctx, faulty=False, tag="baseline")
        common.write_hash_checkpoint(
            ctx, INPUT_HASHES, common.capture_table_hashes(w, INPUT_TABLES))

    def _daily_by_route(self, ctx: RunContext) -> dict[str, list[int]]:
        out: dict[str, list[int]] = {}
        for r in db.query(ctx.warehouse,
                          "SELECT route_id, distance_m FROM mp04_daily_distance "
                          "ORDER BY route_id, service_date"):
            out.setdefault(r["route_id"], []).append(r["distance_m"])
        return out

    def _materialize(self, ctx: RunContext, faulty: bool, tag: str) -> int:
        w = ctx.warehouse
        pub = f"PUB-{ctx.run_id}-{tag}"
        width = NARROW_WIDTH if faulty else WIDE_WIDTH
        rows = []
        for route, values in sorted(self._daily_by_route(ctx).items()):
            total = _accumulate_narrow(values) if faulty else _accumulate_wide(values)
            rows.append((MONTH, route, total, pub))
        with db.transaction(w):
            w.execute(f"DELETE FROM {PUBLISHED_TABLE}")
            db.insert_rows(w, PUBLISHED_TABLE,
                           ("month", "route_id", "total_distance_m", "publication_id"), rows)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp04_type_provenance "
                "(dataset, accumulator_width, publication_id) VALUES (?, ?, ?)",
                (PUBLISHED_TABLE, width, pub))
        return len(rows)

    # -- analysis -------------------------------------------------------------
    def _published(self, ctx: RunContext) -> dict[str, int]:
        return {r["route_id"]: r["total_distance_m"]
                for r in db.query(ctx.warehouse,
                                  f"SELECT route_id, total_distance_m FROM {PUBLISHED_TABLE}")}

    def _truth(self, ctx: RunContext) -> dict[str, int]:
        return {route: _accumulate_wide(values)
                for route, values in self._daily_by_route(ctx).items()}

    def baseline_ok(self, ctx: RunContext) -> bool:
        pub = self._published(ctx)
        truth = self._truth(ctx)
        return pub == truth and all(v >= 0 for v in pub.values())

    def _width(self, ctx: RunContext) -> str | None:
        row = db.query_one(ctx.control, "SELECT accumulator_width FROM mp04_type_provenance "
                           "WHERE dataset = ?", (PUBLISHED_TABLE,))
        return row["accumulator_width"] if row else None

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
            Assertion("MP-04-INJECT-PRE", "precondition",
                      "clean baseline totals are non-negative and match the daily detail",
                      expected=True, actual=clean_ok, status="PASS" if clean_ok else "FAIL"),
            Assertion("MP-04-INJECT-POST", "postcondition",
                      "faulty run produces a negative or mismatched monthly total",
                      expected=False, actual=faulty_ok, status="PASS" if not faulty_ok else "FAIL"),
            Assertion("MP-04-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection",
                      expected=True, actual=fx_ok, status="PASS" if fx_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected", fields={"fault": FAULT})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-04", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "negative_routes": sum(1 for v in self._published(ctx).values() if v < 0),
                                    "fixture_checksums_unchanged": fx_ok,
                                    "fixture_detail": fx_detail,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        rows = self._materialize(ctx, faulty=True, tag="faulty")
        return StepReport("run", "MP-04", ctx.run_id, "executed",
                          fields={"monthly_rows": rows,
                                  "negative_routes": sum(1 for v in self._published(ctx).values() if v < 0)})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        pub = self._published(ctx)
        truth = self._truth(ctx)
        negative = sorted(r for r, v in pub.items() if v < 0)
        mismatched = sorted(r for r in pub if pub[r] != truth.get(r))
        width = self._width(ctx)
        control_bad = width != WIDE_WIDTH
        detected = bool(negative) or bool(mismatched)

        common.write_quality_result(
            ctx, "MP-04-RANGE-001", "all", "range",
            expected="every monthly route total is non-negative",
            actual=f"{len(negative)} routes have a negative monthly total",
            status="FAIL" if negative else "PASS")
        common.write_quality_result(
            ctx, "MP-04-CONSERVATION-001", "all", "conservation",
            expected="monthly total equals the sum of that route's daily distances",
            actual=f"{len(mismatched)} routes disagree with the daily detail",
            status="FAIL" if mismatched else "PASS")
        common.write_quality_result(
            ctx, "MP-04-TYPEPROV-001", "all", "control_consistency",
            expected="aggregate accumulator covers the proven maximum range",
            actual=f"aggregate accumulator recorded as '{width}'",
            status="FAIL" if control_bad else "PASS")

        assertions = [
            Assertion("MP-04-DET-RANGE", "range", "no negative monthly total",
                      expected=0, actual=len(negative),
                      status="FAIL" if negative else "PASS", evidence="reports/detection.json"),
            Assertion("MP-04-DET-CONSERVE", "conservation",
                      "monthly totals equal the summed daily detail",
                      expected=0, actual=len(mismatched),
                      status="FAIL" if mismatched else "PASS", evidence="reports/detection.json"),
            Assertion("MP-04-DET-CONTROL", "control_consistency",
                      "aggregate accumulator provenance covers the proven range",
                      expected=WIDE_WIDTH, actual=width,
                      status="FAIL" if control_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "negative_routes": negative,
                  "mismatched_routes": mismatched}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-04", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-04", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        w = ctx.warehouse

        summary = db.query(
            w, "SELECT route_id, COUNT(*) AS days, MIN(distance_m) AS min_daily, "
            "MAX(distance_m) AS max_daily, SUM(distance_m) AS sum_daily "
            "FROM mp04_daily_distance GROUP BY route_id ORDER BY route_id")
        common.write_csv(
            tables / "daily_distance_by_route.csv",
            [{"route_id": r["route_id"], "days": r["days"],
              "min_daily_distance_m": r["min_daily"], "max_daily_distance_m": r["max_daily"],
              "sum_of_daily_distance_m": r["sum_daily"]} for r in summary],
            ["route_id", "days", "min_daily_distance_m", "max_daily_distance_m",
             "sum_of_daily_distance_m"])

        pub = self._published(ctx)
        truth = self._truth(ctx)
        common.write_csv(
            tables / "published_monthly.csv",
            [{"month": MONTH, "route_id": r, "published_total_m": pub[r],
              "sum_of_daily_distance_m": truth[r], "difference_m": pub[r] - truth[r]}
             for r in sorted(pub)],
            ["month", "route_id", "published_total_m", "sum_of_daily_distance_m", "difference_m"])

        detail = db.query(
            w, "SELECT route_id, service_date, distance_m FROM mp04_daily_distance "
            "ORDER BY route_id, service_date")
        common.write_csv(
            tables / "daily_detail.csv",
            [{"route_id": r["route_id"], "service_date": r["service_date"],
              "daily_distance_m": r["distance_m"]} for r in detail],
            ["route_id", "service_date", "daily_distance_m"])

        alert = {
            "incident_id": "MP-04",
            "title": self.metadata.reader_title,
            "reported_symptom": (
                "The monthly route-distance summary shows a negative total for some "
                "routes. The daily per-vehicle distances that feed it are every one "
                "non-negative, and no correction or reversal rows are present. The job "
                "reported success."),
            "time_pressure": "The monthly distance summary feeds a capacity review.",
            "affected_product": PUBLISHED_TABLE,
            "fictional_notice": common.FICTIONAL_NOTICE,
        }
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx))
        write_json(ctx.paths.evidence_index, common.evidence_index("MP-04", ctx.run_id, ev))
        negatives = sum(1 for v in pub.values() if v < 0)
        return StepReport("evidence", "MP-04", ctx.run_id, "collected",
                          fields={"negative_routes": negatives})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        pub = self._published(ctx)
        blast_radius = {
            "held_dataset": PUBLISHED_TABLE,
            "negative_routes": sorted(r for r, v in pub.items() if v < 0),
            "daily_detail_rows": db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp04_daily_distance"),
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields={"negative_routes": len(blast_radius["negative_routes"])})
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-04", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-04", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["mp04_accumulator_width"] = WIDE_WIDTH
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"accumulator": WIDE_WIDTH})
        report = StepReport("repair", "MP-04", ctx.run_id, "repaired",
                            fields={"change": "the monthly aggregate is summed in a wide "
                                             "integer chosen to cover the proven maximum range",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        rows = self._materialize(ctx, faulty=False, tag="recover")
        pub = self._published(ctx)
        manifest = {
            "mode": "REPLAY",
            "publication_id": f"PUB-{ctx.run_id}-recover",
            "scope": {"datasets": [PUBLISHED_TABLE], "month": MONTH},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key (month, route_id); deterministic recompute",
            "validation": {"negative_routes_after": sum(1 for v in pub.values() if v < 0)},
            "corrected_accumulator": WIDE_WIDTH,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"monthly_rows": rows})
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-04", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        w = ctx.warehouse
        assertions: list[Assertion] = []

        pub = self._published(ctx)
        truth = self._truth(ctx)

        assertions.append(Assertion(
            "MP-04-VER-CORRECT", "correctness",
            "each monthly total equals the wide-integer sum of the daily detail",
            expected=True, actual=(pub == truth), status="PASS" if pub == truth else "FAIL",
            evidence="reports/verification.json"))

        negatives = [r for r, v in pub.items() if v < 0]
        assertions.append(Assertion(
            "MP-04-VER-RANGE", "range", "no monthly total is negative",
            expected=0, actual=len(negatives), status="PASS" if not negatives else "FAIL"))

        assertions.append(Assertion(
            "MP-04-VER-COMPLETE", "completeness",
            "every route present for the month",
            expected=len(truth), actual=len(pub),
            status="PASS" if set(pub) == set(truth) else "FAIL"))

        dup = db.scalar(w, f"SELECT COUNT(*) - COUNT(DISTINCT month || '|' || route_id) FROM {PUBLISHED_TABLE}")
        assertions.append(Assertion(
            "MP-04-VER-UNIQUE", "uniqueness", "no duplicate at (month, route_id) grain",
            expected=0, actual=dup, status="PASS" if dup == 0 else "FAIL"))

        published_sum = sum(pub.values())
        daily_sum = db.scalar(w, "SELECT COALESCE(SUM(distance_m), 0) FROM mp04_daily_distance")
        assertions.append(Assertion(
            "MP-04-VER-CONSERVE", "history",
            "summed monthly totals equal all daily distances",
            expected=daily_sum, actual=published_sum,
            status="PASS" if published_sum == daily_sum else "FAIL"))

        width = self._width(ctx)
        assertions.append(Assertion(
            "MP-04-VER-CONTROL", "control_consistency",
            "accumulator provenance records the wide integer",
            expected=WIDE_WIDTH, actual=width, status="PASS" if width == WIDE_WIDTH else "FAIL"))

        faulty_snap = common.read_hash_checkpoint(ctx, FAULTY_SNAPSHOT)
        std_changed = [t for t in SNAPSHOT_TABLES
                       if t in faulty_snap and faulty_snap[t] != table_logical_hash(w, t)]
        base = common.read_hash_checkpoint(ctx, INPUT_HASHES)
        input_changed = [t for t in INPUT_TABLES if base.get(t) != table_logical_hash(w, t)]
        bounded = not std_changed and not input_changed
        assertions.append(Assertion(
            "MP-04-VER-BOUNDED", "boundedness",
            "recovery changed only the monthly totals; standard and input tables unchanged",
            expected=[], actual=std_changed + input_changed,
            status="PASS" if bounded else "FAIL"))

        assertions.append(Assertion(
            "MP-04-VER-IDEMPOTENT", "idempotency",
            "independent wide recompute equals the persisted totals",
            expected=True, actual=(pub == truth), status="PASS" if pub == truth else "FAIL"))

        integ_ok, integ_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-04-VER-FIXTURE", "fixture_integrity",
            "landing inputs match checked-in fixture checksums",
            expected=True, actual=integ_ok, status="PASS" if integ_ok else "FAIL",
            evidence=str(integ_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-04", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP04Incident()
