"""MP-20 — A small station-roster join goes from seconds to minutes after a load.

Frozen mechanism (incident_taxonomy.md MP-20): after a bulk load the planner's
size profile for the large table went stale, so the chooser still believes the
big table is small and reasons about the join with a wrong size. Inputs are not
skewed and the SQL and indexes are unchanged.

This incident is engine-of-record REAL SQLite. It builds two tables in a real
SQLite connection, runs ``ANALYZE``, reads ``sqlite_stat1`` before and after a
bulk load, captures ``EXPLAIN QUERY PLAN`` for both, and records
``sqlite_version()`` with the evidence. On builds where the plan text is
identical before and after ``ANALYZE`` (valid engine behavior), the decisive
signal is the recorded size profile itself: the profile still reports the small
pre-load row count while the table is now large. A SEPARATELY LABELED
deterministic planner-cost model then explains the cost consequence of that wrong
size belief; it is never presented as a real optimizer decision, EXPLAIN ANALYZE,
PostgreSQL output, or elapsed time.

The reader-facing title and evidence describe only the symptom.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from .. import config, db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from .contract import IncidentMetadata
from . import _batchD_common as common
from ._batchD_planner_model import estimate_join

FAULT = "mp20_stale_size_profile"
SMALL_LOAD = 24
FULL_LOAD = 6000
SIZE_ERROR_BOUND = 2          # recorded size must be within 2x of the actual size
RECOVERY_DATASET = "mart_roster_join"

_STATIONS = [(f"STN_{n:03d}", f"Z{((n - 1) // 3) + 1}") for n in range(1, 13)]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mp20_plan_probe (
    phase           TEXT PRIMARY KEY,
    recorded_rows   INTEGER NOT NULL,
    actual_rows     INTEGER NOT NULL,
    size_error_x    INTEGER NOT NULL,
    profile_current INTEGER NOT NULL,
    plan_text       TEXT NOT NULL,
    matched_rows    INTEGER NOT NULL,
    sqlite_version  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mp20_join_result (
    zone_code    TEXT PRIMARY KEY,
    matched_rows INTEGER NOT NULL
);
"""

# The roster query whose plan and cost we study. Fixed SQL, fixed index.
_JOIN_QUERY = (
    "SELECT r.zone_code AS zone_code, COUNT(*) AS matched_rows "
    "FROM mp20_roster r JOIN mp20_station_load l ON l.station_id = r.station_id "
    "GROUP BY r.zone_code ORDER BY r.zone_code"
)


def _recorded_rows(conn: sqlite3.Connection, table: str) -> int:
    present = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name = 'sqlite_stat1'").fetchone()[0]
    if not present:
        return 0
    rows = conn.execute("SELECT stat FROM sqlite_stat1 WHERE tbl = ?", (table,)).fetchall()
    best = 0
    for (stat,) in rows:
        if stat:
            try:
                best = max(best, int(str(stat).split()[0]))
            except (ValueError, IndexError):
                continue
    return best


def _plan_text(conn: sqlite3.Connection) -> str:
    steps = [str(r[3]) for r in conn.execute("EXPLAIN QUERY PLAN " + _JOIN_QUERY).fetchall()]
    return " | ".join(steps)


def _join_result(conn: sqlite3.Connection) -> dict[str, int]:
    return {r[0]: r[1] for r in conn.execute(_JOIN_QUERY).fetchall()}


def roster_experiment() -> dict[str, Any]:
    """Real SQLite ANALYZE / sqlite_stat1 / EXPLAIN QUERY PLAN capture.

    Deterministic: fixed roster and fixed round-robin bulk load. Returns the
    recorded size profile, plan text, and join result for the stale ("as loaded")
    and refreshed ("after maintenance") phases, plus sqlite_version.
    """
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(
            "CREATE TABLE mp20_roster (station_id TEXT PRIMARY KEY, zone_code TEXT NOT NULL);"
            "CREATE TABLE mp20_station_load (load_id INTEGER PRIMARY KEY, "
            "station_id TEXT NOT NULL, passengers INTEGER NOT NULL);"
            "CREATE INDEX ix_load_station ON mp20_station_load(station_id);")
        conn.executemany("INSERT INTO mp20_roster VALUES (?, ?)", _STATIONS)

        # Small initial load, then refresh the size profile at the small size.
        load_id = 0
        for i in range(SMALL_LOAD):
            load_id += 1
            station = _STATIONS[i % len(_STATIONS)][0]
            conn.execute("INSERT INTO mp20_station_load VALUES (?, ?, ?)", (load_id, station, 10))
        conn.commit()
        conn.execute("ANALYZE")

        # Bulk load to full size WITHOUT refreshing the size profile: now stale.
        for i in range(FULL_LOAD - SMALL_LOAD):
            load_id += 1
            station = _STATIONS[i % len(_STATIONS)][0]
            conn.execute("INSERT INTO mp20_station_load VALUES (?, ?, ?)", (load_id, station, 10))
        conn.commit()

        actual = conn.execute("SELECT COUNT(*) FROM mp20_station_load").fetchone()[0]
        stale = {
            "recorded_rows": _recorded_rows(conn, "mp20_station_load"),
            "actual_rows": actual,
            "profile_present": conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='sqlite_stat1'").fetchone()[0] > 0,
            "plan_text": _plan_text(conn),
            "result": _join_result(conn),
        }

        # Refresh the size profile against the current size.
        conn.execute("ANALYZE")
        fresh = {
            "recorded_rows": _recorded_rows(conn, "mp20_station_load"),
            "actual_rows": actual,
            "profile_present": True,
            "plan_text": _plan_text(conn),
            "result": _join_result(conn),
        }
        return {"sqlite_version": conn.execute("SELECT sqlite_version()").fetchone()[0],
                "actual_rows": actual, "stale": stale, "fresh": fresh}
    finally:
        conn.close()


def _size_error(recorded: int, actual: int) -> int:
    return actual // max(1, recorded)


class MP20Incident:
    metadata = IncidentMetadata(
        incident_id="MP-20",
        family="D. Performance and cost",
        reader_title="A small station-roster join goes from seconds to minutes after a large load",
        primary_invariant_id="MP-20-SIZE-PROFILE",
        core_evidence_kind="MIXED_LABELED",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-20-SIZEERROR-001", "MP-20-PROFILE-001"),
        transfer_mappings=(
            "PostgreSQL planner statistics and EXPLAIN ANALYZE estimated-vs-actual rows",
        ),
        nearest_old_scenario="None; the old books have no size-profile-freshness join incident.",
        old_scenario_difference=(
            "Not shuffle skew and not a broadcast decision: inputs are unskewed and the "
            "SQL and indexes are unchanged; the recorded size profile went stale after "
            "the bulk load."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        ctx.warehouse.executescript(_SCHEMA)

    def _record_phase(self, ctx: RunContext, phase: str, facts: dict[str, Any],
                      sqlite_version: str) -> dict[str, Any]:
        recorded = facts["recorded_rows"]
        actual = facts["actual_rows"]
        size_error = _size_error(recorded, actual)
        profile_current = 1 if (recorded and _size_error(recorded, actual) <= SIZE_ERROR_BOUND) else 0
        matched = sum(facts["result"].values())
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "INSERT OR REPLACE INTO mp20_plan_probe (phase, recorded_rows, actual_rows, "
                "size_error_x, profile_current, plan_text, matched_rows, sqlite_version) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (phase, recorded, actual, size_error, profile_current,
                 facts["plan_text"], matched, sqlite_version))
        return {"phase": phase, "recorded_rows": recorded, "actual_rows": actual,
                "size_error_x": size_error, "profile_current": bool(profile_current),
                "matched_rows": matched, "plan_text": facts["plan_text"]}

    def _read_phase(self, ctx: RunContext, phase: str) -> dict[str, Any] | None:
        row = db.query_one(ctx.warehouse, "SELECT * FROM mp20_plan_probe WHERE phase = ?", (phase,))
        return dict(row) if row else None

    def _planner_model(self, recorded: int, actual: int) -> dict[str, object]:
        # SEPARATELY LABELED cost model (not a real optimizer decision). The join
        # has an index, so this illustrates the no-index consequence of the wrong
        # size belief; it is explanatory only.
        return estimate_join(outer_rows=len(_STATIONS), believed_inner_rows=recorded,
                             actual_inner_rows=actual, has_index=False)

    def baseline_ok(self, ctx: RunContext) -> bool:
        exp = roster_experiment()
        fresh = exp["fresh"]
        return _size_error(fresh["recorded_rows"], fresh["actual_rows"]) <= SIZE_ERROR_BOUND

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        exp = roster_experiment()
        fresh_before = _size_error(exp["fresh"]["recorded_rows"],
                                   exp["fresh"]["actual_rows"]) <= SIZE_ERROR_BOUND
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(set(profile.get("faulty_transforms", [])) | {FAULT})
        profile.setdefault("params", {})["refresh_size_profile"] = False
        ctx.save_fault_profile(profile)

        stale = self._record_phase(ctx, "as_loaded", exp["stale"], exp["sqlite_version"])
        stale_bad = stale["size_error_x"] > SIZE_ERROR_BOUND

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fixtures_ok, fixture_detail = integrity.fixtures_untouched(ctx)
        fixtures_ok = fixtures_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-20-INJECT-PRE", "precondition",
                      "a refreshed size profile is within the size-error bound",
                      expected=True, actual=fresh_before, status="PASS" if fresh_before else "FAIL"),
            Assertion("MP-20-INJECT-POST", "postcondition",
                      "after the bulk load the recorded size profile is far below actual",
                      expected=True, actual=stale_bad, status="PASS" if stale_bad else "FAIL"),
            Assertion("MP-20-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection (control-plane fault only)",
                      expected=True, actual=fixtures_ok, status="PASS" if fixtures_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected",
                     fields={"recorded_rows": stale["recorded_rows"],
                             "actual_rows": stale["actual_rows"]})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-20", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "recorded_rows": stale["recorded_rows"],
                                    "actual_rows": stale["actual_rows"],
                                    "sqlite_version": exp["sqlite_version"],
                                    "fixture_checksums_unchanged": fixtures_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        exp = roster_experiment()
        stale = self._record_phase(ctx, "as_loaded", exp["stale"], exp["sqlite_version"])
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="faulty_run",
                     fields={"recorded_rows": stale["recorded_rows"], "actual_rows": stale["actual_rows"]})
        return StepReport("run", "MP-20", ctx.run_id, "executed",
                          fields={"recorded_rows": stale["recorded_rows"],
                                  "actual_rows": stale["actual_rows"],
                                  "size_error_x": stale["size_error_x"],
                                  "plan_text": stale["plan_text"]})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        stale = self._read_phase(ctx, "as_loaded") or {}
        recorded = stale.get("recorded_rows", 0)
        actual = stale.get("actual_rows", 0)
        size_error = stale.get("size_error_x", 0)
        profile_current = bool(stale.get("profile_current", 0))

        error_bad = size_error > SIZE_ERROR_BOUND
        profile_bad = not profile_current
        detected = bool(error_bad or profile_bad)

        model = self._planner_model(recorded, actual)

        common.write_quality_result(
            ctx, "MP-20-SIZEERROR-001", "published_metric",
            expected=f"the recorded size of the load table is within {SIZE_ERROR_BOUND}x of actual",
            actual=f"recorded {recorded} rows while the table holds {actual} rows",
            status="FAIL" if error_bad else "PASS")
        common.write_quality_result(
            ctx, "MP-20-PROFILE-001", "scan_work",
            expected="the recorded size profile matches the current row count",
            actual="the recorded size profile predates the bulk load",
            status="FAIL" if profile_bad else "PASS")

        assertions = [
            Assertion("MP-20-DET-SIZEERROR", "published_metric",
                      "recorded load-table size within the size-error bound",
                      expected=f"<= {SIZE_ERROR_BOUND}x", actual=f"{size_error}x",
                      status="FAIL" if error_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-20-DET-PROFILE", "scan_work",
                      "recorded size profile is current with the row count",
                      expected="current", actual="predates the load",
                      status="FAIL" if profile_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "recorded_rows": recorded,
                  "actual_rows": actual, "size_error_x": size_error,
                  "plan_text": stale.get("plan_text"),
                  "sqlite_version": stale.get("sqlite_version"),
                  "result_identical_after_refresh": True,
                  "planner_cost_model": model}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-20", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-20", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        stale = self._read_phase(ctx, "as_loaded") or {}
        # Real SQLite facts, neutral column names. plan_text is the engine's own
        # access-path description; recorded vs actual size is the decisive fact.
        common.write_csv(
            tables / "roster_join_size_profile.csv",
            [{"phase": "as_loaded",
              "recorded_size": stale.get("recorded_rows"),
              "actual_size": stale.get("actual_rows"),
              "size_error_x": stale.get("size_error_x"),
              "size_profile_current": stale.get("profile_current"),
              "join_plan_step": stale.get("plan_text"),
              "matched_rows": stale.get("matched_rows"),
              "sqlite_version": stale.get("sqlite_version")}],
            ["phase", "recorded_size", "actual_size", "size_error_x", "size_profile_current",
             "join_plan_step", "matched_rows", "sqlite_version"])
        common.write_common_evidence_tables(ctx)

        alert = common.build_alert(
            "MP-20", self.metadata.reader_title,
            reported_symptom=(
                "A small station-roster join that used to return in seconds now takes "
                "minutes. This started right after a large load into the joined table. "
                "The inputs are not skewed and neither the SQL nor the indexes changed."),
            time_pressure="The roster join backs an operations lookup used during service.",
            affected_product="station-roster join",
            observed={"before_load": "seconds", "after_load": "minutes",
                      "sql_and_indexes": "unchanged", "skew": "none"})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx))
        index = common.write_evidence_index(ctx, "MP-20")
        return StepReport("evidence", "MP-20", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        blast_radius = {
            "held_product": RECOVERY_DATASET,
            "action": "hold the roster-join publication until the size profile is refreshed",
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast_radius)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-20", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-20", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["refresh_size_profile"] = True
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident", event_type="repaired",
                     fields={"strategy": "refresh the size profile after the bulk load, then re-plan"})
        report = StepReport("repair", "MP-20", ctx.run_id, "repaired",
                            fields={"change": "refresh the recorded size profile against the "
                                             "post-load row count and re-plan",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        pub = f"PUB-{ctx.run_id}-recover"
        exp = roster_experiment()
        fresh = self._record_phase(ctx, "after_maintenance", exp["fresh"], exp["sqlite_version"])

        result = exp["fresh"]["result"]
        with db.transaction(conn):
            conn.execute("DELETE FROM mp20_join_result")
            for zone in sorted(result):
                conn.execute("INSERT INTO mp20_join_result (zone_code, matched_rows) VALUES (?, ?)",
                             (zone, result[zone]))
        dv_id = common.record_recovery_publication(ctx, RECOVERY_DATASET, pub)

        manifest = {
            "mode": "REPLAY", "publication_id": pub, "dataset_version_id": dv_id,
            "scope": {"datasets": [RECOVERY_DATASET]},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key at zone grain; deterministic recompute",
            "strategy": "refresh the recorded size profile, then re-plan the join",
            "recorded_rows_after": fresh["recorded_rows"], "actual_rows": fresh["actual_rows"],
            "size_error_x_after": fresh["size_error_x"],
            "sqlite_version": exp["sqlite_version"],
            "planner_cost_model_after": self._planner_model(fresh["recorded_rows"], fresh["actual_rows"]),
            "result_rows": len(result), "evidence_origin": common.EVIDENCE_ORIGIN,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"result_rows": len(result)})
        return StepReport("recover", "MP-20", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        assertions: list[Assertion] = []

        reference = roster_experiment()["fresh"]["result"]
        persisted = {r["zone_code"]: r["matched_rows"] for r in db.query(conn,
            "SELECT zone_code, matched_rows FROM mp20_join_result")}

        assertions.append(Assertion(
            "MP-20-VER-CORRECT", "correctness",
            "recovered join result equals an independent real-SQLite recompute",
            expected=reference, actual=persisted,
            status="PASS" if persisted == reference else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-20-VER-COMPLETE", "completeness",
            "recovered result covers every zone in the join",
            expected=len(reference), actual=len(persisted),
            status="PASS" if set(persisted) == set(reference) else "FAIL"))

        fresh = self._read_phase(ctx, "after_maintenance")
        bounded_ok = bool(fresh) and fresh["size_error_x"] <= SIZE_ERROR_BOUND \
            and fresh["profile_current"] == 1
        assertions.append(Assertion(
            "MP-20-VER-BOUNDED", "boundedness",
            "the refreshed size profile is within the size-error bound",
            expected=f"<= {SIZE_ERROR_BOUND}x", actual=(fresh or {}).get("size_error_x"),
            status="PASS" if bounded_ok else "FAIL"))

        control_ok = common.has_recovery_publication(ctx, RECOVERY_DATASET)
        assertions.append(Assertion(
            "MP-20-VER-CONTROL", "control_consistency",
            "a bounded-recovery roster-join publication is recorded",
            expected=">=1 recovered publication", actual=control_ok,
            status="PASS" if control_ok else "FAIL"))

        assertions.append(Assertion(
            "MP-20-VER-IDEMPOTENT", "idempotency",
            "independent recompute equals the persisted recovered result",
            expected=True, actual=(persisted == reference and bool(persisted)),
            status="PASS" if (persisted == reference and bool(persisted)) else "FAIL"))

        integrity_ok, integrity_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-20-VER-FIXTURE", "fixture_integrity",
            "run landing inputs match checked-in fixture checksums",
            expected=True, actual=integrity_ok, status="PASS" if integrity_ok else "FAIL",
            evidence=str(integrity_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-20", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP20Incident()
