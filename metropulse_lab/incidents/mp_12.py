"""MP-12 — A long station run loses access after the first block of stations.

Frozen mechanism (incident_taxonomy.md MP-12): a one-time, short-lived access
grant is handed to a run that outlasts it. Nothing renews the grant mid-run, and
a generic retry treats the resulting deterministic access denial as if it were a
transient error, so it keeps re-issuing reads that can never succeed. Partial
output is left in staging.

The access grant here is a simulated, non-secret fixture record made only of
logical offsets — there is no real credential, token, or secret anywhere.

Reader-facing evidence describes only the symptom: the first block of stations
returned data, every later station was denied, and partial output remained.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchC_common as cc
from . import _batchC_sim as sim
from .contract import IncidentMetadata

N_STATIONS = 100
SECONDS_PER_STATION = 5
STATIC_LIFETIME_S = 200          # covers the first 40 stations only
GENERIC_RETRIES_ON_DENIED = 3
STATIONS: tuple[tuple[int, str], ...] = tuple(
    (i, f"STN_L{i:03d}") for i in range(1, N_STATIONS + 1))

PRIVATE_TABLES = ("mp12_access_grant", "mp12_station_plan", "mp12_processed_station")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS mp12_access_grant (
    grant_id        TEXT NOT NULL PRIMARY KEY,
    kind            TEXT NOT NULL,
    issued_offset_s INTEGER NOT NULL,
    lifetime_s      INTEGER NOT NULL,
    fail_fast       INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mp12_station_plan (
    station_index INTEGER NOT NULL PRIMARY KEY,
    station_id    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mp12_processed_station (
    attempt_kind   TEXT NOT NULL,
    station_index  INTEGER NOT NULL,
    station_id     TEXT NOT NULL,
    offset_s       INTEGER NOT NULL,
    access_state   TEXT NOT NULL,
    staged         INTEGER NOT NULL,
    extra_attempts INTEGER NOT NULL,
    PRIMARY KEY (attempt_kind, station_index)
);
"""

HEALTHY_GRANT = dict(kind="refreshable", issued_offset_s=0, lifetime_s=STATIC_LIFETIME_S, fail_fast=1)
FAULTY_GRANT = dict(kind="static", issued_offset_s=0, lifetime_s=STATIC_LIFETIME_S, fail_fast=0)


class MP12Incident:
    metadata = IncidentMetadata(
        incident_id="MP-12",
        family="C. Execution / dependency / access",
        reader_title="A long station run loses access after the first block of stations",
        primary_invariant_id="MP-12-ACCESS-COVERAGE",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("set_access_grant:static_short_lived",),
        expected_detection_ids=("MP-12-BOUNDARY-001", "MP-12-GRANT-001"),
        transfer_mappings=("IAM/SDK refreshable credential providers",),
        nearest_old_scenario="Old S13 was an external source outage.",
        old_scenario_difference=(
            "The source is up the whole time. The run's own access lapses partway "
            "through because it was granted once for less than the run's length, and a "
            "generic retry mistook a permanent denial for a transient error."
        ),
        affected_tables=PRIVATE_TABLES,
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        ctx.control.executescript(CREATE_SQL)
        with db.transaction(ctx.control):
            ctx.control.executemany(
                "INSERT OR REPLACE INTO mp12_station_plan (station_index, station_id) VALUES (?, ?)",
                STATIONS)
        self._set_grant(ctx, HEALTHY_GRANT)
        self._persist_pass(ctx, "baseline", HEALTHY_GRANT)

    def baseline_ok(self, ctx: RunContext) -> bool:
        s = self._summary(ctx, "baseline")
        return s["staged"] == N_STATIONS and s["denied"] == 0

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        from ..checks import integrity
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"static_short_lived_access"})
        ctx.save_fault_profile(profile)
        self._set_grant(ctx, FAULTY_GRANT)

        grant = self._grant(ctx)
        run_duration = N_STATIONS * SECONDS_PER_STATION
        post_faulty = grant["kind"] == "static" and grant["lifetime_s"] < run_duration
        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-12-INJECT-PRE", "precondition",
                      "clean baseline reads every station",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-12-INJECT-POST", "postcondition",
                      "the access grant is static and shorter than the run",
                      expected=True, actual=post_faulty, status="PASS" if post_faulty else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-12-INJECT-FIXTURE"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected", fields={"grant": "static_short_lived"})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-12", ctx.run_id, status,
                            fields={"fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before),
                                    "fixture_detail": fx_detail},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._persist_pass(ctx, "faulty", self._grant(ctx))
        s = self._summary(ctx, "faulty")
        ctx.log.emit(logical_time=ctx.clock.tick(), component="task",
                     event_type="station_pass_finished",
                     fields={"stations_read": s["granted"], "stations_denied": s["denied"],
                             "staged": s["staged"]})
        return StepReport("run", "MP-12", ctx.run_id, "executed", fields=s)

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        phase = self._effective_phase(ctx)
        s = self._summary(ctx, phase)
        grant = self._grant(ctx)
        run_duration = N_STATIONS * SECONDS_PER_STATION

        data_bad = s["staged"] < N_STATIONS or s["denied"] > 0
        control_bad = grant["kind"] == "static" and grant["lifetime_s"] < run_duration
        detected = data_bad or control_bad

        cc.write_quality_result(
            ctx, "MP-12-BOUNDARY-001", "completeness",
            expected=f"all {N_STATIONS} stations read and staged",
            actual=f"{s['granted']} stations read, {s['denied']} denied starting at station "
                   f"#{s['first_denied']}; {s['staged']} staged",
            status="FAIL" if data_bad else "PASS")
        cc.write_quality_result(
            ctx, "MP-12-GRANT-001", "control_consistency",
            expected="the access grant covers the whole run and renews itself",
            actual=f"a {grant['kind']} grant valid for {grant['lifetime_s']}s was used for a "
                   f"{run_duration}s run",
            status="FAIL" if control_bad else "PASS")

        assertions = [
            Assertion("MP-12-DET-BOUNDARY", "completeness",
                      "every station is read and staged",
                      expected=N_STATIONS, actual=s["staged"],
                      status="FAIL" if data_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-12-DET-GRANT", "control_consistency",
                      "the access grant covers the run and renews",
                      expected="refreshable, covers run", actual=f"{grant['kind']}/{grant['lifetime_s']}s",
                      status="FAIL" if control_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "phase": phase,
                  "granted": s["granted"], "denied": s["denied"], "staged": s["staged"],
                  "first_denied_index": s["first_denied"],
                  "grant_lifetime_s": grant["lifetime_s"], "run_duration_s": run_duration}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-12", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-12", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        phase = self._effective_phase(ctx)
        rows = db.query(ctx.control,
            "SELECT station_index, station_id, offset_s, access_state, staged, extra_attempts "
            "FROM mp12_processed_station WHERE attempt_kind = ? ORDER BY station_index", (phase,))
        cc.write_csv(tables / "processed_stations.csv",
                     [dict(r) for r in rows],
                     ["station_index", "station_id", "offset_s", "access_state",
                      "staged", "extra_attempts"])
        s = self._summary(ctx, phase)
        grant = self._grant(ctx)
        cc.write_csv(tables / "access_boundary.csv",
                     [{"stations_expected": N_STATIONS, "stations_read": s["granted"],
                       "stations_denied": s["denied"], "staged": s["staged"],
                       "first_denied_index": s["first_denied"],
                       "grant_lifetime_s": grant["lifetime_s"],
                       "run_duration_s": N_STATIONS * SECONDS_PER_STATION}],
                     ["stations_expected", "stations_read", "stations_denied", "staged",
                      "first_denied_index", "grant_lifetime_s", "run_duration_s"])
        cc.write_standard_tables(ctx)

        alert = {
            "incident_id": "MP-12", "title": self.metadata.reader_title,
            "reported_symptom": (
                f"The first {s['granted']} stations returned data and were staged. Every "
                f"station after that was denied access, and re-issuing the reads did not "
                f"help. Partial output for the first block of stations is still sitting in "
                f"staging."),
            "time_pressure": "The station dataset is incomplete for the current cycle.",
            "affected_product": "station dataset (staging)",
            "observed": {"stations_read": s["granted"], "stations_denied": s["denied"],
                         "first_denied_index": s["first_denied"], "staged": s["staged"]},
            "fictional_notice": cc.FICTIONAL_NOTICE,
        }
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json", cc.sanitized_timeline(ctx))
        index = cc.write_evidence_index(ctx, "MP-12")
        return StepReport("evidence", "MP-12", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        s = self._summary(ctx, "faulty")
        blast = {"affected_product": "station dataset (staging)",
                 "partial_staged_rows": s["staged"],
                 "action": "partial staging quarantined; not published; evidence preserved",
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-12", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-12", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        self._set_grant(ctx, HEALTHY_GRANT)
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", [])
                                        if t != "static_short_lived_access"]
        profile.setdefault("params", {})["access_grant"] = "refreshable_fail_fast"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"access_grant": "refreshable_fail_fast"})
        report = StepReport("repair", "MP-12", ctx.run_id, "repaired",
                            fields={"change": "a self-renewing access grant covers the whole run; "
                                    "a denial now fails fast instead of being re-issued",
                                    "staging": "partial staging quarantined before replay"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (REPLAY the remaining stations after refresh) ----------------
    def recover(self, ctx: RunContext) -> StepReport:
        self._persist_pass(ctx, "recovery", self._grant(ctx))
        first_missing = self._summary(ctx, "faulty")["first_denied"]
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM simulated_outbox WHERE message_id LIKE 'MSG-mp12-%'")
            for index, station_id in STATIONS:
                ctx.control.execute(
                    "INSERT OR REPLACE INTO simulated_outbox "
                    "(message_id, topic, payload_json, logical_time) VALUES (?, ?, ?, ?)",
                    (f"MSG-mp12-{index:03d}", "station_read",
                     f'{{"station": "{station_id}", "state": "staged"}}',
                     ctx.clock.at_offset(0)))
        s = self._summary(ctx, "recovery")
        manifest = {
            "mode": "REPLAY",
            "scope": {"replayed_station_indices": f"{first_missing}-{N_STATIONS}",
                      "revalidated_indices": f"1-{first_missing - 1}",
                      "target": "station dataset (staging)"},
            "write_mode": "bounded_replace",
            "duplicate_protection": "one idempotent outbox message per station (stable id)",
            "stations_staged": s["staged"], "stations_denied": s["denied"],
            "boundary_first_missing_index": first_missing,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"stations_staged": s["staged"]})
        return StepReport("recover", "MP-12", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        rec = self._summary(ctx, "recovery")
        grant = self._grant(ctx)
        outbox = [r["message_id"] for r in db.query(ctx.control,
            "SELECT message_id FROM simulated_outbox WHERE message_id LIKE 'MSG-mp12-%'")]
        recovery_present = rec["total"] > 0

        assertions = [
            Assertion("MP-12-VER-COMPLETE", "completeness",
                      "the replay reads and stages every station",
                      expected=N_STATIONS, actual=rec["staged"],
                      status="PASS" if recovery_present and rec["staged"] == N_STATIONS else "FAIL",
                      evidence="reports/recovery.json"),
            Assertion("MP-12-VER-CORRECT", "correctness",
                      "no station is left denied after the replay",
                      expected=0, actual=rec["denied"],
                      status="PASS" if recovery_present and rec["denied"] == 0 else "FAIL"),
            Assertion("MP-12-VER-CONTROL", "control_consistency",
                      "the access grant renews and covers the whole run",
                      expected="refreshable", actual=grant["kind"],
                      status="PASS" if grant["kind"] == "refreshable" and grant["fail_fast"] == 1 else "FAIL"),
            Assertion("MP-12-VER-SIDEEFFECT", "side_effects",
                      "exactly one idempotent staged record per station",
                      expected=N_STATIONS, actual=len(outbox),
                      status="PASS" if len(outbox) == N_STATIONS else "FAIL"),
            Assertion("MP-12-VER-IDEMPOTENT", "idempotency",
                      "no duplicate staged-record message id",
                      expected=len(outbox), actual=len(set(outbox)),
                      status="PASS" if len(outbox) == len(set(outbox)) else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-12-VER-FIXTURE"),
        ]
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-12", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _persist_pass(self, ctx, phase, grant) -> None:
        access = sim.AccessGrant(grant["kind"], grant["issued_offset_s"], grant["lifetime_s"])
        results = sim.run_station_pass(
            list(STATIONS), access, SECONDS_PER_STATION,
            fail_fast=bool(grant["fail_fast"]),
            generic_retries_on_denied=GENERIC_RETRIES_ON_DENIED)
        rows = [(phase, r["station_index"], r["station_id"], r["offset_s"],
                 r["access_state"], r["staged"], r["extra_attempts"]) for r in results]
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp12_processed_station WHERE attempt_kind = ?", (phase,))
            db.insert_rows(ctx.control, "mp12_processed_station",
                           ("attempt_kind", "station_index", "station_id", "offset_s",
                            "access_state", "staged", "extra_attempts"), rows)

    def _summary(self, ctx, phase) -> dict[str, Any]:
        rows = db.query(ctx.control,
            "SELECT station_index, access_state, staged FROM mp12_processed_station "
            "WHERE attempt_kind = ? ORDER BY station_index", (phase,))
        granted = sum(1 for r in rows if r["access_state"] == "GRANTED")
        denied = sum(1 for r in rows if r["access_state"] == "DENIED")
        staged = sum(r["staged"] for r in rows)
        first_denied = next((r["station_index"] for r in rows if r["access_state"] == "DENIED"), 0)
        return {"total": len(rows), "granted": granted, "denied": denied,
                "staged": staged, "first_denied": first_denied}

    def _effective_phase(self, ctx) -> str:
        has_recovery = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM mp12_processed_station WHERE attempt_kind = 'recovery'")
        return "recovery" if has_recovery else "faulty"

    def _set_grant(self, ctx, g) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp12_access_grant "
                "(grant_id, kind, issued_offset_s, lifetime_s, fail_fast) VALUES ('current', ?, ?, ?, ?)",
                (g["kind"], g["issued_offset_s"], g["lifetime_s"], g["fail_fast"]))

    def _grant(self, ctx) -> dict[str, Any]:
        row = db.query_one(ctx.control, "SELECT * FROM mp12_access_grant WHERE grant_id = 'current'")
        return dict(row) if row is not None else {}


INCIDENT = MP12Incident()
