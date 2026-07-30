"""MP-13 — The same commit reads a different timestamp on a fresh runner.

Frozen mechanism (incident_taxonomy.md MP-13): the build's direct components are
constrained loosely, so a fresh resolve lets one component (a date reader) move
to a newer build. Two runners of the same commit therefore end up with different
component builds, and the newer build reads an ambiguous date the other way.
Same input string, different instant.

Everything is compared from two checked-in local environment fingerprints and two
parser builds; nothing is installed from a network index.

Reader-facing evidence describes only the symptom: identical input read to a
different timestamp, and two runners that ended up with different component
builds. The frozen mechanism vocabulary is kept out of ``evidence/``.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchC_common as cc
from . import _batchC_sim as sim
from .contract import IncidentMetadata

REFERENCE_ENV = "env_reference"      # what the approved build resolved to
FRESH_ENV = "env_fresh_runner"       # what a loose resolve produced later
EXPECTED = sim.parse_all(REFERENCE_ENV)  # the reference build's output is the oracle

PRIVATE_TABLES = ("mp13_env_config", "mp13_parse_output")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS mp13_env_config (
    config_id          TEXT NOT NULL PRIMARY KEY,
    mode               TEXT NOT NULL,
    active_environment TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mp13_parse_output (
    attempt_kind   TEXT NOT NULL,
    input_row_id   INTEGER NOT NULL,
    raw_value      TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    parsed_utc     TEXT NOT NULL,
    PRIMARY KEY (attempt_kind, input_row_id)
);
"""


class MP13Incident:
    metadata = IncidentMetadata(
        incident_id="MP-13",
        family="C. Execution / dependency / access",
        reader_title="The same commit reads a different timestamp on a fresh runner",
        primary_invariant_id="MP-13-BUILD-DETERMINISM",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="ROLLBACK",
        fault_operations=("set_environment:loose_fresh_runner",),
        expected_detection_ids=("MP-13-PARSE-001", "MP-13-ENV-001"),
        transfer_mappings=("pip lockfiles, hash-checking installs, immutable images",),
        nearest_old_scenario="An old Q&A only mentioned pinning as advice.",
        old_scenario_difference=(
            "This is a runnable diff between two checked-in environment fingerprints and "
            "two parser builds that read the same input to different instants, not a "
            "one-line reminder to pin."
        ),
        affected_tables=PRIVATE_TABLES,
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        ctx.control.executescript(CREATE_SQL)
        with db.transaction(ctx.control):
            for env in (REFERENCE_ENV, FRESH_ENV):
                for pkg, (version, digest) in sim.ENVIRONMENTS[env].items():
                    ctx.control.execute(
                        "INSERT OR REPLACE INTO dependency_manifest "
                        "(environment_id, package_name, resolved_version, resolved_hash) "
                        "VALUES (?, ?, ?, ?)", (env, pkg, version, digest))
        self._set_config(ctx, "locked", REFERENCE_ENV)
        self._persist_parse(ctx, "baseline", REFERENCE_ENV)

    def baseline_ok(self, ctx: RunContext) -> bool:
        cfg = self._config(ctx)
        rows = self._parse_rows(ctx, "baseline")
        matches = rows == {e["input_row_id"]: e["parsed_utc"] for e in EXPECTED}
        return cfg["mode"] == "locked" and matches

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        from ..checks import integrity
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"loose_environment_resolution"})
        ctx.save_fault_profile(profile)
        self._set_config(ctx, "loose", FRESH_ENV)

        cfg = self._config(ctx)
        differ = self._component_diff(ctx)
        post_faulty = cfg["mode"] == "loose" and bool(differ)
        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-13-INJECT-PRE", "precondition",
                      "the approved environment reads every input to the reference instant",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-13-INJECT-POST", "postcondition",
                      "the fresh runner uses a component build that differs from the reference",
                      expected=True, actual=post_faulty, status="PASS" if post_faulty else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-13-INJECT-FIXTURE"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected", fields={"environment": FRESH_ENV})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-13", ctx.run_id, status,
                            fields={"fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before),
                                    "fixture_detail": fx_detail},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        env = self._config(ctx)["active_environment"]
        self._persist_parse(ctx, "faulty", env)
        mism = self._mismatches(ctx, "faulty")
        ctx.log.emit(logical_time=ctx.clock.tick(), component="task",
                     event_type="parse_finished",
                     fields={"environment": env, "rows": len(EXPECTED),
                             "rows_differing_from_reference": len(mism)})
        return StepReport("run", "MP-13", ctx.run_id, "executed",
                          fields={"environment": env, "rows_differing": len(mism)})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        phase = self._effective_phase(ctx)
        mism = self._mismatches(ctx, phase)
        differ = self._component_diff(ctx)

        data_bad = bool(mism)
        control_bad = bool(differ)
        detected = data_bad or control_bad

        cc.write_quality_result(
            ctx, "MP-13-PARSE-001", "correctness",
            expected="the same input reads to the same instant as the reference build",
            actual=f"{len(mism)} of {len(EXPECTED)} input rows read to a different instant",
            status="FAIL" if data_bad else "PASS")
        cc.write_quality_result(
            ctx, "MP-13-ENV-001", "control_consistency",
            expected="both runners use the same component builds",
            actual=f"{len(differ)} component build(s) differ between the two runners "
                   f"({', '.join(d['component_name'] for d in differ) or 'none'})",
            status="FAIL" if control_bad else "PASS")

        assertions = [
            Assertion("MP-13-DET-PARSE", "correctness",
                      "identical input reads to the reference instant",
                      expected=0, actual=len(mism),
                      status="FAIL" if data_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-13-DET-ENV", "control_consistency",
                      "both runners share the same component builds",
                      expected=0, actual=len(differ),
                      status="FAIL" if control_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "phase": phase,
                  "rows_differing": len(mism), "components_differing": len(differ),
                  "differing_components": [d["component_name"] for d in differ]}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-13", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-13", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        phase = self._effective_phase(ctx)
        observed = self._parse_rows(ctx, phase)
        cc.write_csv(
            tables / "parse_diff.csv",
            [{"input_row_id": e["input_row_id"], "raw_value": e["raw_value"],
              "reference_parsed": e["parsed_utc"],
              "observed_parsed": observed.get(e["input_row_id"], ""),
              "same": int(observed.get(e["input_row_id"]) == e["parsed_utc"])}
             for e in EXPECTED],
            ["input_row_id", "raw_value", "reference_parsed", "observed_parsed", "same"])

        fps = db.query(ctx.control,
            "SELECT environment_id, package_name, resolved_version, resolved_hash "
            "FROM dependency_manifest ORDER BY environment_id, package_name")
        cc.write_csv(
            tables / "environment_fingerprints.csv",
            [{"environment_id": r["environment_id"], "component_name": r["package_name"],
              "build_fingerprint": r["resolved_hash"]} for r in fps],
            ["environment_id", "component_name", "build_fingerprint"])
        cc.write_standard_tables(ctx)

        differ = [d["component_name"] for d in self._component_diff(ctx)]
        alert = {
            "incident_id": "MP-13", "title": self.metadata.reader_title,
            "reported_symptom": (
                "An unchanged commit read the same input file to a different timestamp on "
                "a fresh runner. The fresh runner had a different build of a date-reading "
                "component than the approved runner, even though no code changed."),
            "time_pressure": "Downstream service days shift until the runners agree.",
            "affected_product": "parsed event timestamps",
            "observed": {"input_rows": len(EXPECTED),
                         "rows_differing": len(self._mismatches(ctx, phase)),
                         "components_differing": differ},
            "fictional_notice": cc.FICTIONAL_NOTICE,
        }
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json", cc.sanitized_timeline(ctx))
        index = cc.write_evidence_index(ctx, "MP-13")
        return StepReport("evidence", "MP-13", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        mism = self._mismatches(ctx, "faulty")
        blast = {"affected_product": "parsed event timestamps",
                 "rows_affected": len(mism),
                 "action": "outputs from the fresh runner held; not published; evidence preserved",
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-13", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-13", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        self._set_config(ctx, "locked", REFERENCE_ENV)
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", [])
                                        if t != "loose_environment_resolution"]
        profile.setdefault("params", {})["environment"] = "reference_exact"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"environment": "reference_exact"})
        report = StepReport("repair", "MP-13", ctx.run_id, "repaired",
                            fields={"change": "rebuild from the approved environment with exact, "
                                    "checked component builds so both runners match"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (ROLLBACK) ---------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        self._persist_parse(ctx, "recovery", REFERENCE_ENV)
        mism = self._mismatches(ctx, "recovery")
        manifest = {
            "mode": "ROLLBACK",
            "target": {"environment": REFERENCE_ENV, "kind": "approved exact environment"},
            "scope": {"input_rows": len(EXPECTED), "dataset": "parsed event timestamps"},
            "write_mode": "bounded_replace",
            "duplicate_protection": "one parsed row per input id; deterministic re-parse",
            "rows_matching_reference": len(EXPECTED) - len(mism),
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="rollback_completed", fields={"environment": REFERENCE_ENV})
        return StepReport("recover", "MP-13", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        rec = self._parse_rows(ctx, "recovery")
        expected_map = {e["input_row_id"]: e["parsed_utc"] for e in EXPECTED}
        cfg = self._config(ctx)
        differ_after = self._component_diff_for(ctx, cfg["active_environment"])
        locked_manifest_ok = self._env_manifest_matches(ctx, REFERENCE_ENV)

        assertions = [
            Assertion("MP-13-VER-CORRECT", "correctness",
                      "every input reads to the reference instant after rollback",
                      expected=len(EXPECTED), actual=sum(1 for k, v in rec.items() if expected_map.get(k) == v),
                      status="PASS" if rec == expected_map else "FAIL",
                      evidence="reports/recovery.json"),
            Assertion("MP-13-VER-COMPLETE", "completeness",
                      "the rollback re-reads every input row",
                      expected=len(EXPECTED), actual=len(rec),
                      status="PASS" if len(rec) == len(EXPECTED) else "FAIL"),
            Assertion("MP-13-VER-CONTROL", "control_consistency",
                      "the active environment is the approved one and matches the reference builds",
                      expected="locked/env_reference/no diff",
                      actual=f"{cfg['mode']}/{cfg['active_environment']}/{len(differ_after)} diff",
                      status="PASS" if (cfg["mode"] == "locked"
                                        and cfg["active_environment"] == REFERENCE_ENV
                                        and not differ_after) else "FAIL"),
            Assertion("MP-13-VER-REPRODUCIBLE", "history",
                      "the approved environment resolves to its recorded component builds",
                      expected=True, actual=locked_manifest_ok,
                      status="PASS" if locked_manifest_ok else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-13-VER-FIXTURE"),
        ]
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-13", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _persist_parse(self, ctx, phase, environment_id) -> None:
        rows = [(phase, e["input_row_id"], e["raw_value"], environment_id, e["parsed_utc"])
                for e in sim.parse_all(environment_id)]
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp13_parse_output WHERE attempt_kind = ?", (phase,))
            db.insert_rows(ctx.control, "mp13_parse_output",
                           ("attempt_kind", "input_row_id", "raw_value", "environment_id",
                            "parsed_utc"), rows)

    def _parse_rows(self, ctx, phase) -> dict[int, str]:
        return {r["input_row_id"]: r["parsed_utc"] for r in db.query(ctx.control,
            "SELECT input_row_id, parsed_utc FROM mp13_parse_output "
            "WHERE attempt_kind = ? ORDER BY input_row_id", (phase,))}

    def _mismatches(self, ctx, phase) -> list[int]:
        rows = self._parse_rows(ctx, phase)
        expected_map = {e["input_row_id"]: e["parsed_utc"] for e in EXPECTED}
        return sorted(k for k, v in rows.items() if expected_map.get(k) != v)

    def _component_diff(self, ctx) -> list[dict[str, Any]]:
        ref = {r["package_name"]: (r["resolved_version"], r["resolved_hash"]) for r in db.query(
            ctx.control, "SELECT package_name, resolved_version, resolved_hash FROM "
            "dependency_manifest WHERE environment_id = ?", (REFERENCE_ENV,))}
        out = []
        for r in db.query(ctx.control, "SELECT package_name, resolved_version, resolved_hash FROM "
                          "dependency_manifest WHERE environment_id = ? ORDER BY package_name",
                          (FRESH_ENV,)):
            if ref.get(r["package_name"]) != (r["resolved_version"], r["resolved_hash"]):
                out.append({"component_name": r["package_name"]})
        return out

    def _component_diff_for(self, ctx, environment_id) -> list[dict[str, Any]]:
        ref = {r["package_name"]: (r["resolved_version"], r["resolved_hash"]) for r in db.query(
            ctx.control, "SELECT package_name, resolved_version, resolved_hash FROM "
            "dependency_manifest WHERE environment_id = ?", (REFERENCE_ENV,))}
        out = []
        for r in db.query(ctx.control, "SELECT package_name, resolved_version, resolved_hash FROM "
                          "dependency_manifest WHERE environment_id = ? ORDER BY package_name",
                          (environment_id,)):
            if ref.get(r["package_name"]) != (r["resolved_version"], r["resolved_hash"]):
                out.append({"component_name": r["package_name"]})
        return out

    def _env_manifest_matches(self, ctx, environment_id) -> bool:
        rows = {r["package_name"]: (r["resolved_version"], r["resolved_hash"]) for r in db.query(
            ctx.control, "SELECT package_name, resolved_version, resolved_hash FROM "
            "dependency_manifest WHERE environment_id = ?", (environment_id,))}
        return rows == sim.ENVIRONMENTS[environment_id]

    def _effective_phase(self, ctx) -> str:
        has_recovery = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM mp13_parse_output WHERE attempt_kind = 'recovery'")
        return "recovery" if has_recovery else "faulty"

    def _set_config(self, ctx, mode, active) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp13_env_config (config_id, mode, active_environment) "
                "VALUES ('current', ?, ?)", (mode, active))

    def _config(self, ctx) -> dict[str, Any]:
        row = db.query_one(ctx.control, "SELECT * FROM mp13_env_config WHERE config_id = 'current'")
        return dict(row) if row is not None else {}


INCIDENT = MP13Incident()
