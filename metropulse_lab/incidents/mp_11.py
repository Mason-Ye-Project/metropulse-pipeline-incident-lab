"""MP-11 — A brief burst of rejected station-status requests becomes an outage.

Frozen mechanism (incident_taxonomy.md MP-11): the orchestrator, the HTTP client,
and the task wrapper each re-issue a failed request immediately, with no spacing,
no ceiling on total attempts, and no global cap. A short period where the
station-status upstream is overloaded is turned into a self-sustaining flood: the
logical clock never advances past the overloaded phase, so nothing recovers.

Reader-facing evidence describes only the symptom (a short burst of rejected
requests, then a spike in request volume with persistent errors). The mechanism
vocabulary is kept out of everything under ``evidence/``.
"""

from __future__ import annotations

from typing import Any

from .. import config, db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchC_common as cc
from . import _batchC_sim as sim
from .contract import IncidentMetadata

STATIONS: tuple[str, ...] = tuple(config.STATIONS)
RECOVERY_OFFSET_S = 8
SERVER_DELAY_S = 2
BACKOFF_BASE_S = 2
HEALTHY_MAX_ATTEMPTS = 6
PER_STATION_BUDGET = 6
LAYER_FANOUT = (3, 3, 3)  # three independent owners, immediate re-issue each

PRIVATE_TABLES = ("mp11_retry_policy", "mp11_fetch_attempt")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS mp11_retry_policy (
    policy_id       TEXT NOT NULL PRIMARY KEY,
    owner_layers    INTEGER NOT NULL,
    max_attempts    INTEGER NOT NULL,
    backoff_base_s  INTEGER NOT NULL,
    use_backoff     INTEGER NOT NULL,
    use_jitter      INTEGER NOT NULL,
    request_budget  INTEGER NOT NULL,
    concurrency_cap INTEGER NOT NULL,
    honor_server_delay INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mp11_fetch_attempt (
    attempt_kind   TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    station        TEXT NOT NULL,
    layer          TEXT NOT NULL,
    n              INTEGER NOT NULL,
    offset_s       INTEGER NOT NULL,
    response_class TEXT NOT NULL,
    succeeded      INTEGER NOT NULL,
    PRIMARY KEY (attempt_kind, seq)
);
"""

HEALTHY_POLICY = dict(owner_layers=1, max_attempts=HEALTHY_MAX_ATTEMPTS,
                      backoff_base_s=BACKOFF_BASE_S, use_backoff=1, use_jitter=1,
                      request_budget=PER_STATION_BUDGET, concurrency_cap=4,
                      honor_server_delay=1)
FAULTY_POLICY = dict(owner_layers=len(LAYER_FANOUT), max_attempts=-1,
                     backoff_base_s=0, use_backoff=0, use_jitter=0,
                     request_budget=-1, concurrency_cap=-1, honor_server_delay=0)


class MP11Incident:
    metadata = IncidentMetadata(
        incident_id="MP-11",
        family="C. Execution / dependency / access",
        reader_title="A brief burst of rejected station-status requests turns into a sustained outage",
        primary_invariant_id="MP-11-REQUEST-DISCIPLINE",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("set_request_policy:layered_immediate",),
        expected_detection_ids=("MP-11-RATE-001", "MP-11-OWNER-001"),
        transfer_mappings=("Airflow pools and AWS SDK retry token budgets",),
        nearest_old_scenario="Old S5 lost a write ACK and produced duplicates.",
        old_scenario_difference=(
            "Not a duplicate side effect from a lost acknowledgement; the flood is "
            "produced by several independent owners re-issuing the same request with "
            "no spacing or ceiling, so a short overloaded phase never clears."
        ),
        affected_tables=PRIVATE_TABLES,
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        ctx.control.executescript(CREATE_SQL)
        ctx.control.commit()
        self._set_policy(ctx, "current", HEALTHY_POLICY)
        self._persist_collection(ctx, "baseline", self._collect(faulty=False))

    def baseline_ok(self, ctx: RunContext) -> bool:
        per = self._per_station(ctx, "baseline")
        if not per:
            return False
        all_ok = all(p["succeeded"] for p in per.values())
        bounded = all(p["requests"] <= PER_STATION_BUDGET for p in per.values())
        return all_ok and bounded

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        from ..checks import integrity
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"layered_immediate_request"})
        ctx.save_fault_profile(profile)
        self._set_policy(ctx, "current", FAULTY_POLICY)

        policy = self._policy(ctx, "current")
        post_faulty = policy["owner_layers"] > 1 and policy["request_budget"] < 0
        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fx_ok, fx_detail = integrity.fixtures_untouched(ctx)
        fx_ok = fx_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-11-INJECT-PRE", "precondition",
                      "clean baseline keeps request volume within the per-owner ceiling",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-11-INJECT-POST", "postcondition",
                      "faulty request policy has several independent owners and no ceiling",
                      expected=True, actual=post_faulty, status="PASS" if post_faulty else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-11-INJECT-FIXTURE"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected",
                     fields={"policy": "layered_immediate_request"})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-11", ctx.run_id, status,
                            fields={"fixture_checksums_unchanged": fx_ok,
                                    "landing_files_checked": len(fixtures_before),
                                    "fixture_detail": fx_detail},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._persist_collection(ctx, "faulty", self._collect(faulty=True))
        per = self._per_station(ctx, "faulty")
        total = sum(p["requests"] for p in per.values())
        succeeded = sum(1 for p in per.values() if p["succeeded"])
        ctx.log.emit(logical_time=ctx.clock.tick(), component="task",
                     event_type="collection_finished",
                     fields={"stations": len(per), "requests_issued": total,
                             "stations_succeeded": succeeded})
        return StepReport("run", "MP-11", ctx.run_id, "executed",
                          fields={"requests_issued": total, "stations_succeeded": succeeded})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        phase = self._effective_phase(ctx)
        per = self._per_station(ctx, phase)
        total = sum(p["requests"] for p in per.values())
        succeeded = sum(1 for p in per.values() if p["succeeded"])
        worst = max((p["requests"] for p in per.values()), default=0)
        baseline_total = sum(p["requests"] for p in self._per_station(ctx, "baseline").values())

        data_bad = (succeeded < len(STATIONS)) or (worst > PER_STATION_BUDGET)
        policy = self._policy(ctx, "current")
        control_bad = (policy["owner_layers"] > 1 or policy["request_budget"] < 0
                       or policy["use_backoff"] == 0 or policy["concurrency_cap"] < 0)
        detected = data_bad or control_bad

        cc.write_quality_result(
            ctx, "MP-11-RATE-001", "completeness",
            expected=f"station-status collected for all {len(STATIONS)} stations within "
                     f"the observed baseline of {baseline_total} requests",
            actual=f"{succeeded}/{len(STATIONS)} stations collected after {total} requests "
                   f"(worst station {worst})",
            status="FAIL" if data_bad else "PASS")
        cc.write_quality_result(
            ctx, "MP-11-OWNER-001", "control_consistency",
            expected="a single owner issues requests within a bounded, spaced ceiling",
            actual=f"{policy['owner_layers']} independent owners; ceiling "
                   f"{'absent' if policy['request_budget'] < 0 else policy['request_budget']}",
            status="FAIL" if control_bad else "PASS")

        assertions = [
            Assertion("MP-11-DET-RATE", "completeness",
                      "station-status volume stays within the baseline and all stations complete",
                      expected=len(STATIONS), actual=succeeded,
                      status="FAIL" if data_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-11-DET-OWNER", "control_consistency",
                      "requests have a single owner with a bounded spaced ceiling",
                      expected=1, actual=policy["owner_layers"],
                      status="FAIL" if control_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "phase": phase,
                  "requests_issued": total, "baseline_requests": baseline_total,
                  "stations_succeeded": succeeded, "worst_station_requests": worst,
                  "owner_layers": policy["owner_layers"]}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-11", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-11", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        phase = self._effective_phase(ctx)
        faulty = self._per_station(ctx, phase)
        baseline = self._per_station(ctx, "baseline")

        cc.write_csv(
            tables / "request_volume.csv",
            [{"station": st, "observed_requests": faulty[st]["requests"],
              "successful": faulty[st]["succeeded"],
              "baseline_requests": baseline.get(st, {}).get("requests", 0)}
             for st in STATIONS],
            ["station", "observed_requests", "successful", "baseline_requests"])

        classes = db.query(ctx.control,
            "SELECT response_class, COUNT(*) AS c FROM mp11_fetch_attempt "
            "WHERE attempt_kind = ? GROUP BY response_class ORDER BY response_class",
            (phase,))
        cc.write_csv(tables / "response_class_counts.csv",
                     [{"response_class": r["response_class"], "count": r["c"]} for r in classes],
                     ["response_class", "count"])
        cc.write_standard_tables(ctx)

        total = sum(p["requests"] for p in faulty.values())
        baseline_total = sum(p["requests"] for p in baseline.values())
        alert = {
            "incident_id": "MP-11", "title": self.metadata.reader_title,
            "reported_symptom": (
                "The station-status service briefly rejected requests. Request volume "
                "then rose sharply and errors kept coming even after the upstream "
                "service started to answer normally again. The collection task never "
                "finished for this cycle."),
            "time_pressure": "The public service-status view is stale until collection recovers.",
            "affected_product": "station-status collection",
            "observed": {"requests_issued": total, "baseline_requests": baseline_total,
                         "stations_collected": sum(1 for p in faulty.values() if p["succeeded"]),
                         "stations_expected": len(STATIONS)},
            "fictional_notice": cc.FICTIONAL_NOTICE,
        }
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json", cc.sanitized_timeline(ctx))
        index = cc.write_evidence_index(ctx, "MP-11")
        return StepReport("evidence", "MP-11", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        per = self._per_station(ctx, "faulty")
        blast = {"affected_task": "station-status collection",
                 "requests_observed": sum(p["requests"] for p in per.values()),
                 "action": "non-critical collection paused; evidence preserved",
                 "evidence_preserved": True}
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-11", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-11", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        self._set_policy(ctx, "current", HEALTHY_POLICY)
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", [])
                                        if t != "layered_immediate_request"]
        profile.setdefault("params", {})["request_policy"] = "single_owner_bounded"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired", fields={"request_policy": "single_owner_bounded"})
        report = StepReport("repair", "MP-11", ctx.run_id, "repaired",
                            fields={"change": "one owner issues bounded, spaced requests that "
                                    "honour the upstream's requested delay, under a global cap",
                                    "classification": "transient vs deterministic errors separated"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (REPLAY) -----------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        self._persist_collection(ctx, "recovery", self._collect(faulty=False))
        per = self._per_station(ctx, "recovery")
        # Idempotent outbox: exactly one published status per station.
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM simulated_outbox WHERE message_id LIKE 'MSG-mp11-%'")
            for st in STATIONS:
                ctx.control.execute(
                    "INSERT OR REPLACE INTO simulated_outbox "
                    "(message_id, topic, payload_json, logical_time) VALUES (?, ?, ?, ?)",
                    (f"MSG-mp11-{st}", "station_status",
                     f'{{"station": "{st}", "status": "collected"}}',
                     ctx.clock.at_offset(0)))
        total = sum(p["requests"] for p in per.values())
        worst = max((p["requests"] for p in per.values()), default=0)
        manifest = {
            "mode": "REPLAY",
            "scope": {"stations": list(STATIONS), "target": "station-status collection"},
            "write_mode": "bounded_replace",
            "duplicate_protection": "one idempotent outbox message per station (stable id)",
            "requests_issued": total, "worst_station_requests": worst,
            "stations_collected": sum(1 for p in per.values() if p["succeeded"]),
            "corrected_policy": "single owner, bounded spaced requests, global cap",
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"requests_issued": total})
        return StepReport("recover", "MP-11", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        rec = self._per_station(ctx, "recovery")
        succeeded = sum(1 for p in rec.values() if p["succeeded"])
        worst = max((p["requests"] for p in rec.values()), default=0)
        policy = self._policy(ctx, "current")
        outbox = db.query(ctx.control,
            "SELECT message_id FROM simulated_outbox WHERE message_id LIKE 'MSG-mp11-%'")
        outbox_ids = [r["message_id"] for r in outbox]

        assertions = [
            Assertion("MP-11-VER-CORRECT", "correctness",
                      "the corrected replay collects every station",
                      expected=len(STATIONS), actual=succeeded,
                      status="PASS" if succeeded == len(STATIONS) and rec else "FAIL",
                      evidence="reports/recovery.json"),
            Assertion("MP-11-VER-BOUNDED", "boundedness",
                      "no station exceeds the bounded request ceiling in the replay",
                      expected=f"<= {PER_STATION_BUDGET}", actual=worst,
                      status="PASS" if rec and worst <= PER_STATION_BUDGET else "FAIL"),
            Assertion("MP-11-VER-CONTROL", "control_consistency",
                      "request policy is a single owner with backoff, ceiling, and a global cap",
                      expected=1, actual=policy["owner_layers"],
                      status="PASS" if (policy["owner_layers"] == 1 and policy["use_backoff"] == 1
                                        and policy["request_budget"] > 0
                                        and policy["concurrency_cap"] > 0) else "FAIL"),
            Assertion("MP-11-VER-SIDEEFFECT", "side_effects",
                      "exactly one idempotent published status per station",
                      expected=len(STATIONS), actual=len(outbox_ids),
                      status="PASS" if len(outbox_ids) == len(STATIONS) else "FAIL"),
            Assertion("MP-11-VER-IDEMPOTENT", "idempotency",
                      "no duplicate published-status message id",
                      expected=len(outbox_ids), actual=len(set(outbox_ids)),
                      status="PASS" if len(outbox_ids) == len(set(outbox_ids)) else "FAIL"),
            cc.fixture_integrity_assertion(ctx, "MP-11-VER-FIXTURE"),
        ]
        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-11", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _collect(self, faulty: bool) -> list[tuple[str, list[dict[str, Any]]]]:
        out = []
        for st in STATIONS:
            if faulty:
                attempts = sim.layered_immediate_collect(st, RECOVERY_OFFSET_S, LAYER_FANOUT)
            else:
                attempts = sim.single_owner_collect(
                    st, RECOVERY_OFFSET_S, SERVER_DELAY_S, BACKOFF_BASE_S,
                    HEALTHY_MAX_ATTEMPTS, use_backoff=True, use_jitter=True)
            out.append((st, attempts))
        return out

    def _persist_collection(self, ctx, phase, station_attempts) -> None:
        rows = []
        api_rows = []
        seq = 0
        for station, attempts in station_attempts:
            for a in attempts:
                seq += 1
                rows.append((phase, seq, station, a["layer"], a["n"], a["offset_s"],
                             a["response_class"], a["succeeded"]))
                if phase in ("faulty", "recovery"):
                    prefix = "F" if phase == "faulty" else "R"
                    api_rows.append((
                        f"MP11-{prefix}-{seq:05d}", "station_status_api", station, None, None,
                        200 if a["response_class"] == "OK" else 429, a["n"], a["layer"],
                        ctx.clock.at_offset(a["offset_s"])))
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp11_fetch_attempt WHERE attempt_kind = ?", (phase,))
            db.insert_rows(ctx.control, "mp11_fetch_attempt",
                           ("attempt_kind", "seq", "station", "layer", "n", "offset_s",
                            "response_class", "succeeded"), rows)
            if phase in ("faulty", "recovery"):
                like = "MP11-F-%" if phase == "faulty" else "MP11-R-%"
                ctx.control.execute("DELETE FROM api_request_log WHERE request_id LIKE ?", (like,))
                for r in api_rows:
                    ctx.control.execute(
                        "INSERT OR REPLACE INTO api_request_log "
                        "(request_id, source_name, snapshot_id, page_token, next_token, "
                        " response_code, attempt, worker, logical_time) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", r)

    def _per_station(self, ctx, phase) -> dict[str, dict[str, Any]]:
        rows = db.query(ctx.control,
            "SELECT station, COUNT(*) AS requests, MAX(succeeded) AS ok "
            "FROM mp11_fetch_attempt WHERE attempt_kind = ? "
            "GROUP BY station ORDER BY station", (phase,))
        return {r["station"]: {"requests": r["requests"], "succeeded": bool(r["ok"])}
                for r in rows}

    def _effective_phase(self, ctx) -> str:
        has_recovery = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM mp11_fetch_attempt WHERE attempt_kind = 'recovery'")
        return "recovery" if has_recovery else "faulty"

    def _set_policy(self, ctx, policy_id, p) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp11_retry_policy "
                "(policy_id, owner_layers, max_attempts, backoff_base_s, use_backoff, "
                " use_jitter, request_budget, concurrency_cap, honor_server_delay) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (policy_id, p["owner_layers"], p["max_attempts"], p["backoff_base_s"],
                 p["use_backoff"], p["use_jitter"], p["request_budget"],
                 p["concurrency_cap"], p["honor_server_delay"]))

    def _policy(self, ctx, policy_id) -> dict[str, Any]:
        row = db.query_one(ctx.control,
            "SELECT * FROM mp11_retry_policy WHERE policy_id = ?", (policy_id,))
        return dict(row) if row is not None else {}


INCIDENT = MP11Incident()
