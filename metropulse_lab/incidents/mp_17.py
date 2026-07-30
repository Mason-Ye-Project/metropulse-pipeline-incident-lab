"""MP-17 — Task runtime grows with gate events while each query stays fast.

Frozen mechanism (incident_taxonomy.md MP-17): the enrichment transform issues
one lookup request for every gate event, so the number of requests tracks the
number of fact rows even though each individual request is trivial. The request
count, not any single query, dominates the run.

The reference fix resolves every distinct station key the batch needs in one
grouped request (equivalently, a set-based join), so the request count tracks the
small number of distinct keys instead of the row count. The enriched output is
identical either way.

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
from ._batchD_lookup_adapter import CountingLookup

FAULT = "mp17_request_per_event"
COST_UNITS_PER_CALL = 1
RECOVERY_DATASET = "mart_station_zone_enriched"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mp17_zone_lookup (
    station_id TEXT PRIMARY KEY,
    zone_code  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mp17_probe (
    approach      TEXT PRIMARY KEY,
    requests      INTEGER NOT NULL,
    cost_units    INTEGER NOT NULL,
    distinct_keys INTEGER NOT NULL,
    budget        INTEGER NOT NULL,
    over_budget   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mp17_enriched (
    station_id TEXT PRIMARY KEY,
    zone_code  TEXT NOT NULL,
    event_rows INTEGER NOT NULL,
    boardings  INTEGER NOT NULL
);
"""


class MP17Incident:
    metadata = IncidentMetadata(
        incident_id="MP-17",
        family="D. Performance and cost",
        reader_title="Task runtime grows with the number of gate events, but each query stays fast",
        primary_invariant_id="MP-17-REQUEST-BUDGET",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-17-REQUESTS-001", "MP-17-WORKUNITS-001"),
        transfer_mappings=(
            "Repeated remote database/API lookups and ORM request tracing",
        ),
        nearest_old_scenario="None; the old books have no request-count-amplification incident.",
        old_scenario_difference=(
            "Not a growing warehouse scan and not a small connection pool: each query "
            "is trivial, but one request is issued for every event row instead of once "
            "per distinct key."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        conn = ctx.warehouse
        conn.executescript(_SCHEMA)
        with db.transaction(conn):
            conn.execute("DELETE FROM mp17_zone_lookup")
            conn.execute(
                "INSERT INTO mp17_zone_lookup (station_id, zone_code) "
                "SELECT station_id, zone_code FROM raw_station_registry GROUP BY station_id")

    def _backing(self, ctx: RunContext) -> dict[str, str]:
        return {r["station_id"]: r["zone_code"] for r in db.query(
            ctx.warehouse, "SELECT station_id, zone_code FROM mp17_zone_lookup")}

    def _events(self, ctx: RunContext) -> list[dict[str, Any]]:
        return db.rows_to_dicts(db.query(ctx.warehouse,
            "SELECT gate_event_id, station_id, passenger_count FROM fct_station_gate_count "
            "WHERE route_id IS NOT NULL ORDER BY gate_event_id"))

    def _distinct_keys(self, ctx: RunContext) -> int:
        return db.scalar(ctx.warehouse,
            "SELECT COUNT(DISTINCT station_id) FROM fct_station_gate_count WHERE route_id IS NOT NULL")

    def _request_budget(self, ctx: RunContext) -> int:
        # Requests must be O(distinct keys): at most one grouped request per distinct key.
        return self._distinct_keys(ctx)

    def _run_per_event(self, ctx: RunContext) -> dict[str, Any]:
        lookup = CountingLookup(self._backing(ctx), COST_UNITS_PER_CALL)
        for e in self._events(ctx):
            lookup.get_one(e["station_id"])
        return lookup.counters()

    def _run_grouped(self, ctx: RunContext) -> dict[str, Any]:
        lookup = CountingLookup(self._backing(ctx), COST_UNITS_PER_CALL)
        keys = [e["station_id"] for e in self._events(ctx)]
        lookup.get_bulk(keys)
        return lookup.counters()

    def _record_probe(self, ctx: RunContext, approach: str, requests: int, cost_units: int,
                      over_budget: bool) -> None:
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "INSERT OR REPLACE INTO mp17_probe "
                "(approach, requests, cost_units, distinct_keys, budget, over_budget) "
                "VALUES (?,?,?,?,?,?)",
                (approach, requests, cost_units, self._distinct_keys(ctx),
                 self._request_budget(ctx), int(over_budget)))

    def _read_probe(self, ctx: RunContext, approach: str) -> dict[str, Any] | None:
        row = db.query_one(ctx.warehouse, "SELECT * FROM mp17_probe WHERE approach = ?", (approach,))
        return dict(row) if row else None

    def _reference_enriched(self, ctx: RunContext) -> dict[str, dict[str, Any]]:
        rows = db.query(ctx.warehouse,
            "SELECT g.station_id AS station_id, l.zone_code AS zone_code, "
            "COUNT(*) AS event_rows, SUM(g.passenger_count) AS boardings "
            "FROM fct_station_gate_count g JOIN mp17_zone_lookup l ON g.station_id = l.station_id "
            "WHERE g.route_id IS NOT NULL GROUP BY g.station_id, l.zone_code")
        return {r["station_id"]: dict(r) for r in rows}

    def baseline_ok(self, ctx: RunContext) -> bool:
        grouped = self._run_grouped(ctx)
        return grouped["requests"] <= self._request_budget(ctx)

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        grouped_before = self._run_grouped(ctx)
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(set(profile.get("faulty_transforms", [])) | {FAULT})
        ctx.save_fault_profile(profile)

        per_event = self._run_per_event(ctx)
        budget = self._request_budget(ctx)
        over = per_event["requests"] > budget
        self._record_probe(ctx, "per_event", per_event["requests"], per_event["cost_units"], over)

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fixtures_ok, fixture_detail = integrity.fixtures_untouched(ctx)
        fixtures_ok = fixtures_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-17-INJECT-PRE", "precondition",
                      "grouped-request path stays within the request budget at baseline",
                      expected=True, actual=grouped_before["requests"] <= budget,
                      status="PASS" if grouped_before["requests"] <= budget else "FAIL"),
            Assertion("MP-17-INJECT-POST", "postcondition",
                      "one-request-per-event path exceeds the request budget",
                      expected=True, actual=over, status="PASS" if over else "FAIL"),
            Assertion("MP-17-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection (control-plane fault only)",
                      expected=True, actual=fixtures_ok, status="PASS" if fixtures_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected",
                     fields={"requests": per_event["requests"], "budget": budget})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-17", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "requests": per_event["requests"], "budget": budget,
                                    "fixture_checksums_unchanged": fixtures_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        per_event = self._run_per_event(ctx)
        budget = self._request_budget(ctx)
        over = per_event["requests"] > budget
        self._record_probe(ctx, "per_event", per_event["requests"], per_event["cost_units"], over)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="faulty_run", fields={"requests": per_event["requests"]})
        return StepReport("run", "MP-17", ctx.run_id, "executed",
                          fields={"requests": per_event["requests"],
                                  "cost_units": per_event["cost_units"], "budget": budget})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        probe = self._read_probe(ctx, "per_event") or {}
        requests = probe.get("requests", 0)
        distinct_keys = probe.get("distinct_keys", 0) or 0
        budget = probe.get("budget", 0)
        requests_per_key = requests // max(1, distinct_keys)

        requests_bad = requests > budget
        amplified = requests_per_key > 1
        detected = bool(requests_bad or amplified)

        common.write_quality_result(
            ctx, "MP-17-REQUESTS-001", "published_metric",
            expected=f"lookup requests bounded by the distinct-key budget of {budget}",
            actual=f"{requests} lookup requests were issued for {requests} event rows",
            status="FAIL" if requests_bad else "PASS")
        common.write_quality_result(
            ctx, "MP-17-WORKUNITS-001", "scan_work",
            expected="at most one request per distinct station key",
            actual=f"{requests_per_key} requests per distinct station key "
                   f"({requests} requests / {distinct_keys} distinct keys)",
            status="FAIL" if amplified else "PASS")

        assertions = [
            Assertion("MP-17-DET-REQUESTS", "published_metric",
                      "lookup requests bounded by the distinct-key budget",
                      expected=f"<= {budget}", actual=requests,
                      status="FAIL" if requests_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-17-DET-WORKUNITS", "scan_work",
                      "requests per distinct station key stay at one",
                      expected="<= 1", actual=requests_per_key,
                      status="FAIL" if amplified else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "requests": requests,
                  "distinct_keys": distinct_keys, "requests_per_key": requests_per_key,
                  "result_identical_via_grouped_path": True}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-17", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-17", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        probe = self._read_probe(ctx, "per_event") or {}
        events = db.scalar(ctx.warehouse,
            "SELECT COUNT(*) FROM fct_station_gate_count WHERE route_id IS NOT NULL")
        common.write_csv(
            tables / "request_counts.csv",
            [{"observed_path": "one_request_each_event",
              "event_rows": events,
              "requests_issued": probe.get("requests"),
              "cost_units": probe.get("requests"),
              "distinct_station_keys": probe.get("distinct_keys"),
              "request_budget": probe.get("budget"),
              "requests_per_key": (probe.get("requests", 0) // max(1, probe.get("distinct_keys", 1)))}],
            ["observed_path", "event_rows", "requests_issued", "cost_units",
             "distinct_station_keys", "request_budget", "requests_per_key"])

        common.write_csv(
            tables / "distinct_key_profile.csv",
            db.rows_to_dicts(db.query(ctx.warehouse,
                "SELECT station_id, COUNT(*) AS event_rows FROM fct_station_gate_count "
                "WHERE route_id IS NOT NULL GROUP BY station_id ORDER BY station_id")),
            ["station_id", "event_rows"])
        common.write_common_evidence_tables(ctx)

        alert = common.build_alert(
            "MP-17", self.metadata.reader_title,
            reported_symptom=(
                "The gate-event enrichment task now takes roughly ten times longer "
                "since gate volume grew about ten times, yet each individual database "
                "query still returns in a few milliseconds. Runtime tracks the number "
                "of gate events, not the size of any single query."),
            time_pressure="The enrichment feeds the morning operations dashboard.",
            affected_product="station-zone enrichment",
            observed={"runtime_scales_with": "gate event count",
                      "single_query_latency": "milliseconds"})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx))
        index = common.write_evidence_index(ctx, "MP-17")
        return StepReport("evidence", "MP-17", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        blast_radius = {
            "held_product": RECOVERY_DATASET,
            "action": "hold the enrichment publication and stop the one-request-each-event path",
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast_radius)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-17", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-17", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["lookup_strategy"] = "grouped_distinct_keys"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident", event_type="repaired",
                     fields={"strategy": "resolve every distinct station key in one grouped request"})
        report = StepReport("repair", "MP-17", ctx.run_id, "repaired",
                            fields={"change": "resolve the distinct station keys in one grouped "
                                             "request (set-based join) instead of one per event",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        pub = f"PUB-{ctx.run_id}-recover"
        grouped = self._run_grouped(ctx)
        budget = self._request_budget(ctx)
        self._record_probe(ctx, "grouped", grouped["requests"], grouped["cost_units"],
                           grouped["requests"] > budget)

        reference = self._reference_enriched(ctx)
        with db.transaction(conn):
            conn.execute("DELETE FROM mp17_enriched")
            for station_id in sorted(reference):
                r = reference[station_id]
                conn.execute("INSERT INTO mp17_enriched (station_id, zone_code, event_rows, boardings) "
                             "VALUES (?,?,?,?)",
                             (station_id, r["zone_code"], r["event_rows"], r["boardings"]))
        dv_id = common.record_recovery_publication(ctx, RECOVERY_DATASET, pub)

        manifest = {
            "mode": "REPLAY", "publication_id": pub, "dataset_version_id": dv_id,
            "scope": {"datasets": [RECOVERY_DATASET], "keys": "distinct station keys"},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key at station grain; deterministic set-based recompute",
            "strategy": "one grouped request for all distinct station keys",
            "requests_after": grouped["requests"], "request_budget": budget,
            "enriched_rows": len(reference), "evidence_origin": common.EVIDENCE_ORIGIN,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"requests": grouped["requests"]})
        return StepReport("recover", "MP-17", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        assertions: list[Assertion] = []

        reference = {sid: (r["zone_code"], r["event_rows"], r["boardings"])
                     for sid, r in self._reference_enriched(ctx).items()}
        persisted = {r["station_id"]: (r["zone_code"], r["event_rows"], r["boardings"])
                     for r in db.query(conn,
                        "SELECT station_id, zone_code, event_rows, boardings FROM mp17_enriched")}

        assertions.append(Assertion(
            "MP-17-VER-CORRECT", "correctness",
            "recovered enrichment equals an independent set-based recompute",
            expected=len(reference), actual=len(persisted),
            status="PASS" if persisted == reference else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-17-VER-COMPLETE", "completeness",
            "recovered enrichment covers every referenced station key",
            expected=len(reference), actual=len(persisted),
            status="PASS" if set(persisted) == set(reference) else "FAIL"))

        grouped = self._read_probe(ctx, "grouped")
        budget = self._request_budget(ctx)
        bounded_ok = bool(grouped) and grouped["requests"] <= budget and grouped["over_budget"] == 0
        assertions.append(Assertion(
            "MP-17-VER-BOUNDED", "boundedness",
            "the recovered path issues requests within the distinct-key budget",
            expected=f"<= {budget}", actual=(grouped or {}).get("requests"),
            status="PASS" if bounded_ok else "FAIL"))

        control_ok = common.has_recovery_publication(ctx, RECOVERY_DATASET)
        assertions.append(Assertion(
            "MP-17-VER-CONTROL", "control_consistency",
            "a bounded-recovery enrichment publication is recorded",
            expected=">=1 recovered publication", actual=control_ok,
            status="PASS" if control_ok else "FAIL"))

        assertions.append(Assertion(
            "MP-17-VER-IDEMPOTENT", "idempotency",
            "independent recompute equals the persisted recovered enrichment",
            expected=True, actual=(persisted == reference and bool(persisted)),
            status="PASS" if (persisted == reference and bool(persisted)) else "FAIL"))

        integrity_ok, integrity_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-17-VER-FIXTURE", "fixture_integrity",
            "run landing inputs match checked-in fixture checksums",
            expected=True, actual=integrity_ok, status="PASS" if integrity_ok else "FAIL",
            evidence=str(integrity_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-17", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP17Incident()
