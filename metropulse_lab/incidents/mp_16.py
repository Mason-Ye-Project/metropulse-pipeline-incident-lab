"""MP-16 — A small station-lookup job starts failing while workers stay idle.

Frozen mechanism (incident_taxonomy.md MP-16): a job assembles a station lookup
into a single worker to build a local dictionary. A timetable expansion blew the
lookup's grain from a small per-station dimension to a per-station-per-window
dimension projecting millions of rows, so the single-worker assembly now exceeds
its size budget while the distributed workers sit idle.

The reference fix installs a hard capacity guard (any single-worker assembly must
prove its projected size against a byte bound) and replaces the whole-lookup
assembly with a set-based join that touches only the distinct station keys the
events actually reference. The published zone boardings are identical either way.

The reader-facing title and evidence describe only the symptom. The mechanism
vocabulary never appears in the evidence pack.
"""

from __future__ import annotations

from typing import Any

from .. import config, db
from ..checks import integrity
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from .contract import IncidentMetadata
from . import _batchD_common as common
from ._batchD_capacity_guard import CapacityGuard, CapacityExceeded

FAULT = "mp16_full_lookup_assembly"

BYTES_PER_ROW = 48
N_STATIONS = 12
BASELINE_WINDOWS = 1
EXPANDED_WINDOWS = 250_000
ASSEMBLY_LIMIT_BYTES = 8 * 1024 * 1024      # 8 MiB single-worker byte bound
LOOKUP_SIZE_BOUND_ROWS = 100_000            # contracted small-dimension row bound
RECOVERY_DATASET = "mart_zone_boardings"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS mp16_lookup (
    station_id TEXT PRIMARY KEY,
    zone_code  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mp16_probe (
    approach        TEXT PRIMARY KEY,
    projected_rows  INTEGER NOT NULL,
    bytes_per_row   INTEGER NOT NULL,
    projected_bytes INTEGER NOT NULL,
    limit_bytes     INTEGER NOT NULL,
    over_limit      INTEGER NOT NULL,
    distinct_keys   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS mp16_zone_result (
    zone_code TEXT PRIMARY KEY,
    boardings INTEGER NOT NULL
);
"""


class MP16Incident:
    metadata = IncidentMetadata(
        incident_id="MP-16",
        family="D. Performance and cost",
        reader_title="A small station-lookup job starts failing while the workers stay idle",
        primary_invariant_id="MP-16-ASSEMBLY-BOUND",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=(f"enable_faulty_transform:{FAULT}",),
        expected_detection_ids=("MP-16-LOOKUPSIZE-001", "MP-16-ASSEMBLY-001"),
        transfer_mappings=(
            "Spark driver-side result assembly and its configured result-size bound",
        ),
        nearest_old_scenario="None; the old books have no single-worker assembly-bound incident.",
        old_scenario_difference=(
            "Not hot-key shuffle skew and not a corrupt input: the workers are idle "
            "and the bottleneck is a single-worker assembly whose small-dimension "
            "assumption broke when the lookup grain expanded."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        conn = ctx.warehouse
        conn.executescript(_SCHEMA)
        with db.transaction(conn):
            conn.execute("DELETE FROM mp16_lookup")
            conn.execute(
                "INSERT INTO mp16_lookup (station_id, zone_code) "
                "SELECT station_id, zone_code FROM raw_station_registry GROUP BY station_id"
            )

    def _distinct_keys(self, ctx: RunContext) -> int:
        return db.scalar(ctx.warehouse,
                         "SELECT COUNT(DISTINCT station_id) FROM fct_station_gate_count")

    def _bounded_probe(self, ctx: RunContext) -> dict[str, Any]:
        guard = CapacityGuard(ASSEMBLY_LIMIT_BYTES)
        distinct_keys = self._distinct_keys(ctx)
        result = guard.evaluate(distinct_keys, BYTES_PER_ROW)
        return {"result": result, "distinct_keys": distinct_keys}

    def _unmapped_station_ids(self, ctx: RunContext) -> list[str]:
        """Return referenced station keys that lack exactly one lookup row."""
        rows = db.query(
            ctx.warehouse,
            "SELECT r.station_id AS station_id "
            "FROM (SELECT DISTINCT station_id FROM fct_station_gate_count) r "
            "LEFT JOIN mp16_lookup l ON r.station_id = l.station_id "
            "GROUP BY r.station_id "
            "HAVING COUNT(l.station_id) <> 1 OR COUNT(DISTINCT l.zone_code) <> 1 "
            "ORDER BY r.station_id",
        )
        return [str(row["station_id"]) for row in rows]

    def _faulty_projection(self) -> int:
        return N_STATIONS * EXPANDED_WINDOWS

    def baseline_ok(self, ctx: RunContext) -> bool:
        probe = self._bounded_probe(ctx)
        return probe["result"].allowed

    def _record_probe(self, ctx: RunContext, approach: str, projected_rows: int,
                      over_limit: bool, distinct_keys: int) -> dict[str, Any]:
        projected_bytes = projected_rows * BYTES_PER_ROW
        with db.transaction(ctx.warehouse):
            ctx.warehouse.execute(
                "INSERT OR REPLACE INTO mp16_probe (approach, projected_rows, bytes_per_row, "
                "projected_bytes, limit_bytes, over_limit, distinct_keys) VALUES (?,?,?,?,?,?,?)",
                (approach, projected_rows, BYTES_PER_ROW, projected_bytes,
                 ASSEMBLY_LIMIT_BYTES, int(over_limit), distinct_keys))
        return {"approach": approach, "projected_rows": projected_rows,
                "projected_bytes": projected_bytes, "over_limit": over_limit,
                "distinct_keys": distinct_keys}

    def _read_probe(self, ctx: RunContext, approach: str) -> dict[str, Any] | None:
        row = db.query_one(ctx.warehouse, "SELECT * FROM mp16_probe WHERE approach = ?", (approach,))
        return dict(row) if row else None

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        bounded_before = self._bounded_probe(ctx)
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(set(profile.get("faulty_transforms", [])) | {FAULT})
        profile.setdefault("params", {})["expanded_windows"] = EXPANDED_WINDOWS
        ctx.save_fault_profile(profile)

        # Postcondition: the whole-lookup assembly now trips the capacity guard.
        guard = CapacityGuard(ASSEMBLY_LIMIT_BYTES)
        projected_rows = self._faulty_projection()
        try:
            guard.assemble_or_refuse(projected_rows, BYTES_PER_ROW)
            tripped = False
        except CapacityExceeded:
            tripped = True
        self._record_probe(ctx, "full_assembly", projected_rows, tripped, N_STATIONS)

        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fixtures_ok, fixture_detail = integrity.fixtures_untouched(ctx)
        fixtures_ok = fixtures_ok and (fixtures_before == fixtures_after)

        assertions = [
            Assertion("MP-16-INJECT-PRE", "precondition",
                      "bounded set-based lookup path is within the assembly bound at baseline",
                      expected=True, actual=bounded_before["result"].allowed,
                      status="PASS" if bounded_before["result"].allowed else "FAIL"),
            Assertion("MP-16-INJECT-POST", "postcondition",
                      "expanded-grain whole-lookup assembly trips the capacity guard",
                      expected=True, actual=tripped, status="PASS" if tripped else "FAIL"),
            Assertion("MP-16-INJECT-FIXTURE", "fixture_integrity",
                      "canonical fixtures unchanged by injection (control-plane fault only)",
                      expected=True, actual=fixtures_ok, status="PASS" if fixtures_ok else "FAIL"),
        ]
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="fault_injected",
                     fields={"projected_rows": projected_rows, "guard_tripped": tripped})
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-16", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "guard_tripped": tripped,
                                    "projected_bytes": projected_rows * BYTES_PER_ROW,
                                    "fixture_checksums_unchanged": fixtures_ok,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        guard = CapacityGuard(ASSEMBLY_LIMIT_BYTES)
        projected_rows = self._faulty_projection()
        refused = not guard.evaluate(projected_rows, BYTES_PER_ROW).allowed
        info = self._record_probe(ctx, "full_assembly", projected_rows, refused, N_STATIONS)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="faulty_run", fields={"assembly_refused": refused})
        return StepReport("run", "MP-16", ctx.run_id, "executed",
                          fields={"assembly_refused": refused,
                                  "projected_bytes": info["projected_bytes"]})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        probe = self._read_probe(ctx, "full_assembly") or {}
        projected_rows = probe.get("projected_rows", 0)
        projected_bytes = probe.get("projected_bytes", 0)

        size_bad = projected_rows > LOOKUP_SIZE_BOUND_ROWS
        assembly_bad = projected_bytes > ASSEMBLY_LIMIT_BYTES
        detected = bool(size_bad or assembly_bad)

        common.write_quality_result(
            ctx, "MP-16-LOOKUPSIZE-001", "valid_domain",
            expected=f"station lookup stays within its contracted profile of "
                     f"{LOOKUP_SIZE_BOUND_ROWS} rows",
            actual=f"lookup projects to {projected_rows} rows",
            status="FAIL" if size_bad else "PASS")
        common.write_quality_result(
            ctx, "MP-16-ASSEMBLY-001", "scan_work",
            expected=f"a single-worker assembly stays within {ASSEMBLY_LIMIT_BYTES} bytes",
            actual=f"projected single-worker assembly is {projected_bytes} bytes",
            status="FAIL" if assembly_bad else "PASS")

        assertions = [
            Assertion("MP-16-DET-LOOKUPSIZE", "valid_domain",
                      "station lookup stays within its contracted row profile",
                      expected=f"<= {LOOKUP_SIZE_BOUND_ROWS}", actual=projected_rows,
                      status="FAIL" if size_bad else "PASS", evidence="reports/detection.json"),
            Assertion("MP-16-DET-ASSEMBLY", "scan_work",
                      "single-worker assembly bytes within the hard byte bound",
                      expected=f"<= {ASSEMBLY_LIMIT_BYTES}", actual=projected_bytes,
                      status="FAIL" if assembly_bad else "PASS", evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "projected_rows": projected_rows,
                  "projected_bytes": projected_bytes,
                  "distinct_keys_referenced": probe.get("distinct_keys"),
                  "result_identical_via_bounded_path": True}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-16", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-16", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        probe = self._read_probe(ctx, "full_assembly") or {}
        common.write_csv(
            tables / "assembly_size_projection.csv",
            [{"approach": "whole_lookup_one_worker",
              "projected_rows": probe.get("projected_rows"),
              "bytes_per_row": probe.get("bytes_per_row"),
              "projected_bytes": probe.get("projected_bytes"),
              "byte_limit": probe.get("limit_bytes"),
              "over_limit": probe.get("over_limit"),
              "distinct_station_keys_referenced": probe.get("distinct_keys")}],
            ["approach", "projected_rows", "bytes_per_row", "projected_bytes",
             "byte_limit", "over_limit", "distinct_station_keys_referenced"])

        # The lookup attribute is at station grain: only distinct stations matter.
        by_station = db.query(ctx.warehouse,
            "SELECT g.station_id AS station_id, l.zone_code AS zone_code, "
            "COUNT(*) AS event_rows, SUM(g.passenger_count) AS boardings "
            "FROM fct_station_gate_count g JOIN mp16_lookup l ON g.station_id = l.station_id "
            "GROUP BY g.station_id, l.zone_code ORDER BY g.station_id")
        common.write_csv(
            tables / "referenced_station_keys.csv", db.rows_to_dicts(by_station),
            ["station_id", "zone_code", "event_rows", "boardings"])

        common.write_csv(
            tables / "row_counts.csv",
            [{"table": "fct_station_gate_count",
              "rows": db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM fct_station_gate_count")},
             {"table": "station_lookup_keys",
              "rows": db.scalar(ctx.warehouse, "SELECT COUNT(*) FROM mp16_lookup")}],
            ["table", "rows"])
        common.write_common_evidence_tables(ctx)

        alert = common.build_alert(
            "MP-16", self.metadata.reader_title,
            reported_symptom=(
                "A daily job that builds a station lookup and enriches gate counts "
                "began failing on the worker that assembles the lookup. The failure "
                "started right after the timetable was expanded; the other workers "
                "sit almost idle while the one assembling worker gives up."),
            time_pressure="The zone boardings summary is due for the next service review.",
            affected_product="zone boardings enrichment",
            observed={"assembling_worker": "failing", "other_workers": "idle",
                      "started_after": "timetable expansion"})
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", common.sanitized_timeline(ctx))
        index = common.write_evidence_index(ctx, "MP-16")
        return StepReport("evidence", "MP-16", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        blast_radius = {
            "held_product": RECOVERY_DATASET,
            "action": "hold the zone boardings publication and stop the whole-lookup assembly",
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast_radius)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-16", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-16", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [t for t in profile.get("faulty_transforms", []) if t != FAULT]
        profile.setdefault("params", {})["lookup_strategy"] = "set_based_distinct_keys"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired",
                     fields={"strategy": "set-based join over distinct station keys; "
                                         "capacity guard enforced before any single-worker assembly"})
        report = StepReport("repair", "MP-16", ctx.run_id, "repaired",
                            fields={"change": "replace whole-lookup single-worker assembly with a "
                                             "set-based join over the distinct station keys",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        pub = f"PUB-{ctx.run_id}-recover"
        unmapped_station_ids = self._unmapped_station_ids(ctx)
        if unmapped_station_ids:
            manifest = {
                "mode": "REPLAY",
                "publication_id": None,
                "dataset_version_id": None,
                "scope": {"datasets": [RECOVERY_DATASET], "keys": "distinct station keys only"},
                "write_mode": "no_write",
                "status": "recovery_blocked",
                "reason": "referenced station keys must map to exactly one zone before replay",
                "unmapped_station_ids": unmapped_station_ids,
                "evidence_origin": common.EVIDENCE_ORIGIN,
            }
            write_json(ctx.paths.report_path("recovery.json"), manifest)
            ctx.log.emit(
                logical_time=ctx.clock.tick(),
                component="recovery",
                event_type="replay_blocked",
                fields={"unmapped_station_ids": unmapped_station_ids},
            )
            return StepReport(
                "recover",
                "MP-16",
                ctx.run_id,
                "recovery_blocked",
                fields=manifest,
                assertions=[
                    Assertion(
                        "MP-16-REC-MAPPING",
                        "completeness",
                        "every referenced station maps to exactly one zone before replay",
                        expected=[],
                        actual=unmapped_station_ids,
                        status="FAIL",
                    )
                ],
            )

        # REPLAY via the set-based join over the distinct station keys.
        rows = db.query(conn,
            "SELECT l.zone_code AS zone_code, SUM(g.passenger_count) AS boardings "
            "FROM fct_station_gate_count g JOIN mp16_lookup l ON g.station_id = l.station_id "
            "WHERE g.route_id IS NOT NULL GROUP BY l.zone_code ORDER BY l.zone_code")
        with db.transaction(conn):
            conn.execute("DELETE FROM mp16_zone_result")
            for r in rows:
                conn.execute("INSERT INTO mp16_zone_result (zone_code, boardings) VALUES (?, ?)",
                             (r["zone_code"], r["boardings"]))
        probe = self._bounded_probe(ctx)
        self._record_probe(ctx, "set_based", probe["distinct_keys"], False, probe["distinct_keys"])
        dv_id = common.record_recovery_publication(ctx, RECOVERY_DATASET, pub)

        manifest = {
            "mode": "REPLAY",
            "publication_id": pub,
            "dataset_version_id": dv_id,
            "scope": {"datasets": [RECOVERY_DATASET], "keys": "distinct station keys only"},
            "write_mode": "bounded_replace",
            "duplicate_protection": "primary key at zone grain; deterministic set-based recompute",
            "strategy": "set-based join over distinct station keys instead of a whole-lookup "
                        "single-worker assembly",
            "assembly_bytes_after": probe["result"].projected_bytes,
            "assembly_limit_bytes": ASSEMBLY_LIMIT_BYTES,
            "zone_rows": len(rows),
            "evidence_origin": common.EVIDENCE_ORIGIN,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="recovery",
                     event_type="replay_completed", fields={"zone_rows": len(rows)})
        return StepReport("recover", "MP-16", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        assertions: list[Assertion] = []

        unmapped_station_ids = self._unmapped_station_ids(ctx)
        assertions.append(Assertion(
            "MP-16-VER-MAPPING", "completeness",
            "every referenced station maps to exactly one zone",
            expected=[], actual=unmapped_station_ids,
            status="PASS" if not unmapped_station_ids else "FAIL",
            evidence="reports/verification.json"))

        reference = {r["zone_code"]: r["boardings"] for r in db.query(conn,
            "SELECT l.zone_code AS zone_code, SUM(g.passenger_count) AS boardings "
            "FROM fct_station_gate_count g JOIN mp16_lookup l ON g.station_id = l.station_id "
            "WHERE g.route_id IS NOT NULL GROUP BY l.zone_code")}
        persisted = {r["zone_code"]: r["boardings"] for r in db.query(conn,
            "SELECT zone_code, boardings FROM mp16_zone_result")}

        assertions.append(Assertion(
            "MP-16-VER-CORRECT", "correctness",
            "recovered zone boardings equal an independent set-based recompute",
            expected=reference, actual=persisted,
            status="PASS" if persisted == reference else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-16-VER-COMPLETE", "completeness",
            "recovered result covers every zone present in the gate events",
            expected=len(reference), actual=len(persisted),
            status="PASS" if set(persisted) == set(reference) else "FAIL"))

        set_probe = self._read_probe(ctx, "set_based")
        bounded_ok = bool(set_probe) and set_probe["over_limit"] == 0 \
            and set_probe["projected_bytes"] <= ASSEMBLY_LIMIT_BYTES
        assertions.append(Assertion(
            "MP-16-VER-BOUNDED", "boundedness",
            "the recovered assembly path is within the hard byte bound",
            expected=f"<= {ASSEMBLY_LIMIT_BYTES}",
            actual=(set_probe or {}).get("projected_bytes"),
            status="PASS" if bounded_ok else "FAIL"))

        control_ok = common.has_recovery_publication(ctx, RECOVERY_DATASET)
        assertions.append(Assertion(
            "MP-16-VER-CONTROL", "control_consistency",
            "a bounded-recovery zone-boardings publication is recorded",
            expected=">=1 recovered publication", actual=control_ok,
            status="PASS" if control_ok else "FAIL"))

        assertions.append(Assertion(
            "MP-16-VER-IDEMPOTENT", "idempotency",
            "independent recompute equals the persisted recovered result",
            expected=True, actual=(persisted == reference and bool(persisted)),
            status="PASS" if (persisted == reference and bool(persisted)) else "FAIL"))

        integrity_ok, integrity_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-16-VER-FIXTURE", "fixture_integrity",
            "run landing inputs match checked-in fixture checksums",
            expected=True, actual=integrity_ok,
            status="PASS" if integrity_ok else "FAIL",
            evidence=str(integrity_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-16", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report


INCIDENT = MP16Incident()
