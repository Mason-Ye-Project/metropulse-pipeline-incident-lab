"""MP-01 — Published route-hour ridership reads several times the gate control.

Frozen mechanism (incident_taxonomy.md MP-01): the onboard passenger counter is
cumulative since the device booted, but the transform misreads each cumulative
reading as this interval's boardings. The independent station gate-count control
disagrees, and a per-session conservation invariant is violated.

The reader-facing title and evidence describe only the symptom. The transform
name and ``delta_method`` column are never exposed in the evidence pack.
"""

from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .. import config, db
from ..clock import format_utc
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from ..checks import reconciliation, invariants, integrity
from ..checks.snapshots import table_logical_hash
from ..pipeline import build, recovery
from ..pipeline.model import COUNTER_AS_DELTA
from .contract import IncidentMetadata

AFFECTED_TABLES = ("fct_vehicle_counter_delta", "mart_route_hour_ridership")
ALL_OUTPUT_TABLES = tuple(config.WAREHOUSE_TABLES) + tuple(config.MART_TABLES)
UNRELATED_TABLES = tuple(t for t in ALL_OUTPUT_TABLES if t not in AFFECTED_TABLES)
FAULTY_SNAPSHOT = "faulty_snapshots.json"


class MP01Incident:
    metadata = IncidentMetadata(
        incident_id="MP-01",
        family="A. Present but wrong",
        reader_title="Published route-hour ridership reads several times the gate-count control",
        primary_invariant_id="MP-01-CONSERVATION",
        core_evidence_kind="REAL_SQLITE",
        recovery_mode="REPLAY",
        fault_operations=("enable_faulty_transform:counter_as_delta",),
        expected_detection_ids=("MP-01-RECON-001", "MP-01-CONSERVATION-001", "MP-01-CONTROL-001"),
        transfer_mappings=("DuckDB/Spark window functions for session-aware deltas",),
        nearest_old_scenario="None; the old books have no cumulative-counter incident.",
        old_scenario_difference=(
            "Not a duplicate telemetry message and not a real ridership surge; the "
            "source counter is cumulative-since-boot and was summed as if per-event."
        ),
        affected_tables=AFFECTED_TABLES,
    )

    # -- baseline invariant ---------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        # MP-01's clean scenario is the standard ridership baseline; no extra work.
        return None

    def baseline_ok(self, ctx: RunContext) -> bool:
        recon = reconciliation.ridership_vs_gate(ctx.warehouse)
        return not reconciliation.violations(recon, config.RIDERSHIP_RECONCILE_TOLERANCE)

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        # Precondition: the clean baseline currently reconciles.
        recon_before = reconciliation.ridership_vs_gate(ctx.warehouse)
        violations_before = reconciliation.violations(recon_before, config.RIDERSHIP_RECONCILE_TOLERANCE)

        # Section 10: record fixture checksums before the mutation.
        fixtures_before = integrity.landing_checksums(ctx.paths.landing_dir)

        # Arm the faulty transform branch (bounded control-plane mutation).
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(set(profile.get("faulty_transforms", [])) | {COUNTER_AS_DELTA})
        profile.setdefault("params", {})
        ctx.save_fault_profile(profile)

        # Materialize the faulty state so the postcondition is observable.
        build.build_all(ctx, "faulty")

        recon_after = reconciliation.ridership_vs_gate(ctx.warehouse)
        violations_after = reconciliation.violations(recon_after, config.RIDERSHIP_RECONCILE_TOLERANCE)

        # Section 10: prove the canonical fixtures were not altered by the fault.
        fixtures_after = integrity.landing_checksums(ctx.paths.landing_dir)
        fixtures_unchanged, fixture_detail = integrity.fixtures_untouched(ctx)
        fixtures_unchanged = fixtures_unchanged and (fixtures_before == fixtures_after)

        assertions = [
            Assertion(
                "MP-01-INJECT-PRE", "precondition",
                "clean baseline reconciles before injection",
                expected=0, actual=len(violations_before),
                status="PASS" if not violations_before else "FAIL",
            ),
            Assertion(
                "MP-01-INJECT-POST", "postcondition",
                "faulty run violates ridership reconciliation",
                expected=">0", actual=len(violations_after),
                status="PASS" if violations_after else "FAIL",
            ),
            Assertion(
                "MP-01-INJECT-FIXTURE", "fixture_integrity",
                "canonical fixtures unchanged by injection (fault is control-plane only)",
                expected=True, actual=fixtures_unchanged,
                status="PASS" if fixtures_unchanged else "FAIL",
            ),
        ]
        ctx.log.emit(
            logical_time=ctx.clock.tick(), component="incident",
            event_type="fault_injected",
            fields={"fault": "counter_as_delta", "violations": len(violations_after)},
        )
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-01", ctx.run_id, status,
                            fields={"armed": profile["faulty_transforms"],
                                    "reconciliation_violations": len(violations_after),
                                    "fixture_checksums_unchanged": fixtures_unchanged,
                                    "fixture_detail": fixture_detail,
                                    "landing_files_checked": len(fixtures_before)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    # -- run faulty -----------------------------------------------------------
    def run_faulty(self, ctx: RunContext) -> StepReport:
        summary = build.build_all(ctx, "faulty")
        return StepReport("run", "MP-01", ctx.run_id, "executed",
                          fields={"publication_id": summary["publication_id"],
                                  "ridership_rows": summary["publish"]["mart_route_hour_ridership"]})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        recon = reconciliation.ridership_vs_gate(ctx.warehouse)
        recon_bad = reconciliation.violations(recon, config.RIDERSHIP_RECONCILE_TOLERANCE)
        cons = invariants.counter_session_conservation(ctx.warehouse)
        cons_bad = invariants.conservation_violations(cons)
        control = self._control_plane_signal(ctx, data_wrong=bool(recon_bad))

        worst = max(recon, key=lambda r: (r["ratio"] or 0)) if recon else None
        detected = bool(recon_bad) or bool(cons_bad)

        self._write_quality_result(
            ctx, "MP-01-RECON-001", "reconciliation",
            expected="route-hour boardings within tolerance of gate control",
            actual=f"{len(recon_bad)} route-hours exceed tolerance",
            status="FAIL" if recon_bad else "PASS",
        )
        self._write_quality_result(
            ctx, "MP-01-CONSERVATION-001", "conservation",
            expected="attributed boardings must not exceed the largest counter reading per session",
            actual=f"{len(cons_bad)} sessions exceed the largest counter reading",
            status="FAIL" if cons_bad else "PASS",
        )
        self._write_quality_result(
            ctx, "MP-01-CONTROL-001", "control_consistency",
            expected="a PUBLISHED, fully-committed ridership dataset must be reconciled",
            actual=("control plane reports success while the published metric is "
                    "unreconciled" if control["gap"] else "control and data agree"),
            status="FAIL" if control["gap"] else "PASS",
        )

        assertions = [
            Assertion("MP-01-DET-RECON", "reconciliation",
                      "reconciliation residual within tolerance",
                      expected=0, actual=len(recon_bad),
                      status="FAIL" if recon_bad else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-01-DET-CONS", "conservation",
                      "per-session boardings within cumulative bound",
                      expected=0, actual=len(cons_bad),
                      status="FAIL" if cons_bad else "PASS",
                      evidence="reports/detection.json"),
            # Control-plane signal: a green, fully-committed publication that is
            # not reconciled is itself a failed observability invariant.
            Assertion("MP-01-DET-CONTROL", "control_consistency",
                      "published+committed ridership implies reconciled data",
                      expected=False, actual=control["gap"],
                      status="FAIL" if control["gap"] else "PASS",
                      evidence="reports/detection.json"),
        ]
        fields = {
            "incident_detected": detected,
            "reconciliation_violations": len(recon_bad),
            "conservation_violations": len(cons_bad),
            "control_plane": control,
            "worst_route_hour": None if worst is None else {
                "service_date": worst["service_date"], "route_id": worst["route_id"],
                "service_hour_utc": worst["service_hour_utc"],
                "boardings": worst["boardings"], "gate_control": worst["gate_control"],
                "ratio": round(worst["ratio"], 2) if worst["ratio"] else None,
            },
        }
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-01", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-01", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- collect evidence (read-only) -----------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        ev = ctx.paths.evidence_dir
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)

        # 1. Onboard counter readings per session (no delta_method, no interpreted_delta).
        readings = db.query(
            ctx.warehouse,
            "SELECT vehicle_test_id, boot_id, observed_at_utc, previous_counter, "
            "current_counter FROM fct_vehicle_counter_delta "
            "ORDER BY vehicle_test_id, boot_id, observed_at_utc, telemetry_event_id",
        )
        reading_rows = []
        for r in readings:
            prev = r["previous_counter"]
            adjacent = None if prev is None else r["current_counter"] - prev
            reading_rows.append({
                "vehicle_test_id": r["vehicle_test_id"], "boot_id": r["boot_id"],
                "observed_at_utc": r["observed_at_utc"],
                "passenger_counter": r["current_counter"],
                "previous_counter": prev, "adjacent_diff": adjacent,
            })
        self._write_csv(tables / "vehicle_counter_readings.csv", reading_rows,
                        ["vehicle_test_id", "boot_id", "observed_at_utc",
                         "passenger_counter", "previous_counter", "adjacent_diff"])

        # 2. Published ridership vs independent gate control.
        recon = reconciliation.ridership_vs_gate(ctx.warehouse)
        self._write_csv(
            tables / "ridership_vs_gate.csv",
            [{"service_date": r["service_date"], "route_id": r["route_id"],
              "service_hour_utc": r["service_hour_utc"], "published_boardings": r["boardings"],
              "gate_control": r["gate_control"], "residual": r["residual"],
              "ratio": round(r["ratio"], 2) if r["ratio"] else None} for r in recon],
            ["service_date", "route_id", "service_hour_utc", "published_boardings",
             "gate_control", "residual", "ratio"],
        )

        # 3. Conservation view (decisive evidence, still not naming the mechanism).
        cons = invariants.counter_session_conservation(ctx.warehouse)
        self._write_csv(
            tables / "counter_conservation.csv",
            [{"vehicle_test_id": c["vehicle_test_id"], "boot_id": c["boot_id"],
              "attributed_boardings": c["attributed_boardings"],
              "max_counter_reading": c["max_cumulative_counter"],
              "readings": c["readings"]} for c in cons],
            ["vehicle_test_id", "boot_id", "attributed_boardings",
             "max_counter_reading", "readings"],
        )

        # 4. Row counts, input manifest, quality results, lineage, timeline.
        self._write_csv(
            tables / "row_counts.csv",
            [{"table": t, "rows": db.scalar(ctx.warehouse, f"SELECT COUNT(*) FROM {t}")}
             for t in ("fct_station_gate_count", "fct_vehicle_counter_delta",
                       "mart_route_hour_ridership", "mart_route_day")],
            ["table", "rows"],
        )
        self._write_csv(
            tables / "input_manifest.csv",
            db.rows_to_dicts(db.query(ctx.control,
                "SELECT delivery_id, source_name, record_count, byte_count, "
                "completeness_token, commit_state FROM ingestion_manifest "
                "ORDER BY delivery_id")),
            ["delivery_id", "source_name", "record_count", "byte_count",
             "completeness_token", "commit_state"],
        )
        self._write_csv(
            tables / "quality_results.csv",
            db.rows_to_dicts(db.query(ctx.control,
                "SELECT check_id, scope_key, dimension, expected, actual, status "
                "FROM quality_result WHERE run_id = ? ORDER BY check_id", (ctx.run_id,))),
            ["check_id", "scope_key", "dimension", "expected", "actual", "status"],
        )
        lineage = db.rows_to_dicts(db.query(ctx.control,
            "SELECT dataset_version_id, upstream_version_id FROM dataset_lineage "
            "ORDER BY dataset_version_id"))
        write_json(ev / "lineage.json", lineage)

        worst = max(recon, key=lambda r: (r["ratio"] or 0)) if recon else None
        alert = {
            "incident_id": "MP-01",
            "title": self.metadata.reader_title,
            "reported_symptom": (
                "An operations analyst opened the route-hour ridership report and found "
                "published boardings far above the independent station gate counts for "
                "the same route and hour. The daily pipeline reported success and the raw "
                "vehicle telemetry has no duplicate deliveries."
            ),
            "time_pressure": "A public ridership summary is due at the next service review.",
            "affected_product": "mart_route_hour_ridership",
            "worst_observed": None if worst is None else {
                "service_date": worst["service_date"], "route_id": worst["route_id"],
                "service_hour_utc": worst["service_hour_utc"],
                "published_boardings": worst["boardings"], "gate_control": worst["gate_control"],
                "times_gate_control": round(worst["ratio"], 1) if worst["ratio"] else None,
            },
            "fictional_notice": (
                "MetroPulse is a fictional, synthetic city-transit system. These are lab "
                "values, not a real transit agency or a real incident."
            ),
        }
        write_json(ev / "alert.json", alert)
        write_json(ev / "timeline.json", self._sanitized_timeline(ctx))

        index = self._evidence_index(ctx, ev)
        write_json(ctx.paths.evidence_index, index)
        return StepReport("evidence", "MP-01", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys()),
                                  "worst_ratio": None if worst is None or not worst["ratio"]
                                  else round(worst["ratio"], 2)})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        recon = reconciliation.ridership_vs_gate(ctx.warehouse)
        bad = reconciliation.violations(recon, config.RIDERSHIP_RECONCILE_TOLERANCE)
        affected = sorted({(r["service_date"], r["route_id"]) for r in bad})
        with db.transaction(ctx.control):
            ctx.control.execute(
                "UPDATE dataset_version SET state = 'HELD' "
                "WHERE dataset_name = 'mart_route_hour_ridership' AND state = 'PUBLISHED'"
            )
        blast_radius = {
            "held_dataset": "mart_route_hour_ridership",
            "affected_service_date_routes": [f"{d}/{r}" for d, r in affected],
            "affected_route_hours": len(bad),
            "evidence_preserved": True,
        }
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="contained", fields=blast_radius)
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-01", "run_id": ctx.run_id, "blast_radius": blast_radius})
        return StepReport("contain", "MP-01", ctx.run_id, "contained", fields=blast_radius)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        before = list(profile.get("faulty_transforms", []))
        profile["faulty_transforms"] = [t for t in before if t != COUNTER_AS_DELTA]
        profile.setdefault("params", {})["corrected_delta_method"] = "session_delta"
        ctx.save_fault_profile(profile)
        ctx.log.emit(logical_time=ctx.clock.tick(), component="incident",
                     event_type="repaired",
                     fields={"removed_transform": COUNTER_AS_DELTA,
                             "corrected_method": "session_delta"})
        report = StepReport("repair", "MP-01", ctx.run_id, "repaired",
                            fields={"change": "counter interpreted as non-negative "
                                             "per-session delta instead of cumulative value",
                                    "faulty_transforms_after": profile["faulty_transforms"]})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover --------------------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        manifest = recovery.replay_counter_ridership(ctx)
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-01", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        conn = ctx.warehouse
        assertions: list[Assertion] = []

        # correctness: recovered ridership equals independent gate control.
        recon = reconciliation.ridership_vs_gate(conn)
        residual_max = max((abs(r["residual"]) for r in recon), default=0)
        assertions.append(Assertion(
            "MP-01-VER-CORRECT", "correctness",
            "recovered route-hour boardings equal the gate-count control",
            expected=0, actual=residual_max,
            status="PASS" if residual_max == 0 else "FAIL",
            evidence="reports/verification.json"))

        # completeness: ridership covers exactly the gate-control route-hour keys.
        ridership_keys = {(r["service_date"], r["route_id"], r["service_hour_utc"]) for r in recon}
        gate_keys = {(row["service_date"], row["route_id"], row["service_hour_utc"])
                     for row in db.query(conn,
                        "SELECT DISTINCT service_date, route_id, service_hour_utc "
                        "FROM fct_station_gate_count WHERE route_id IS NOT NULL")}
        assertions.append(Assertion(
            "MP-01-VER-COMPLETE", "completeness",
            "published route-hour count matches the gate-control route-hour count",
            expected=len(gate_keys), actual=len(ridership_keys),
            status="PASS" if ridership_keys == gate_keys else "FAIL"))

        # uniqueness at declared grain.
        dup = db.scalar(conn,
            "SELECT COUNT(*) - COUNT(DISTINCT service_date || '|' || route_id || '|' || "
            "service_hour_utc) FROM mart_route_hour_ridership")
        assertions.append(Assertion(
            "MP-01-VER-UNIQUE", "uniqueness",
            "no duplicate at mart_route_hour_ridership grain",
            expected=0, actual=dup, status="PASS" if dup == 0 else "FAIL"))

        # conservation holds after recovery.
        cons_bad = invariants.conservation_violations(invariants.counter_session_conservation(conn))
        assertions.append(Assertion(
            "MP-01-VER-CONSERVE", "history",
            "attributed boardings within cumulative bound per session",
            expected=0, actual=len(cons_bad), status="PASS" if not cons_bad else "FAIL"))

        # control consistency: a recovery publication exists.
        rec_pub = db.scalar(ctx.control,
            "SELECT COUNT(*) FROM dataset_version WHERE dataset_name = "
            "'mart_route_hour_ridership' AND state = 'PUBLISHED' AND referenced_object LIKE ?",
            (f"%recover%",))
        assertions.append(Assertion(
            "MP-01-VER-CONTROL", "control_consistency",
            "a recovered ridership publication is recorded",
            expected=">=1", actual=rec_pub, status="PASS" if rec_pub >= 1 else "FAIL"))

        # boundedness: unrelated tables unchanged since the faulty run.
        faulty_snap = self._read_faulty_snapshot(ctx)
        changed = [t for t in UNRELATED_TABLES
                   if faulty_snap.get(t) != table_logical_hash(conn, t)]
        assertions.append(Assertion(
            "MP-01-VER-BOUNDED", "boundedness",
            "no table outside the recovery scope changed",
            expected=[], actual=changed, status="PASS" if not changed else "FAIL"))

        # idempotency/determinism: an independent corrected recompute matches persisted.
        recomputed = self._independent_corrected_ridership(conn)
        persisted = {(r["service_date"], r["route_id"], r["service_hour_utc"]): r["boardings"]
                     for r in db.query(conn, "SELECT service_date, route_id, "
                        "service_hour_utc, boardings FROM mart_route_hour_ridership")}
        assertions.append(Assertion(
            "MP-01-VER-IDEMPOTENT", "idempotency",
            "independent corrected recompute equals persisted ridership",
            expected=True, actual=(recomputed == persisted),
            status="PASS" if recomputed == persisted else "FAIL"))

        # fixture integrity: landing inputs unchanged vs checked-in fixtures.
        integrity_ok, integrity_detail = integrity.fixtures_untouched(ctx)
        assertions.append(Assertion(
            "MP-01-VER-FIXTURE", "fixture_integrity",
            "run landing inputs match checked-in fixture checksums",
            expected=True, actual=integrity_ok,
            status="PASS" if integrity_ok else "FAIL",
            evidence=str(integrity_detail.get("status"))))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-01", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions)})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _write_quality_result(self, ctx, check_id, dimension, expected, actual, status):
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO quality_result "
                "(run_id, check_id, scope_key, dimension, expected, actual, status, evidence_ptr) "
                "VALUES (?, ?, 'all', ?, ?, ?, ?, 'evidence/quality_results.csv')",
                (ctx.run_id, check_id, dimension, expected, actual, status))

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({c: row.get(c) for c in columns})

    def _evidence_index(self, ctx: RunContext, ev: Path) -> dict[str, Any]:
        artifacts = {}
        for path in sorted(ev.rglob("*")):
            if path.is_file() and path.name != "evidence.json":
                rel = str(path.relative_to(ev))
                artifacts[rel] = {
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "bytes": path.stat().st_size,
                }
        return {
            "incident_id": "MP-01", "run_id": ctx.run_id,
            "description": "Evidence pack. Symptom and facts only; no root-cause label.",
            "artifacts": artifacts,
        }

    @staticmethod
    def _sanitized_timeline(ctx: RunContext) -> list[dict[str, Any]]:
        """A bounded, filtered log excerpt: operational events only.

        Injection/repair/recovery internals (which name the mechanism) are
        excluded so the evidence pack cannot reveal the root cause early.
        """
        allowed = {"receive", "stage", "model", "publish"}
        out = []
        for e in ctx.log.read_all():
            if e.get("component") not in allowed:
                continue
            fields = {k: v for k, v in (e.get("fields") or {}).items() if k != "fault"}
            out.append({"logical_time": e.get("logical_time"),
                        "component": e.get("component"),
                        "event_type": e.get("event_type"), "fields": fields})
        return out

    @staticmethod
    def _control_plane_signal(ctx: RunContext, data_wrong: bool) -> dict[str, Any]:
        """A green control plane (committed inputs + PUBLISHED, lineage-linked
        output) alongside wrong data is itself a control-plane failure."""
        manifests = db.query(
            ctx.control, "SELECT commit_state, completeness_token FROM ingestion_manifest")
        manifests_committed = bool(manifests) and all(
            m["commit_state"] == "COMMITTED" and m["completeness_token"] == "COMPLETE"
            for m in manifests)
        ridership_published = db.scalar(
            ctx.control, "SELECT COUNT(*) FROM dataset_version WHERE "
            "dataset_name = 'mart_route_hour_ridership' AND state = 'PUBLISHED'") >= 1
        lineage_ok = db.scalar(
            ctx.control,
            "SELECT COUNT(*) FROM dataset_lineage dl JOIN dataset_version dv "
            "ON dl.dataset_version_id = dv.dataset_version_id "
            "WHERE dv.dataset_name = 'mart_route_hour_ridership'") >= 1
        green_control = manifests_committed and ridership_published and lineage_ok
        return {
            "manifests_committed": manifests_committed,
            "ridership_published": ridership_published,
            "lineage_linked": lineage_ok,
            "green_control": green_control,
            "gap": bool(green_control and data_wrong),
        }

    def _read_faulty_snapshot(self, ctx: RunContext) -> dict[str, str]:
        import json
        path = ctx.paths.checkpoints_dir / FAULTY_SNAPSHOT
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def _independent_corrected_ridership(self, conn) -> dict[tuple, int]:
        """Recompute corrected route-hour boardings straight from staging."""
        rows = db.query(conn,
            "SELECT vehicle_test_id, boot_id, passenger_counter, observed_at_utc "
            "FROM stg_vehicle_telemetry "
            "ORDER BY vehicle_test_id, boot_id, observed_at_utc, telemetry_event_id")
        prev: dict[tuple, int] = {}
        out: dict[tuple, int] = {}
        for r in rows:
            vehicle, boot = r["vehicle_test_id"], r["boot_id"]
            route = config.VEHICLE_ROUTE.get(vehicle)
            if route is None:
                continue
            dt = datetime.fromisoformat(r["observed_at_utc"].replace("Z", "+00:00")).astimezone(timezone.utc)
            key = (dt.strftime("%Y-%m-%d"), route, dt.hour)
            p = prev.get((vehicle, boot))
            delta = r["passenger_counter"] if p is None else r["passenger_counter"] - p
            if delta < 0:
                delta = 0
            out[key] = out.get(key, 0) + delta
            prev[(vehicle, boot)] = r["passenger_counter"]
        return out

INCIDENT = MP01Incident()
