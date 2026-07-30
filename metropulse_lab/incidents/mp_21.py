"""MP-21 — A dataset marked published cannot fully open; one object is missing.

Frozen mechanism (incident_taxonomy.md MP-21): the writer made a new dataset
version visible before one of its referenced objects had finished being written
and verified. A reader that opened the newly-visible version resolved one
reference to an object that is not yet readable.

The reader-facing title and the evidence pack describe only the symptom: a
version that reports published, and a referenced object that cannot be opened.
The frozen mechanism vocabulary is never written under ``evidence/``.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchE_common as be
from .contract import IncidentMetadata

DATASET = "station_feed_snapshot"

# The current visible version after a healthy data-first publish.
M_V1 = "MAN-station_feed-v1"
M_V2 = "MAN-station_feed-v2"
# The version whose visibility was exposed before an object finished.
M_V3 = "MAN-station_feed-v3"

LEAK_TOKENS = ("pointer", "commit order", "durable", "race",
               "metadata plane", "data plane", "manifest before")


class MP21Incident:
    metadata = IncidentMetadata(
        incident_id="MP-21",
        family="E. Recovery accidents",
        reader_title="A dataset marked published cannot fully open — one referenced object is missing",
        primary_invariant_id="MP-21-CLOSURE",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="ROLLBACK",
        fault_operations=("expose_version_before_object_ready",),
        expected_detection_ids=("MP-21-RESOLVE-001", "MP-21-VISIBILITY-001"),
        transfer_mappings=("S3 conditional writes; Iceberg audit branch",),
        nearest_old_scenario="Closest to the object-store completeness scenario (old S10).",
        old_scenario_difference=(
            "Not a lifecycle deletion or a reader permission error: the version was "
            "made visible before one referenced object finished being written and "
            "verified, so a reader entered between the two commits."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        # Healthy history: v1 fully written and made visible, then v2 published
        # data-first (every object written and verified, then the version made
        # visible). The current visible version is v2 and every reference opens.
        be.put_object(ctx, f"OBJ-{M_V2}-0", b"station-feed v2 part 0")
        be.put_object(ctx, f"OBJ-{M_V2}-1", b"station-feed v2 part 1")
        be.put_object(ctx, f"OBJ-{M_V1}-0", b"station-feed v1 part 0")
        be.put_object(ctx, f"OBJ-{M_V1}-1", b"station-feed v1 part 1")
        self._put_manifest(ctx, M_V1, "v1", "PUBLISHED", [f"OBJ-{M_V1}-0", f"OBJ-{M_V1}-1"])
        self._put_manifest(ctx, M_V2, "v2", "PUBLISHED", [f"OBJ-{M_V2}-0", f"OBJ-{M_V2}-1"])
        self._set_current(ctx, M_V2, visible_seq=6)
        self._reset_publish_sequence(ctx, healthy=True)

    def baseline_ok(self, ctx: RunContext) -> bool:
        cur = self._current_manifest(ctx)
        return cur is not None and self._unresolved_refs(ctx, cur) == []

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        fixtures_before = self._fixture_ok(ctx)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"expose_version_before_object_ready"})
        profile.setdefault("params", {})
        ctx.save_fault_profile(profile)

        self._materialize_faulty(ctx)

        unresolved = self._unresolved_refs(ctx, self._current_manifest(ctx))
        fixtures_after = self._fixture_ok(ctx)
        assertions = [
            Assertion("MP-21-INJECT-PRE", "precondition",
                      "current version opens fully before injection",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-21-INJECT-POST", "postcondition",
                      "current version now references an object that cannot be opened",
                      expected=">0", actual=len(unresolved),
                      status="PASS" if unresolved else "FAIL"),
            be.fixture_integrity_assertion(ctx, "MP-21-INJECT-FIXTURE"),
        ]
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-21", ctx.run_id, status,
                            fields={"unresolved_references": len(unresolved),
                                    "fixtures_unchanged": fixtures_before and fixtures_after},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._materialize_faulty(ctx)
        cur = self._current_manifest(ctx)
        return StepReport("run", "MP-21", ctx.run_id, "executed",
                          fields={"current_version": cur, "state": "PUBLISHED"})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        cur = self._current_manifest(ctx)
        unresolved = self._unresolved_refs(ctx, cur)
        state = self._manifest_state(ctx, cur)
        closure_incomplete = bool(unresolved)
        published_but_incomplete = (state == "PUBLISHED") and closure_incomplete
        detected = closure_incomplete or published_but_incomplete

        be.write_quality_result(
            ctx, "MP-21-RESOLVE-001", "object_readability",
            expected="a reader opening the published version resolves every referenced object",
            actual=f"{len(unresolved)} referenced object(s) could not be opened",
            status="FAIL" if closure_incomplete else "PASS")
        be.write_quality_result(
            ctx, "MP-21-VISIBILITY-001", "control_consistency",
            expected="a version reported published has every referenced object readable",
            actual=("the published version references an object that is not readable"
                    if published_but_incomplete else "published version fully readable"),
            status="FAIL" if published_but_incomplete else "PASS")

        assertions = [
            Assertion("MP-21-DET-RESOLVE", "object_readability",
                      "current published version resolves every referenced object",
                      expected=0, actual=len(unresolved),
                      status="FAIL" if closure_incomplete else "PASS",
                      evidence="reports/detection.json"),
            Assertion("MP-21-DET-VISIBILITY", "control_consistency",
                      "a published+visible version implies a complete reference closure",
                      expected=False, actual=published_but_incomplete,
                      status="FAIL" if published_but_incomplete else "PASS",
                      evidence="reports/detection.json"),
        ]
        fields = {
            "incident_detected": detected,
            "current_version": cur,
            "current_state": state,
            "unresolved_references": unresolved,
            "last_complete_version": self._last_complete_manifest(ctx, exclude=(cur,)),
            "publish_order": self._publish_sequence(ctx),
        }
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-21", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-21", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- evidence (read-only) -------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        cur = self._current_manifest(ctx)

        refs = []
        for object_id in self._manifest_refs(ctx, cur):
            present = be.object_row(ctx, object_id)
            readable = be.object_durable(ctx, object_id)
            refs.append({"version": self._manifest_label(ctx, cur),
                         "object_ref": object_id,
                         "present": 1 if present else 0,
                         "readable": 1 if readable else 0})
        be.write_csv(tables / "manifest_references.csv", refs,
                     ["version", "object_ref", "present", "readable"])

        versions = db.rows_to_dicts(db.query(
            ctx.control,
            "SELECT dataset_name, version_label, manifest_id, state, ref_count "
            "FROM mp21_manifest ORDER BY version_label"))
        for v in versions:
            v["is_current"] = 1 if v["manifest_id"] == cur else 0
        be.write_csv(tables / "published_versions.csv", versions,
                     ["dataset_name", "version_label", "manifest_id", "state",
                      "ref_count", "is_current"])

        be.write_quality_results_csv(ctx)

        alert = be.base_alert(
            "MP-21",
            self.metadata.reader_title,
            reported_symptom=(
                "A reader opened the latest published station-feed snapshot. The "
                "catalog reports the version as published, but opening it fails: one "
                "of the objects it lists cannot be read. The prior published version "
                "still opens completely."),
            time_pressure="A downstream consumer refresh is blocked until the version opens.",
            affected_product=DATASET,
            extra={"current_version": self._manifest_label(ctx, cur),
                   "objects_that_cannot_be_opened":
                       [r["object_ref"] for r in refs if r["readable"] == 0]})
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json",
                   be.sanitized_timeline(ctx, LEAK_TOKENS))
        index = be.write_evidence_index(ctx, "MP-21")
        return StepReport("evidence", "MP-21", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        cur = self._current_manifest(ctx)
        unreadable = [r for r in self._manifest_refs(ctx, cur)
                      if not be.object_durable(ctx, r)]
        last_complete = self._last_complete_manifest(ctx, exclude=(cur,))
        with db.transaction(ctx.control):
            ctx.control.execute(
                "UPDATE mp21_manifest SET state = 'HELD' WHERE manifest_id = ? AND state = 'PUBLISHED'",
                (cur,))
        blast = {"dataset": DATASET, "held_version": cur,
                 "objects_that_cannot_be_opened": unreadable,
                 "last_complete_version": last_complete, "evidence_preserved": True}
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-21", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-21", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [
            t for t in profile.get("faulty_transforms", [])
            if t != "expose_version_before_object_ready"]
        profile.setdefault("params", {})["mp21_guardrail"] = "visibility_requires_complete_closure"
        ctx.save_fault_profile(profile)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "UPDATE mp21_manifest SET state = 'WITHDRAWN' WHERE manifest_id = ?", (M_V3,))
        report = StepReport("repair", "MP-21", ctx.run_id, "repaired",
                            fields={"withdrawn_version": M_V3,
                                    "guardrail": "a version becomes visible only after "
                                                 "every referenced object is written and verified"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (ROLLBACK) ---------------------------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        target = self._last_complete_manifest(ctx, exclude=(M_V3,))
        # Conditional swap: only make a version visible if its closure is complete.
        if target is not None and self._unresolved_refs(ctx, target) == []:
            self._set_current(ctx, target, visible_seq=10)
        be.record_outbox(ctx, f"MP-21-{ctx.run_id}-rolled-back", "dataset_version_rolled_back",
                         {"dataset": DATASET, "restored_version": target})
        manifest = {
            "mode": "ROLLBACK",
            "scope": {"dataset": DATASET, "from_version": M_V3, "to_version": target},
            "write_mode": "metadata_only",
            "duplicate_protection": "conditional swap; visible version must have a complete closure",
            "concurrency_fence": "single visible version per dataset",
            "downstream_descendants": [DATASET],
            "validation": "reference closure and checksum on the restored version",
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-21", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        cur = self._current_manifest(ctx)
        unresolved = self._unresolved_refs(ctx, cur)
        state = self._manifest_state(ctx, cur)
        guardrail = ctx.load_fault_profile().get("params", {}).get("mp21_guardrail")
        incomplete_visible = self._incomplete_but_visible(ctx)

        assertions: list[Assertion] = []
        assertions.append(Assertion(
            "MP-21-VER-CORRECT", "correctness",
            "the current visible version resolves every referenced object",
            expected=0, actual=len(unresolved),
            status="PASS" if not unresolved else "FAIL",
            evidence="reports/verification.json"))
        ref_count = self._manifest_ref_count(ctx, cur)
        readable_count = sum(1 for r in self._manifest_refs(ctx, cur)
                             if be.object_durable(ctx, r))
        assertions.append(Assertion(
            "MP-21-VER-COMPLETE", "completeness",
            "every object the current version lists is readable",
            expected=ref_count, actual=readable_count,
            status="PASS" if ref_count == readable_count else "FAIL"))
        assertions.append(Assertion(
            "MP-21-VER-HISTORY", "history",
            "the current visible version is a complete published version",
            expected="PUBLISHED+complete",
            actual=f"{state}+{'complete' if not unresolved else 'incomplete'}",
            status="PASS" if state == "PUBLISHED" and not unresolved else "FAIL"))
        assertions.append(Assertion(
            "MP-21-VER-CONTROL", "control_consistency",
            "guardrail present and no incomplete version is visible",
            expected="guardrail;0-incomplete-visible",
            actual=f"{guardrail};{incomplete_visible}",
            status="PASS" if guardrail and incomplete_visible == 0 else "FAIL"))
        changed = be.shared_tables_changed(ctx)
        assertions.append(Assertion(
            "MP-21-VER-BOUNDED", "boundedness",
            "no shared warehouse/mart table changed during recovery",
            expected=[], actual=changed, status="PASS" if not changed else "FAIL"))
        recomputed = self._last_complete_manifest(ctx, exclude=(M_V3,))
        assertions.append(Assertion(
            "MP-21-VER-IDEMPOTENT", "idempotency",
            "an independent recompute of the newest complete version equals the current one",
            expected=recomputed, actual=cur,
            status="PASS" if recomputed == cur else "FAIL"))
        assertions.append(be.fixture_integrity_assertion(ctx, "MP-21-VER-FIXTURE"))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-21", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions), "current_version": cur})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.control):
            ctx.control.executescript(
                """
                CREATE TABLE IF NOT EXISTS mp21_manifest (
                    manifest_id   TEXT NOT NULL PRIMARY KEY,
                    dataset_name  TEXT NOT NULL,
                    version_label TEXT NOT NULL,
                    state         TEXT NOT NULL,
                    ref_count     INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp21_manifest_ref (
                    manifest_id TEXT NOT NULL,
                    ref_index   INTEGER NOT NULL,
                    object_id   TEXT NOT NULL,
                    PRIMARY KEY (manifest_id, ref_index)
                );
                CREATE TABLE IF NOT EXISTS mp21_current (
                    dataset_name        TEXT NOT NULL PRIMARY KEY,
                    current_manifest_id TEXT NOT NULL,
                    visible_seq         INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp21_publish_event (
                    seq       INTEGER NOT NULL PRIMARY KEY,
                    action    TEXT NOT NULL,
                    object_ref TEXT,
                    manifest_id TEXT
                );
                """)

    def _put_manifest(self, ctx, manifest_id, label, state, object_ids) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp21_manifest "
                "(manifest_id, dataset_name, version_label, state, ref_count) VALUES (?, ?, ?, ?, ?)",
                (manifest_id, DATASET, label, state, len(object_ids)))
            ctx.control.execute("DELETE FROM mp21_manifest_ref WHERE manifest_id = ?", (manifest_id,))
            for idx, oid in enumerate(object_ids):
                ctx.control.execute(
                    "INSERT INTO mp21_manifest_ref (manifest_id, ref_index, object_id) VALUES (?, ?, ?)",
                    (manifest_id, idx, oid))

    def _set_current(self, ctx, manifest_id, visible_seq) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp21_current "
                "(dataset_name, current_manifest_id, visible_seq) VALUES (?, ?, ?)",
                (DATASET, manifest_id, visible_seq))

    def _reset_publish_sequence(self, ctx, healthy: bool) -> None:
        if healthy:
            events = [
                (1, "write_object", f"OBJ-{M_V2}-0", M_V2),
                (2, "verify_object", f"OBJ-{M_V2}-0", M_V2),
                (3, "write_object", f"OBJ-{M_V2}-1", M_V2),
                (4, "verify_object", f"OBJ-{M_V2}-1", M_V2),
                (5, "closure_complete", None, M_V2),
                (6, "make_version_visible", None, M_V2),
            ]
        else:
            events = [
                (1, "write_object", f"OBJ-{M_V3}-0", M_V3),
                (2, "verify_object", f"OBJ-{M_V3}-0", M_V3),
                (3, "make_version_visible", None, M_V3),
                (4, "write_object", f"OBJ-{M_V3}-1", M_V3),
            ]
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp21_publish_event")
            for seq, action, obj, man in events:
                ctx.control.execute(
                    "INSERT INTO mp21_publish_event (seq, action, object_ref, manifest_id) "
                    "VALUES (?, ?, ?, ?)", (seq, action, obj, man))

    def _materialize_faulty(self, ctx: RunContext) -> None:
        # Deterministic replay of the faulty publish: object 0 finished, the
        # version was made visible, and object 1 was never finished.
        be.put_object(ctx, f"OBJ-{M_V3}-0", b"station-feed v3 part 0")
        be.register_object(ctx, f"OBJ-{M_V3}-1", None, be.OBJ_PENDING)
        self._put_manifest(ctx, M_V3, "v3", "PUBLISHED",
                           [f"OBJ-{M_V3}-0", f"OBJ-{M_V3}-1"])
        self._set_current(ctx, M_V3, visible_seq=3)
        self._reset_publish_sequence(ctx, healthy=False)

    def _current_manifest(self, ctx) -> str | None:
        return db.scalar(ctx.control,
                         "SELECT current_manifest_id FROM mp21_current WHERE dataset_name = ?",
                         (DATASET,))

    def _manifest_refs(self, ctx, manifest_id) -> list[str]:
        if manifest_id is None:
            return []
        return [r["object_id"] for r in db.query(
            ctx.control,
            "SELECT object_id FROM mp21_manifest_ref WHERE manifest_id = ? ORDER BY ref_index",
            (manifest_id,))]

    def _manifest_ref_count(self, ctx, manifest_id) -> int:
        return db.scalar(ctx.control,
                         "SELECT ref_count FROM mp21_manifest WHERE manifest_id = ?",
                         (manifest_id,)) or 0

    def _manifest_state(self, ctx, manifest_id) -> str | None:
        return db.scalar(ctx.control,
                         "SELECT state FROM mp21_manifest WHERE manifest_id = ?", (manifest_id,))

    def _manifest_label(self, ctx, manifest_id) -> str | None:
        return db.scalar(ctx.control,
                         "SELECT version_label FROM mp21_manifest WHERE manifest_id = ?",
                         (manifest_id,))

    def _unresolved_refs(self, ctx, manifest_id) -> list[str]:
        return [oid for oid in self._manifest_refs(ctx, manifest_id)
                if not be.object_durable(ctx, oid)]

    def _last_complete_manifest(self, ctx, exclude: tuple = ()) -> str | None:
        rows = db.query(
            ctx.control,
            "SELECT manifest_id, version_label FROM mp21_manifest "
            "WHERE state IN ('PUBLISHED', 'HELD') ORDER BY version_label DESC")
        for r in rows:
            mid = r["manifest_id"]
            if mid in exclude:
                continue
            if self._unresolved_refs(ctx, mid) == []:
                return mid
        return None

    def _incomplete_but_visible(self, ctx) -> int:
        cur = self._current_manifest(ctx)
        return 1 if (cur is not None and self._unresolved_refs(ctx, cur)) else 0

    def _publish_sequence(self, ctx) -> list[dict[str, Any]]:
        return db.rows_to_dicts(db.query(
            ctx.control,
            "SELECT seq, action, object_ref, manifest_id FROM mp21_publish_event ORDER BY seq"))

    def _fixture_ok(self, ctx) -> bool:
        ok, _ = be.integrity.fixtures_untouched(ctx)
        return ok


INCIDENT = MP21Incident()
