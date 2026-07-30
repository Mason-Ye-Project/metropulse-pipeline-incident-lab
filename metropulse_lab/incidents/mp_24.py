"""MP-24 — A partition a maintenance job reported clean now reads short.

Frozen mechanism (incident_taxonomy.md MP-24): a maintenance removal used a
safety window shorter than the longest write still in flight and ignored the
marker that a file was still being written. It removed a file a writer had just
produced (and a previous-version file kept for undo), so the partition reads
short and the files needed to undo the change are also gone.

The interleaving is replayed from a declared event queue — no threads, no sleeps,
no real deletion (files move to recoverable trash). The reader-facing title and
the evidence describe only the symptom; the frozen mechanism vocabulary never
appears under ``evidence/``.
"""

from __future__ import annotations

from typing import Any

from .. import db
from ..context import RunContext
from ..reports import Assertion, StepReport, write_json
from . import _batchE_common as be
from .contract import IncidentMetadata

DATASET = "station_partition"
PARTITION = "STN_007-2026-04-15"
WRITER = "WRITER-1"

F_BASE = "OBJ-part-base"       # already-committed file that is part of the partition
F_PREV = "OBJ-part-previous"   # previous-version file kept so the change can be undone
F_ORPH = "OBJ-part-leftover"   # a genuine leftover file, safe to remove
F_NEW = "OBJ-part-new"         # the file the writer is producing right now

FILE_ROWS = {F_BASE: 3, F_PREV: 3, F_ORPH: 2, F_NEW: 4}

LEAK_TOKENS = ("orphan", "cleanup", "clean up", "retention", "active writer",
               "in-progress", "in progress", "dry-run", "dry run", "lease")


class MP24Incident:
    metadata = IncidentMetadata(
        incident_id="MP-24",
        family="E. Recovery accidents",
        reader_title="A partition a maintenance job reported clean now reads short, and the files needed to undo it are gone",
        primary_invariant_id="MP-24-PARTITION",
        core_evidence_kind="DETERMINISTIC_SIMULATOR",
        recovery_mode="REPLAY",
        fault_operations=("remove_file_still_being_written",),
        expected_detection_ids=("MP-24-PARTITION-001", "MP-24-DELETION-001"),
        transfer_mappings=("Iceberg orphan-file maintenance",),
        nearest_old_scenario="Adjacent to the compaction/cleanup scenario (old S8).",
        old_scenario_difference=(
            "The earlier compaction scenario never removes a file that is still "
            "being written because its safety window is shorter than the write; "
            "here the maintenance deletes an in-flight writer's file and the "
            "undo file for the same partition."
        ),
        affected_tables=(),
    )

    # -- baseline -------------------------------------------------------------
    def build_baseline(self, ctx: RunContext) -> None:
        self._ensure_tables(ctx)
        self._replay(ctx, safe=True)

    def baseline_ok(self, ctx: RunContext) -> bool:
        expected, present = self._partition_rows(ctx)
        deleted_active = self._deleted_active_or_referenced(ctx)
        return expected == present and not deleted_active

    # -- inject ---------------------------------------------------------------
    def inject(self, ctx: RunContext) -> StepReport:
        pre_ok = self.baseline_ok(ctx)
        fixtures_before = self._fixture_ok(ctx)

        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = sorted(
            set(profile.get("faulty_transforms", [])) | {"remove_file_still_being_written"})
        profile.setdefault("params", {})
        ctx.save_fault_profile(profile)

        self._replay(ctx, safe=False)

        expected, present = self._partition_rows(ctx)
        assertions = [
            Assertion("MP-24-INJECT-PRE", "precondition",
                      "the partition reads complete before injection",
                      expected=True, actual=pre_ok, status="PASS" if pre_ok else "FAIL"),
            Assertion("MP-24-INJECT-POST", "postcondition",
                      "the partition now reads short",
                      expected="<expected", actual=f"{present}<{expected}",
                      status="PASS" if present < expected else "FAIL"),
            be.fixture_integrity_assertion(ctx, "MP-24-INJECT-FIXTURE"),
        ]
        status = "injected" if all(a.status == "PASS" for a in assertions) else "inject_failed"
        report = StepReport("inject", "MP-24", ctx.run_id, status,
                            fields={"expected_rows": expected, "present_rows": present,
                                    "fixtures_unchanged": fixtures_before and self._fixture_ok(ctx)},
                            assertions=assertions)
        write_json(ctx.paths.report_path("injection.json"), report.to_dict())
        return report

    def run_faulty(self, ctx: RunContext) -> StepReport:
        self._replay(ctx, safe=False)
        expected, present = self._partition_rows(ctx)
        return StepReport("run", "MP-24", ctx.run_id, "executed",
                          fields={"expected_rows": expected, "present_rows": present})

    # -- detect ---------------------------------------------------------------
    def detect(self, ctx: RunContext) -> StepReport:
        expected, present = self._partition_rows(ctx)
        short = expected - present
        deleted_active = self._deleted_active_or_referenced(ctx)

        be.write_quality_result(
            ctx, "MP-24-PARTITION-001", "completeness",
            expected="the partition contains every row its listed files provide",
            actual=f"the partition reads short by {short} row(s); a listed file is missing",
            status="FAIL" if short > 0 else "PASS")
        be.write_quality_result(
            ctx, "MP-24-DELETION-001", "control_consistency",
            expected="a maintenance removal never deletes a file that is still being "
                     "written or belongs to the current partition",
            actual=("a maintenance removal deleted a file that was still being written"
                    if deleted_active else "maintenance removed only safe files"),
            status="FAIL" if deleted_active else "PASS")

        detected = short > 0 or deleted_active
        assertions = [
            Assertion("MP-24-DET-PARTITION", "completeness",
                      "the partition reads all of its rows",
                      expected=expected, actual=present,
                      status="FAIL" if short > 0 else "PASS", evidence="reports/detection.json"),
            Assertion("MP-24-DET-DELETION", "control_consistency",
                      "no removed file was still being written or part of the partition",
                      expected=False, actual=deleted_active,
                      status="FAIL" if deleted_active else "PASS",
                      evidence="reports/detection.json"),
        ]
        fields = {"incident_detected": detected, "partition": PARTITION,
                  "expected_rows": expected, "present_rows": present,
                  "removed_files": self._removed_files(ctx)}
        write_json(ctx.paths.report_path("detection.json"),
                   {"incident_id": "MP-24", "run_id": ctx.run_id, "fields": fields,
                    "assertions": [a.to_dict() for a in assertions]})
        return StepReport("detect", "MP-24", ctx.run_id,
                          "incident_detected" if detected else "clean",
                          fields=fields, assertions=assertions)

    # -- evidence (read-only) -------------------------------------------------
    def collect_evidence(self, ctx: RunContext) -> StepReport:
        tables = ctx.paths.evidence_tables_dir
        tables.mkdir(parents=True, exist_ok=True)
        expected, present = self._partition_rows(ctx)
        be.write_csv(tables / "partition_completeness.csv",
                     [{"partition": PARTITION, "expected_rows": expected,
                       "present_rows": present, "missing_rows": expected - present}],
                     ["partition", "expected_rows", "present_rows", "missing_rows"])

        removed = []
        for r in db.query(ctx.control,
                          "SELECT object_id, removed_at_seq, was_being_written, "
                          "part_of_partition FROM mp24_deletion_log ORDER BY removed_at_seq"):
            removed.append(dict(r))
        be.write_csv(tables / "removed_files.csv", removed,
                     ["object_id", "removed_at_seq", "was_being_written", "part_of_partition"])

        be.write_quality_results_csv(ctx)

        alert = be.base_alert(
            "MP-24",
            self.metadata.reader_title,
            reported_symptom=(
                "A maintenance job reported success. Right after, a just-written "
                "station partition reads short: several rows are missing, and the "
                "previous-version file that would let the change be undone is also "
                "gone."),
            time_pressure="A downstream report needs the full partition today.",
            affected_product=DATASET,
            extra={"partition": PARTITION, "rows_missing": expected - present})
        write_json(ctx.paths.evidence_dir / "alert.json", alert)
        write_json(ctx.paths.evidence_dir / "timeline.json",
                   be.sanitized_timeline(ctx, LEAK_TOKENS))
        index = be.write_evidence_index(ctx, "MP-24")
        return StepReport("evidence", "MP-24", ctx.run_id, "collected",
                          fields={"artifacts": sorted(index["artifacts"].keys())})

    # -- contain --------------------------------------------------------------
    def contain(self, ctx: RunContext) -> StepReport:
        expected, present = self._partition_rows(ctx)
        profile = ctx.load_fault_profile()
        profile.setdefault("params", {})["mp24_paused"] = True
        ctx.save_fault_profile(profile)
        blast = {"dataset": DATASET, "partition": PARTITION,
                 "rows_missing": expected - present,
                 "removed_files": [r["object_id"] for r in self._removed_files(ctx)
                                   if r["was_being_written"] or r["part_of_partition"]],
                 "evidence_preserved": True}
        write_json(ctx.paths.report_path("containment.json"),
                   {"incident_id": "MP-24", "run_id": ctx.run_id, "blast_radius": blast})
        return StepReport("contain", "MP-24", ctx.run_id, "contained", fields=blast)

    # -- repair ---------------------------------------------------------------
    def repair(self, ctx: RunContext) -> StepReport:
        profile = ctx.load_fault_profile()
        profile["faulty_transforms"] = [
            t for t in profile.get("faulty_transforms", [])
            if t != "remove_file_still_being_written"]
        params = profile.setdefault("params", {})
        params["mp24_guardrail"] = "protect_files_being_written_and_undo_files"
        ctx.save_fault_profile(profile)
        # Install the safer maintenance policy for future runs.
        self._set_policy(ctx, honor_active=1, honor_undo_files=1, safety_margin=1)
        report = StepReport("repair", "MP-24", ctx.run_id, "repaired",
                            fields={"guardrail": "a maintenance removal skips files still "
                                                 "being written and previous-version files, "
                                                 "and its window is larger than the longest write"})
        write_json(ctx.paths.report_path("repair.json"), report.to_dict())
        return report

    # -- recover (REPLAY restore + rebuild) -----------------------------------
    def recover(self, ctx: RunContext) -> StepReport:
        restored = []
        for oid in (F_NEW, F_PREV):
            if be.restore_object(ctx, oid):
                restored.append(oid)
            elif be.object_durable(ctx, oid):
                restored.append(oid)
        # Rebuild the affected partition to reference the restored files.
        self._set_snapshot_refs(ctx, [F_BASE, F_NEW])
        be.record_outbox(ctx, f"MP-24-{ctx.run_id}-restored", "partition_files_restored",
                         {"partition": PARTITION, "restored": [F_NEW, F_PREV]})
        expected, present = self._partition_rows(ctx)
        manifest = {
            "mode": "REPLAY",
            "scope": {"dataset": DATASET, "partition": PARTITION,
                      "restored_objects": [F_NEW, F_PREV]},
            "write_mode": "restore_then_bounded_rebuild",
            "duplicate_protection": "restore by object id from recoverable trash; "
                                    "partition rebuilt to a fixed file set",
            "concurrency_fence": "single partition rebuild",
            "downstream_descendants": [DATASET],
            "validation": "every listed file present and checksum-valid; partition full; "
                          "the previous-version (undo) file is readable again",
            "expected_rows": expected, "present_rows": present,
        }
        write_json(ctx.paths.report_path("recovery.json"), manifest)
        return StepReport("recover", "MP-24", ctx.run_id, "recovered", fields=manifest)

    # -- verify ---------------------------------------------------------------
    def verify(self, ctx: RunContext) -> StepReport:
        expected, present = self._partition_rows(ctx)
        refs = self._snapshot_refs(ctx)
        all_present = all(be.object_durable(ctx, o) for o in refs)
        undo_ok = be.object_durable(ctx, F_PREV)
        guardrail = ctx.load_fault_profile().get("params", {}).get("mp24_guardrail")
        deleted_active = self._deleted_active_or_referenced_still_missing(ctx)

        assertions: list[Assertion] = []
        assertions.append(Assertion(
            "MP-24-VER-CORRECT", "correctness",
            "the partition reads all of its rows",
            expected=expected, actual=present, status="PASS" if expected == present else "FAIL",
            evidence="reports/verification.json"))
        assertions.append(Assertion(
            "MP-24-VER-COMPLETE", "completeness",
            "every file the partition lists is present and checksum-valid",
            expected=True, actual=all_present, status="PASS" if all_present else "FAIL"))
        assertions.append(Assertion(
            "MP-24-VER-HISTORY", "history",
            "the previous-version (undo) file is readable again",
            expected=True, actual=undo_ok, status="PASS" if undo_ok else "FAIL"))
        assertions.append(Assertion(
            "MP-24-VER-CONTROL", "control_consistency",
            "no file that was still being written remains deleted and the guardrail is in place",
            expected="0-still-missing;guardrail", actual=f"{deleted_active};{guardrail}",
            status="PASS" if not deleted_active and guardrail else "FAIL"))
        changed = be.shared_tables_changed(ctx)
        assertions.append(Assertion(
            "MP-24-VER-BOUNDED", "boundedness",
            "no shared warehouse/mart table changed during recovery",
            expected=[], actual=changed, status="PASS" if not changed else "FAIL"))
        orphan_removed = not be.object_durable(ctx, F_ORPH)
        assertions.append(Assertion(
            "MP-24-VER-SCOPE", "boundedness",
            "the genuinely-leftover file was not restored (recovery stayed in scope)",
            expected=True, actual=orphan_removed, status="PASS" if orphan_removed else "FAIL"))
        assertions.append(be.fixture_integrity_assertion(ctx, "MP-24-VER-FIXTURE"))

        status = "PASS" if all(a.status == "PASS" for a in assertions) else "FAIL"
        report = StepReport("verify", "MP-24", ctx.run_id, status, assertions=assertions,
                            fields={"passed": sum(a.status == "PASS" for a in assertions),
                                    "total": len(assertions),
                                    "expected_rows": expected, "present_rows": present})
        write_json(ctx.paths.report_path("verification.json"), report.to_dict())
        return report

    # -- helpers --------------------------------------------------------------
    def _ensure_tables(self, ctx: RunContext) -> None:
        with db.transaction(ctx.control):
            ctx.control.executescript(
                """
                CREATE TABLE IF NOT EXISTS mp24_file_meta (
                    object_id TEXT NOT NULL PRIMARY KEY,
                    partition TEXT NOT NULL,
                    rows      INTEGER NOT NULL,
                    role      TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp24_snapshot_ref (
                    partition TEXT NOT NULL,
                    object_id TEXT NOT NULL,
                    PRIMARY KEY (partition, object_id)
                );
                CREATE TABLE IF NOT EXISTS mp24_deletion_log (
                    object_id        TEXT NOT NULL PRIMARY KEY,
                    removed_at_seq   INTEGER NOT NULL,
                    was_being_written INTEGER NOT NULL,
                    part_of_partition INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp24_policy (
                    policy_id       TEXT NOT NULL PRIMARY KEY,
                    honor_active    INTEGER NOT NULL,
                    honor_undo_files INTEGER NOT NULL,
                    safety_margin   INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mp24_event (
                    seq    INTEGER NOT NULL PRIMARY KEY,
                    phase  TEXT NOT NULL,
                    object_id TEXT,
                    note   TEXT
                );
                """)

    def _set_policy(self, ctx, honor_active, honor_undo_files, safety_margin) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp24_policy "
                "(policy_id, honor_active, honor_undo_files, safety_margin) "
                "VALUES ('maintenance', ?, ?, ?)", (honor_active, honor_undo_files, safety_margin))

    def _reset_state(self, ctx: RunContext) -> None:
        # Rebuild the partition's file inventory to the pre-write baseline and
        # clear the event/deletion ledgers so the queue replays from scratch.
        with db.transaction(ctx.control):
            for oid, rows, role in ((F_BASE, FILE_ROWS[F_BASE], "base"),
                                    (F_PREV, FILE_ROWS[F_PREV], "previous"),
                                    (F_ORPH, FILE_ROWS[F_ORPH], "orphan")):
                ctx.control.execute(
                    "INSERT OR REPLACE INTO mp24_file_meta (object_id, partition, rows, role) "
                    "VALUES (?, ?, ?, ?)", (oid, PARTITION, rows, role))
            ctx.control.execute(
                "INSERT OR REPLACE INTO mp24_file_meta (object_id, partition, rows, role) "
                "VALUES (?, ?, ?, 'new')", (F_NEW, PARTITION, FILE_ROWS[F_NEW]))
            ctx.control.execute("DELETE FROM mp24_deletion_log")
            ctx.control.execute("DELETE FROM mp24_event")
            ctx.control.execute("DELETE FROM writer_lease WHERE target_name = ?", (PARTITION,))
        # Committed files present; F_NEW absent until the writer starts.
        be.put_object(ctx, F_BASE, b"base rows: 3")
        be.put_object(ctx, F_PREV, b"previous rows: 3")
        be.put_object(ctx, F_ORPH, b"leftover rows: 2")
        row = be.object_row(ctx, F_NEW)
        if row is not None:
            # Remove any leftover F_NEW so the write starts clean.
            with db.transaction(ctx.control):
                ctx.control.execute("DELETE FROM object_inventory WHERE object_id = ?", (F_NEW,))
            abs_path = ctx.paths.run_dir / be.object_relpath(F_NEW)
            if abs_path.exists():
                abs_path.unlink()
        self._set_snapshot_refs(ctx, [F_BASE])

    def _replay(self, ctx: RunContext, safe: bool) -> None:
        """Deterministic writer/maintenance event queue (no threads, no sleeps)."""
        self._reset_state(ctx)
        self._set_policy(ctx, honor_active=1 if safe else 0,
                         honor_undo_files=1 if safe else 0, safety_margin=1 if safe else 0)

        # seq 1: the writer starts producing F_NEW (bytes present, not yet committed).
        be.put_object(ctx, F_NEW, b"new rows: 4 (bytes written, not yet committed)",
                      active_writer=WRITER, lifecycle_state=be.OBJ_WRITING)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO writer_lease (target_name, current_writer, fence) "
                "VALUES (?, ?, 1)", (PARTITION, WRITER))
            ctx.control.execute("INSERT INTO mp24_event (seq, phase, object_id, note) "
                                "VALUES (1, 'write_start', ?, 'writer producing file')", (F_NEW,))

        # seq 2: maintenance scans and removes what it considers safe to delete.
        refs_at_scan = set(self._snapshot_refs(ctx))  # {F_BASE}
        seq = 3
        for oid in (F_ORPH, F_PREV, F_NEW):
            row = be.object_row(ctx, oid)
            role = self._role(ctx, oid)
            being_written = 1 if (row and row["active_writer"]) else 0
            referenced = 1 if oid in refs_at_scan else 0
            if referenced:
                continue  # never remove a currently-referenced file
            if safe:
                if being_written:      # protected: still being written
                    continue
                if role == "previous":  # protected: kept for undo
                    continue
            # Unsafe policy removes any not-currently-referenced file.
            be.trash_object(ctx, oid, set_writer_null=True)
            with db.transaction(ctx.control):
                ctx.control.execute(
                    "INSERT OR REPLACE INTO mp24_deletion_log "
                    "(object_id, removed_at_seq, was_being_written, part_of_partition) "
                    "VALUES (?, ?, ?, ?)",
                    (oid, seq, being_written, 1 if role in ("base", "new", "previous") else 0))
                ctx.control.execute("INSERT INTO mp24_event (seq, phase, object_id, note) "
                                    "VALUES (?, 'maintenance_remove', ?, 'removed by maintenance')",
                                    (seq, oid))
            seq += 1
        with db.transaction(ctx.control):
            ctx.control.execute("INSERT INTO mp24_event (seq, phase, object_id, note) "
                                "VALUES (?, 'maintenance_report', NULL, 'reported success')", (seq,))
        seq += 1

        # seq N: the writer commits. If its file survived, the partition is full;
        # otherwise the commit lands short. The writer records the file either way.
        be.commit_object(ctx, F_NEW)
        self._set_snapshot_refs(ctx, [F_BASE, F_NEW])
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM writer_lease WHERE target_name = ?", (PARTITION,))
            ctx.control.execute("INSERT INTO mp24_event (seq, phase, object_id, note) "
                                "VALUES (?, 'write_commit', ?, 'writer committed')", (seq, F_NEW))

    def _role(self, ctx, object_id) -> str | None:
        return db.scalar(ctx.control,
                         "SELECT role FROM mp24_file_meta WHERE object_id = ?", (object_id,))

    def _snapshot_refs(self, ctx) -> list[str]:
        return [r["object_id"] for r in db.query(
            ctx.control,
            "SELECT object_id FROM mp24_snapshot_ref WHERE partition = ? ORDER BY object_id",
            (PARTITION,))]

    def _set_snapshot_refs(self, ctx, object_ids) -> None:
        with db.transaction(ctx.control):
            ctx.control.execute("DELETE FROM mp24_snapshot_ref WHERE partition = ?", (PARTITION,))
            for oid in object_ids:
                ctx.control.execute(
                    "INSERT INTO mp24_snapshot_ref (partition, object_id) VALUES (?, ?)",
                    (PARTITION, oid))

    def _partition_rows(self, ctx) -> tuple[int, int]:
        expected = 0
        present = 0
        for oid in self._snapshot_refs(ctx):
            rows = FILE_ROWS.get(oid, 0)
            expected += rows
            if be.object_durable(ctx, oid):
                present += rows
        return expected, present

    def _removed_files(self, ctx) -> list[dict[str, Any]]:
        return db.rows_to_dicts(db.query(
            ctx.control,
            "SELECT object_id, removed_at_seq, was_being_written, part_of_partition "
            "FROM mp24_deletion_log ORDER BY removed_at_seq"))

    def _deleted_active_or_referenced(self, ctx) -> bool:
        return db.scalar(ctx.control,
                         "SELECT COUNT(*) FROM mp24_deletion_log WHERE was_being_written = 1") > 0

    def _deleted_active_or_referenced_still_missing(self, ctx) -> int:
        """Files that were removed while being written and are still not durable."""
        rows = db.query(ctx.control,
                        "SELECT object_id FROM mp24_deletion_log WHERE was_being_written = 1")
        return sum(1 for r in rows if not be.object_durable(ctx, r["object_id"]))

    def _fixture_ok(self, ctx) -> bool:
        ok, _ = be.integrity.fixtures_untouched(ctx)
        return ok


INCIDENT = MP24Incident()
