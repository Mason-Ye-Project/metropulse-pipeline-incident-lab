"""Reference-lifecycle and evidence-safety tests for batch C (MP-11 .. MP-15).

Each incident must pass every reference-lifecycle check (clean baseline, fault
detected, verify fails before repair, evidence read-only, bounded idempotent
recovery, fixtures untouched, scoped reset) and must keep its frozen mechanism
vocabulary out of everything under ``evidence/``.

Every test uses an isolated temporary workspace. No test touches the shared
``.lab`` workspace, runs ``test-all``, or removes ``.lab``.
"""

from __future__ import annotations

import csv
import re
import tempfile
import unittest
from pathlib import Path

from metropulse_lab import commands
from metropulse_lab.incidents import registry
from metropulse_lab.reference import reference_lifecycle
from metropulse_lab.reports import read_json

BATCH_C = ("MP-11", "MP-12", "MP-13", "MP-14", "MP-15")
RUN_ID = {inc: f"run-{inc.replace('-', '').lower()}-0001" for inc in BATCH_C}

# The frozen mechanism vocabulary each incident must never expose to the reader.
# Matched on word boundaries so a coincidental substring in a byte count or a
# generic control-plane column name is not a false positive.
LEAK_WORDS = {
    "MP-11": ("retry", "retries", "429", "amplification", "backoff", "jitter",
              "budget", "storm", "throttle"),
    "MP-12": ("credential", "token", "403", "expire", "expired", "refresh",
              "authorization"),
    "MP-13": ("dependency", "lockfile", "lock", "version", "resolution", "pin",
              "transitive", "hash"),
    "MP-14": ("fence", "fencing", "lease", "writer", "writers", "heartbeat",
              "race", "orphan", "concurrent"),
    "MP-15": ("pool", "leak", "connection", "checked-out", "checkout", "exhaust",
              "queuepool"),
}


def _leak_pattern(words):
    return re.compile(r"\b(" + "|".join(re.escape(w) for w in words) + r")\b", re.IGNORECASE)


class BatchCReferenceLifecycleTests(unittest.TestCase):
    def test_reference_lifecycle_all_checks_pass(self):
        for inc in BATCH_C:
            with self.subTest(incident=inc), tempfile.TemporaryDirectory() as ws:
                result = reference_lifecycle(inc, Path(ws))
                failed = [c["check"] for c in result["checks"] if not c["passed"]]
                self.assertTrue(result["passed"], f"{inc} failed checks: {failed}")


class BatchCEvidenceLeakTests(unittest.TestCase):
    def test_evidence_pack_hides_the_mechanism(self):
        for inc in BATCH_C:
            with self.subTest(incident=inc), tempfile.TemporaryDirectory() as ws:
                commands.start(commands._Args(incident=inc, workspace=ws))
                evidence = Path(ws) / "runs" / RUN_ID[inc] / "evidence"
                self.assertTrue(evidence.exists())
                pattern = _leak_pattern(LEAK_WORDS[inc])
                for path in evidence.rglob("*"):
                    if path.is_file():
                        text = path.read_text(encoding="utf-8", errors="replace")
                        match = pattern.search(text)
                        self.assertIsNone(
                            match,
                            f"{inc}: mechanism word {match.group(0) if match else ''!r} "
                            f"leaked into evidence/{path.name}")

    def test_alert_is_symptom_only_with_fictional_notice(self):
        for inc in BATCH_C:
            with self.subTest(incident=inc), tempfile.TemporaryDirectory() as ws:
                commands.start(commands._Args(incident=inc, workspace=ws))
                alert = read_json(Path(ws) / "runs" / RUN_ID[inc] / "evidence" / "alert.json")
                self.assertEqual(alert["incident_id"], inc)
                self.assertIn("MetroPulse is a fictional", alert["fictional_notice"])


class BatchCDetectionTests(unittest.TestCase):
    def test_detection_spans_data_and_control_planes(self):
        # Architecture 12.3: at least one business/data signal AND one control signal.
        for inc in BATCH_C:
            with self.subTest(incident=inc), tempfile.TemporaryDirectory() as ws:
                commands.start(commands._Args(incident=inc, workspace=ws))
                det = read_json(Path(ws) / "runs" / RUN_ID[inc] / "reports" / "detection.json")
                self.assertTrue(det["fields"]["incident_detected"])
                fail_dims = {a["dimension"] for a in det["assertions"] if a["status"] == "FAIL"}
                self.assertIn("control_consistency", fail_dims, f"{inc} missing control signal")
                self.assertTrue(fail_dims - {"control_consistency"},
                                f"{inc} missing a data-plane signal")

    def test_expected_detection_ids_are_written(self):
        for inc in BATCH_C:
            with self.subTest(incident=inc), tempfile.TemporaryDirectory() as ws:
                commands.start(commands._Args(incident=inc, workspace=ws))
                qr = Path(ws) / "runs" / RUN_ID[inc] / "evidence" / "tables" / "quality_results.csv"
                with qr.open(encoding="utf-8") as fh:
                    written = {row["check_id"] for row in csv.DictReader(fh)}
                for check_id in registry.load(inc).metadata.expected_detection_ids:
                    self.assertIn(check_id, written, f"{inc}: {check_id} not in quality results")


class BatchCDeterminismTests(unittest.TestCase):
    def test_evidence_is_byte_identical_across_runs(self):
        import hashlib

        def digest(ws, inc):
            ev = Path(ws) / "runs" / RUN_ID[inc] / "evidence"
            return {str(p.relative_to(ev)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in sorted(ev.rglob("*")) if p.is_file()}

        for inc in BATCH_C:
            with self.subTest(incident=inc):
                runs = []
                for _ in range(2):
                    with tempfile.TemporaryDirectory() as ws:
                        commands.start(commands._Args(incident=inc, workspace=ws))
                        runs.append(digest(ws, inc))
                self.assertEqual(runs[0], runs[1], f"{inc} evidence not deterministic")


if __name__ == "__main__":
    unittest.main()
