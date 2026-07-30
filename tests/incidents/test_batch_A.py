"""Batch A (MP-02 .. MP-05) reference-lifecycle, evidence-leak, and determinism tests.

Every test uses an isolated ``tempfile.TemporaryDirectory`` workspace. The shared
``.lab`` workspace is never touched and ``test-all`` is never invoked here.
"""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
import unittest
from pathlib import Path

from metropulse_lab import commands, paths
from metropulse_lab.incidents import mp_05
from metropulse_lab.reference import reference_lifecycle

BATCH_A = ("MP-02", "MP-03", "MP-04", "MP-05")

# The mechanism words that must never appear in an incident's evidence pack
# (incident_taxonomy.md section 3, per-incident "Leak words OUT of evidence").
LEAK_WORDS = {
    "MP-02": ["NOT IN", "NULL", "three-valued", "unknown", "anti-join", "NOT EXISTS"],
    "MP-03": ["timezone", "UTC", "service day", "03:00", "DST", "daylight", "offset", "cutoff"],
    "MP-04": ["overflow", "32-bit", "int32", "wrap", "width", "signed"],
    "MP-05": ["epoch", "milliseconds", "seconds", "unit", "1970", "magnitude"],
}


def _run_id(incident_id: str) -> str:
    return f"run-{incident_id.replace('-', '').lower()}-0001"


def _evidence_files(ws: str, incident_id: str) -> list[Path]:
    ev = Path(ws) / "runs" / _run_id(incident_id) / "evidence"
    return [p for p in sorted(ev.rglob("*")) if p.is_file()]


class BatchAReferenceLifecycle(unittest.TestCase):
    def test_reference_lifecycle_passes(self):
        for incident_id in BATCH_A:
            with self.subTest(incident=incident_id):
                with tempfile.TemporaryDirectory() as ws:
                    result = reference_lifecycle(incident_id, Path(ws))
                    failed = [c["check"] for c in result["checks"] if not c["passed"]]
                    details = [c for c in result["checks"] if not c["passed"]]
                    self.assertTrue(result["passed"],
                                    f"{incident_id} failed checks: {failed}\n"
                                    + json.dumps(details, indent=2))


class BatchAEvidenceLeak(unittest.TestCase):
    def test_evidence_pack_has_no_mechanism_leak(self):
        for incident_id in BATCH_A:
            with self.subTest(incident=incident_id):
                with tempfile.TemporaryDirectory() as ws:
                    commands.start(commands._Args(incident=incident_id, workspace=ws))
                    files = _evidence_files(ws, incident_id)
                    self.assertTrue(files, f"{incident_id}: no evidence files produced")
                    for path in files:
                        text = path.read_text(encoding="utf-8", errors="replace")
                        for word in LEAK_WORDS[incident_id]:
                            match = re.search(re.escape(word), text, re.IGNORECASE)
                            self.assertIsNone(
                                match,
                                f"{incident_id}: leak word {word!r} in {path.name}: "
                                f"...{text[max(0, (match.start() if match else 0) - 20):(match.start() if match else 0) + 20]}...")

    def test_detection_ids_match_written_quality_results(self):
        for incident_id in BATCH_A:
            with self.subTest(incident=incident_id):
                incident = commands.registry.load(incident_id)
                self.assertTrue(incident.metadata.expected_detection_ids)
                with tempfile.TemporaryDirectory() as ws:
                    commands.start(commands._Args(incident=incident_id, workspace=ws))
                    det = commands.read_json(
                        Path(ws) / "runs" / _run_id(incident_id) / "reports" / "detection.json")
                    dims = {a["dimension"] for a in det["assertions"] if a["status"] == "FAIL"}
                    # Architecture 12.3: at least one data signal AND one control signal.
                    self.assertIn("control_consistency", dims,
                                  f"{incident_id}: no control-plane failure signal")
                    self.assertTrue(
                        dims - {"control_consistency"},
                        f"{incident_id}: no data-plane failure signal")


class BatchADeterminism(unittest.TestCase):
    def test_evidence_is_byte_identical_across_workspaces(self):
        # Two independent workspaces must yield byte-identical evidence CSV/JSON.
        for incident_id in BATCH_A:
            with self.subTest(incident=incident_id):
                digests = []
                for _ in range(2):
                    with tempfile.TemporaryDirectory() as ws:
                        commands.start(commands._Args(incident=incident_id, workspace=ws))
                        pack = {}
                        for path in _evidence_files(ws, incident_id):
                            pack[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
                        digests.append(pack)
                self.assertEqual(digests[0], digests[1],
                                 f"{incident_id}: evidence differs across workspaces")


class MP05ParserContract(unittest.TestCase):
    def test_parser_cases_match_expected_window(self):
        fixture = mp_05._load_fixture()
        contract = fixture["source_contract"]
        for case in fixture["parser_cases"]:
            observed = mp_05._resolve(case["epoch_raw"], case["unit"])
            in_window = mp_05._in_window(observed, contract)
            self.assertEqual(in_window, case["expect_in_window"],
                             f"parser case {case['label']}: got in_window={in_window}")


if __name__ == "__main__":
    unittest.main()
