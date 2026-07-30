"""MP-01 reference lifecycle and evidence-safety tests."""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from metropulse_lab import commands
from metropulse_lab.reference import reference_lifecycle

LEAK_PATTERN = re.compile(
    r"cumulative|counter_as_delta|delta_method|session_delta|per-event|"
    r"ROOT_CAUSE|SOLUTION|misread|treated as",
    re.IGNORECASE,
)


class MP01LifecycleTests(unittest.TestCase):
    def test_reference_lifecycle_all_checks_pass(self):
        with tempfile.TemporaryDirectory() as ws:
            result = reference_lifecycle("MP-01", Path(ws))
            failed = [c["check"] for c in result["checks"] if not c["passed"]]
            self.assertTrue(result["passed"], f"failed checks: {failed}")

    def test_evidence_pack_has_no_root_cause_leak(self):
        with tempfile.TemporaryDirectory() as ws:
            args = commands._Args(incident="MP-01", workspace=ws)
            commands.start(args)
            run_id = "run-mp01-0001"
            evidence = Path(ws) / "runs" / run_id / "evidence"
            self.assertTrue(evidence.exists())
            for path in evidence.rglob("*"):
                if path.is_file():
                    text = path.read_text(encoding="utf-8", errors="replace")
                    match = LEAK_PATTERN.search(text)
                    self.assertIsNone(
                        match, f"root-cause leak in {path.name}: {match.group(0) if match else ''}")

    def test_detect_reports_expected_ids(self):
        incident = commands.registry.load("MP-01")
        self.assertEqual(incident.metadata.expected_detection_ids,
                         ("MP-01-RECON-001", "MP-01-CONSERVATION-001", "MP-01-CONTROL-001"))

    def test_detection_spans_data_and_control_planes(self):
        # Architecture 12.3: at least one data signal AND one control signal.
        from metropulse_lab.reports import read_json
        with tempfile.TemporaryDirectory() as ws:
            commands.start(commands._Args(incident="MP-01", workspace=ws))
            det = read_json(Path(ws) / "runs" / "run-mp01-0001" / "reports" / "detection.json")
            dims = {a["dimension"] for a in det["assertions"] if a["status"] == "FAIL"}
            self.assertIn("reconciliation", dims)  # data plane
            self.assertIn("control_consistency", dims)  # control plane
            self.assertTrue(det["fields"]["control_plane"]["green_control"])
            self.assertTrue(det["fields"]["control_plane"]["gap"])

    def test_injection_records_fixture_checksums(self):
        # Architecture 10: injection report proves fixtures were not altered.
        from metropulse_lab.reports import read_json
        with tempfile.TemporaryDirectory() as ws:
            commands.start(commands._Args(incident="MP-01", workspace=ws))
            inj = read_json(Path(ws) / "runs" / "run-mp01-0001" / "reports" / "injection.json")
            self.assertTrue(inj["fields"]["fixture_checksums_unchanged"])
            self.assertGreater(inj["fields"]["landing_files_checked"], 0)
            ids = {a["assertion_id"] for a in inj["assertions"]}
            self.assertIn("MP-01-INJECT-FIXTURE", ids)


if __name__ == "__main__":
    unittest.main()
