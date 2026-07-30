"""Batch D (MP-16 .. MP-20) reference lifecycle and evidence-leak tests.

Each incident gets its own isolated temp workspace; nothing touches the shared
``.lab`` directory and no test runs the bare ``test-all`` sweep. The leak tests
scan the reader-facing evidence pack for the mechanism vocabulary each incident
must hide until the reader has done the investigation.
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from metropulse_lab import commands
from metropulse_lab.reference import reference_lifecycle

# Mechanism / root-cause vocabulary that must NOT appear in an incident's
# reader-facing evidence pack (taxonomy "leak words OUT" per incident).
LEAK_PATTERNS = {
    "MP-16": r"collect|driver|OOM|materiali|maxResultSize|unbounded",
    "MP-17": r"N\+1|per-row|fan-?out|round-?trip|batch lookup",
    "MP-18": r"cardinality|expansion|mapping|map length|flood|task count",
    "MP-19": r"\bstate\b|eviction|TTL|watermark|unbounded|checkpoint size|soak",
    "MP-20": r"statistics|ANALYZE|planner|stale|estimate|sqlite_stat1|optimizer",
}


class BatchDReferenceLifecycleTests(unittest.TestCase):
    def _run(self, incident_id: str) -> None:
        with tempfile.TemporaryDirectory() as ws:
            result = reference_lifecycle(incident_id, Path(ws))
            failed = [c["check"] for c in result["checks"] if not c["passed"]]
            self.assertTrue(result["passed"], f"{incident_id} failed checks: {failed}")

    def test_mp16_reference_lifecycle(self):
        self._run("MP-16")

    def test_mp17_reference_lifecycle(self):
        self._run("MP-17")

    def test_mp18_reference_lifecycle(self):
        self._run("MP-18")

    def test_mp19_reference_lifecycle(self):
        self._run("MP-19")

    def test_mp20_reference_lifecycle(self):
        self._run("MP-20")


class BatchDEvidenceLeakTests(unittest.TestCase):
    def _assert_no_leak(self, incident_id: str) -> None:
        pattern = re.compile(LEAK_PATTERNS[incident_id], re.IGNORECASE)
        run_slug = "run-" + incident_id.replace("-", "").lower() + "-0001"
        with tempfile.TemporaryDirectory() as ws:
            commands.start(commands._Args(incident=incident_id, workspace=ws))
            evidence = Path(ws) / "runs" / run_slug / "evidence"
            self.assertTrue(evidence.exists(), f"{incident_id} produced no evidence pack")
            found = False
            for path in evidence.rglob("*"):
                if path.is_file():
                    found = True
                    text = path.read_text(encoding="utf-8", errors="replace")
                    match = pattern.search(text)
                    self.assertIsNone(
                        match,
                        f"{incident_id} root-cause leak in {path.name}: "
                        f"{match.group(0) if match else ''}")
            self.assertTrue(found, f"{incident_id} evidence pack was empty")

    def test_mp16_no_leak(self):
        self._assert_no_leak("MP-16")

    def test_mp17_no_leak(self):
        self._assert_no_leak("MP-17")

    def test_mp18_no_leak(self):
        self._assert_no_leak("MP-18")

    def test_mp19_no_leak(self):
        self._assert_no_leak("MP-19")

    def test_mp20_no_leak(self):
        self._assert_no_leak("MP-20")


class BatchDMetadataTests(unittest.TestCase):
    def test_expected_detection_ids_are_two_plane(self):
        for incident_id in ("MP-16", "MP-17", "MP-18", "MP-19", "MP-20"):
            meta = commands.registry.load(incident_id).metadata
            self.assertEqual(meta.family, "D. Performance and cost")
            self.assertEqual(meta.recovery_mode, "REPLAY")
            self.assertEqual(len(meta.expected_detection_ids), 2,
                             f"{incident_id} should expose a data and a control detection id")


if __name__ == "__main__":
    unittest.main()
