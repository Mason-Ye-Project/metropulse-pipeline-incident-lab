"""Batch-E (MP-21 .. MP-25, "Recovery accidents") contract tests.

For every incident this asserts:

- the reference lifecycle passes all of its checks (clean baseline verifies, the
  fault is detected, verify fails before repair, evidence is read-only, recovery
  is bounded and idempotent, fixtures are untouched, reset removes only the run);
- the evidence pack never contains the incident's frozen mechanism vocabulary or
  a generic root-cause marker;
- detection spans a data/business plane and a control plane;
- evidence bytes are deterministic across two independent workspaces.

MP-22 is a ``NO_REPLAY`` incident: its recovery keeps the missing history
unrecoverable and verifies the corrected guardrail, and the reference lifecycle
still passes end to end.
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from metropulse_lab import commands
from metropulse_lab.reference import reference_lifecycle
from metropulse_lab.reports import read_json

# Frozen mechanism vocabulary that must never appear under ``evidence/``.
LEAK_TOKENS = {
    "MP-21": ("pointer", "commit order", "durable", "race",
              "metadata plane", "data plane", "manifest before"),
    "MP-22": ("retention", "lifecycle", "replay window", "90-day", "30-day",
              "aged out", "aged-out", "expiration", "expire"),
    "MP-23": ("rollback", "roll back", "snapshot", "compensation", "compensating",
              "selective", "ancestor", "cherry-pick", "cherry pick", "collateral"),
    "MP-24": ("orphan", "cleanup", "clean up", "retention", "active writer",
              "in-progress", "in progress", "dry-run", "dry run", "lease"),
    "MP-25": ("restore point", "cursor", "consistent point", "gap",
              "offset mismatch", "reconcile", "reconciled", "reconciliation"),
}

EXPECTED_RECOVERY_MODE = {
    "MP-21": "ROLLBACK", "MP-22": "NO_REPLAY", "MP-23": "REPLAY",
    "MP-24": "REPLAY", "MP-25": "REPLAY",
}

# Uppercase redaction markers (matched case-sensitively so innocent words such as
# "resolution" or "answered" do not trip the check).
MARKER_RE = re.compile(r"(?<!\w)(ROOT_CAUSE|SOLUTION|ANSWER)(?!\w)")


def _leak_re(tokens) -> re.Pattern:
    parts = [r"(?<!\w)" + re.escape(t) + r"(?!\w)" for t in tokens]
    return re.compile("|".join(parts), re.IGNORECASE)


def _run_id(incident_id: str) -> str:
    return f"run-{incident_id.replace('-', '').lower()}-0001"


class BatchELifecycleTests(unittest.TestCase):
    def _assert_lifecycle(self, incident_id: str):
        with tempfile.TemporaryDirectory() as ws:
            result = reference_lifecycle(incident_id, Path(ws))
            failed = [c["check"] for c in result["checks"] if not c["passed"]]
            self.assertTrue(result["passed"], f"{incident_id} failed checks: {failed}")

    def test_mp21_reference_lifecycle(self):
        self._assert_lifecycle("MP-21")

    def test_mp22_reference_lifecycle(self):
        self._assert_lifecycle("MP-22")

    def test_mp23_reference_lifecycle(self):
        self._assert_lifecycle("MP-23")

    def test_mp24_reference_lifecycle(self):
        self._assert_lifecycle("MP-24")

    def test_mp25_reference_lifecycle(self):
        self._assert_lifecycle("MP-25")


class BatchEEvidenceLeakTests(unittest.TestCase):
    def _assert_no_leak(self, incident_id: str):
        leak_re = _leak_re(LEAK_TOKENS[incident_id])
        with tempfile.TemporaryDirectory() as ws:
            commands.start(commands._Args(incident=incident_id, workspace=ws))
            evidence = Path(ws) / "runs" / _run_id(incident_id) / "evidence"
            self.assertTrue(evidence.exists(), f"{incident_id} produced no evidence")
            files = [p for p in evidence.rglob("*") if p.is_file()]
            self.assertTrue(files, f"{incident_id} evidence pack is empty")
            for path in files:
                text = path.read_text(encoding="utf-8", errors="replace")
                m = leak_re.search(text)
                self.assertIsNone(
                    m, f"{incident_id} mechanism leak in {path.name}: {m.group(0) if m else ''}")
                mk = MARKER_RE.search(text)
                self.assertIsNone(
                    mk, f"{incident_id} root-cause marker in {path.name}: {mk.group(0) if mk else ''}")

    def test_mp21_evidence_no_leak(self):
        self._assert_no_leak("MP-21")

    def test_mp22_evidence_no_leak(self):
        self._assert_no_leak("MP-22")

    def test_mp23_evidence_no_leak(self):
        self._assert_no_leak("MP-23")

    def test_mp24_evidence_no_leak(self):
        self._assert_no_leak("MP-24")

    def test_mp25_evidence_no_leak(self):
        self._assert_no_leak("MP-25")


class BatchEDetectionPlaneTests(unittest.TestCase):
    """Architecture 12.3: at least one data/business signal AND one control signal."""

    def _assert_two_planes(self, incident_id: str):
        with tempfile.TemporaryDirectory() as ws:
            commands.start(commands._Args(incident=incident_id, workspace=ws))
            det = read_json(Path(ws) / "runs" / _run_id(incident_id) / "reports" / "detection.json")
            failed = [a for a in det["assertions"] if a["status"] == "FAIL"]
            dims = {a["dimension"] for a in failed}
            self.assertIn("control_consistency", dims,
                          f"{incident_id} has no failing control-plane signal")
            data_dims = dims - {"control_consistency"}
            self.assertTrue(data_dims,
                            f"{incident_id} has no failing data/business-plane signal")
            self.assertTrue(det["fields"]["incident_detected"])

    def test_mp21_two_planes(self):
        self._assert_two_planes("MP-21")

    def test_mp22_two_planes(self):
        self._assert_two_planes("MP-22")

    def test_mp23_two_planes(self):
        self._assert_two_planes("MP-23")

    def test_mp24_two_planes(self):
        self._assert_two_planes("MP-24")

    def test_mp25_two_planes(self):
        self._assert_two_planes("MP-25")


class BatchEMetadataTests(unittest.TestCase):
    def test_recovery_modes_and_family(self):
        from metropulse_lab.incidents import registry
        for incident_id, mode in EXPECTED_RECOVERY_MODE.items():
            meta = registry.load(incident_id).metadata
            self.assertEqual(meta.recovery_mode, mode, f"{incident_id} recovery mode")
            self.assertEqual(meta.family, "E. Recovery accidents", f"{incident_id} family")
            self.assertEqual(len(meta.expected_detection_ids), 2, f"{incident_id} detection ids")
            self.assertEqual(meta.affected_tables, (), f"{incident_id} affected_tables")

    def test_mp22_is_no_replay(self):
        from metropulse_lab.incidents import registry
        self.assertEqual(registry.load("MP-22").metadata.recovery_mode, "NO_REPLAY")

    def test_expected_detection_ids_are_emitted(self):
        from metropulse_lab.incidents import registry
        for incident_id in EXPECTED_RECOVERY_MODE:
            expected = set(registry.load(incident_id).metadata.expected_detection_ids)
            with tempfile.TemporaryDirectory() as ws:
                commands.start(commands._Args(incident=incident_id, workspace=ws))
                csv_path = (Path(ws) / "runs" / _run_id(incident_id) /
                            "evidence" / "tables" / "quality_results.csv")
                text = csv_path.read_text(encoding="utf-8")
                for check_id in expected:
                    self.assertIn(check_id, text,
                                  f"{incident_id} did not emit quality result {check_id}")
                    self.assertIn(f"{check_id},all,", text)  # emitted as a FAIL row below
                self.assertIn(",FAIL", text, f"{incident_id} emitted no failing quality result")


class BatchEDeterminismTests(unittest.TestCase):
    """Two independent workspaces produce byte-identical evidence."""

    def _evidence_bytes(self, incident_id: str, ws: str) -> dict[str, bytes]:
        commands.start(commands._Args(incident=incident_id, workspace=ws))
        evidence = Path(ws) / "runs" / _run_id(incident_id) / "evidence"
        out = {}
        for p in sorted(evidence.rglob("*")):
            if p.is_file() and p.name != "evidence.json":
                out[str(p.relative_to(evidence))] = p.read_bytes()
        return out

    def _assert_deterministic(self, incident_id: str):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            self.assertEqual(self._evidence_bytes(incident_id, a),
                             self._evidence_bytes(incident_id, b),
                             f"{incident_id} evidence is not byte-identical across workspaces")

    def test_mp21_deterministic(self):
        self._assert_deterministic("MP-21")

    def test_mp22_deterministic(self):
        self._assert_deterministic("MP-22")

    def test_mp23_deterministic(self):
        self._assert_deterministic("MP-23")

    def test_mp24_deterministic(self):
        self._assert_deterministic("MP-24")

    def test_mp25_deterministic(self):
        self._assert_deterministic("MP-25")


if __name__ == "__main__":
    unittest.main()
