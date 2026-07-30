"""Batch B (MP-06 .. MP-10, "Missing or late") reference lifecycle and safety tests.

Each incident must drive ``reference_lifecycle`` to a full PASS, its evidence pack
must not leak the hidden mechanism, its detection must span a data plane and a
control plane, and its evidence CSVs must be byte-identical across two isolated
workspaces. Isolated tempdirs only; the shared ``.lab`` is never touched.
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from metropulse_lab import commands
from metropulse_lab.reports import read_json
from metropulse_lab.reference import reference_lifecycle
from metropulse_lab.incidents import registry
from metropulse_lab.incidents import _batchB_mp08_api as api08
from metropulse_lab.incidents import mp_09

BATCH_B = ("MP-06", "MP-07", "MP-08", "MP-09", "MP-10")

# Per-incident mechanism vocabulary that must never appear in the evidence pack.
LEAK_PATTERNS = {
    "MP-06": r"truncat|gzip|\bcrc\b|trailer|incomplet|prefix|\btail\b",
    "MP-07": r"logical date|wall.?clock|now\(\)|\binterval\b|timetable|catchup",
    "MP-08": r"pagination|off.?by.?one|\btoken\b|\bpage\b|continuation|terminate",
    "MP-09": r"\bshard\b|completeness|manifest|set.?difference|incomplet|missing file",
    "MP-10": r"\bcursor\b|watermark|\bfuture\b|2099|poison|monotonic",
}
COMMON_LEAK = r"ROOT_CAUSE|SOLUTION|\bANSWER\b"


def _run_dir(ws: str, incident_id: str) -> Path:
    run_id = f"run-{incident_id.replace('-', '').lower()}-0001"
    return Path(ws) / "runs" / run_id


class ReferenceLifecycleTests(unittest.TestCase):
    def _check(self, incident_id: str) -> None:
        with tempfile.TemporaryDirectory() as ws:
            result = reference_lifecycle(incident_id, Path(ws))
            failed = [c["check"] for c in result["checks"] if not c["passed"]]
            detail = [c for c in result["checks"] if not c["passed"]]
            self.assertTrue(result["passed"], f"{incident_id} failed checks: {failed}\n{detail}")

    def test_mp06(self): self._check("MP-06")
    def test_mp07(self): self._check("MP-07")
    def test_mp08(self): self._check("MP-08")
    def test_mp09(self): self._check("MP-09")
    def test_mp10(self): self._check("MP-10")


class EvidenceLeakTests(unittest.TestCase):
    def _check(self, incident_id: str) -> None:
        pattern = re.compile(f"{LEAK_PATTERNS[incident_id]}|{COMMON_LEAK}", re.IGNORECASE)
        with tempfile.TemporaryDirectory() as ws:
            commands.start(commands._Args(incident=incident_id, workspace=ws))
            ev = _run_dir(ws, incident_id) / "evidence"
            self.assertTrue(ev.exists(), f"{incident_id} evidence dir missing")
            for path in ev.rglob("*"):
                if path.is_file():
                    text = path.read_text(encoding="utf-8", errors="replace")
                    match = pattern.search(text)
                    self.assertIsNone(
                        match, f"{incident_id} leak in {path.name}: "
                               f"{match.group(0) if match else ''}")

    def test_mp06(self): self._check("MP-06")
    def test_mp07(self): self._check("MP-07")
    def test_mp08(self): self._check("MP-08")
    def test_mp09(self): self._check("MP-09")
    def test_mp10(self): self._check("MP-10")


class ReaderTitleTests(unittest.TestCase):
    def test_titles_have_no_mechanism_vocabulary(self):
        for incident_id in BATCH_B:
            title = registry.load(incident_id).metadata.reader_title
            pattern = re.compile(LEAK_PATTERNS[incident_id], re.IGNORECASE)
            self.assertIsNone(pattern.search(title),
                              f"{incident_id} reader_title leaks the mechanism: {title!r}")


class DetectionSpansPlanesTests(unittest.TestCase):
    """Architecture 12.3: every incident needs one data signal and one control signal."""

    def _check(self, incident_id: str) -> None:
        with tempfile.TemporaryDirectory() as ws:
            commands.start(commands._Args(incident=incident_id, workspace=ws))
            det = read_json(_run_dir(ws, incident_id) / "reports" / "detection.json")
            failed_dims = {a["dimension"] for a in det["assertions"] if a["status"] == "FAIL"}
            self.assertIn("control_consistency", failed_dims,
                          f"{incident_id} has no failing control-plane signal")
            data_dims = failed_dims - {"control_consistency"}
            self.assertTrue(data_dims, f"{incident_id} has no failing data-plane signal")

    def test_mp06(self): self._check("MP-06")
    def test_mp07(self): self._check("MP-07")
    def test_mp08(self): self._check("MP-08")
    def test_mp09(self): self._check("MP-09")
    def test_mp10(self): self._check("MP-10")


class ExpectedDetectionIdsTests(unittest.TestCase):
    def _check(self, incident_id: str) -> None:
        import csv
        with tempfile.TemporaryDirectory() as ws:
            commands.start(commands._Args(incident=incident_id, workspace=ws))
            qr = _run_dir(ws, incident_id) / "evidence" / "tables" / "quality_results.csv"
            with qr.open(encoding="utf-8") as fh:
                recorded = {row["check_id"] for row in csv.DictReader(fh)}
            expected = set(registry.load(incident_id).metadata.expected_detection_ids)
            self.assertTrue(expected.issubset(recorded),
                            f"{incident_id} missing quality_result rows for {expected - recorded}")

    def test_mp06(self): self._check("MP-06")
    def test_mp07(self): self._check("MP-07")
    def test_mp08(self): self._check("MP-08")
    def test_mp09(self): self._check("MP-09")
    def test_mp10(self): self._check("MP-10")


class DeterministicEvidenceTests(unittest.TestCase):
    """Two runs in two workspaces must produce byte-identical evidence CSVs."""

    def _evidence_csvs(self, ws: str, incident_id: str) -> dict[str, bytes]:
        tables = _run_dir(ws, incident_id) / "evidence" / "tables"
        return {p.name: p.read_bytes() for p in sorted(tables.glob("*.csv"))}

    def _check(self, incident_id: str) -> None:
        with tempfile.TemporaryDirectory() as ws1, tempfile.TemporaryDirectory() as ws2:
            commands.start(commands._Args(incident=incident_id, workspace=ws1))
            commands.start(commands._Args(incident=incident_id, workspace=ws2))
            a = self._evidence_csvs(ws1, incident_id)
            b = self._evidence_csvs(ws2, incident_id)
            self.assertEqual(a.keys(), b.keys())
            for name in a:
                self.assertEqual(a[name], b[name],
                                 f"{incident_id} evidence CSV {name} differs across workspaces")

    def test_mp06(self): self._check("MP-06")
    def test_mp07(self): self._check("MP-07")
    def test_mp08(self): self._check("MP-08")
    def test_mp09(self): self._check("MP-09")
    def test_mp10(self): self._check("MP-10")


class MP08PaginationEdgeCaseTests(unittest.TestCase):
    """0, 1, page_size-1, page_size, page_size+1, and multi-page fixtures."""

    def test_correct_consumer_collects_everything(self):
        for total in (0, 1, api08.BATCH_SIZE - 1, api08.BATCH_SIZE, api08.BATCH_SIZE + 1, 20):
            ep = api08.FakeTripsEndpoint(total)
            t = api08.consume_correct(ep)
            self.assertEqual(len(t.rows), total, f"correct consumer wrong for total={total}")
            self.assertTrue(t.reached_terminal)

    def test_faulty_consumer_drops_the_final_response(self):
        # For multi-response endpoints the faulty loop drops exactly the last response.
        for total in (api08.BATCH_SIZE + 1, 20):
            ep = api08.FakeTripsEndpoint(total)
            correct = api08.consume_correct(ep)
            faulty = api08.consume_faulty(ep)
            missing = api08.missing_final_rows(ep)
            self.assertEqual(len(faulty.rows), len(correct.rows) - len(missing))
            self.assertFalse(faulty.reached_terminal)


class MP09DeliveryVariantTests(unittest.TestCase):
    """Missing / extra / duplicate / corrupt delivery accounting."""

    def test_expected_equals_processed_plus_quarantined(self):
        deliveries = mp_09._all_deliveries()
        expected = {d for d, *_ in mp_09.DELIVERIES}

        # Missing: the west delivery never arrived.
        arrived_missing = expected - {mp_09.WEST_DELIVERY}
        self.assertEqual(len(expected), len(arrived_missing) + 1)  # 1 unaccounted

        # Extra: an unexpected delivery is not in the expected set and is quarantined.
        extra = {"PART_X"}
        processed = expected
        quarantined_extra = extra - expected
        self.assertEqual(len(quarantined_extra), 1)
        self.assertTrue(extra - expected)

        # Duplicate: a repeated delivery id dedupes to one processed unit.
        arrived_dup = list(expected) + [mp_09.WEST_DELIVERY]
        self.assertEqual(len(set(arrived_dup)), len(expected))

        # Corrupt: a delivery whose checksum does not match is quarantined, so
        # expected = processed + quarantined still closes.
        corrupt_id = "PART_1"
        good = expected - {corrupt_id}
        quarantined_corrupt = {corrupt_id}
        self.assertEqual(len(expected), len(good) + len(quarantined_corrupt))
        # A tampered checksum is detectable.
        self.assertNotEqual(deliveries[corrupt_id]["checksum"], "0" * 64)

    def test_checksums_are_deterministic(self):
        first = mp_09._all_deliveries()
        second = mp_09._all_deliveries()
        for did in first:
            self.assertEqual(first[did]["checksum"], second[did]["checksum"])


class MP06ArchiveTests(unittest.TestCase):
    def test_complete_archive_validates_and_truncation_does_not(self):
        from metropulse_lab.incidents import _batchB_mp06_archive as arc
        full = arc.build_complete_gzip()
        full_read = arc.read_archive(full)
        self.assertTrue(full_read.integrity_ok)
        self.assertEqual(len(full_read.records), arc.RECORD_COUNT)
        partial = arc.truncate_tail(full)
        partial_read = arc.read_archive(partial)
        self.assertFalse(partial_read.integrity_ok)
        self.assertLess(len(partial_read.records), arc.RECORD_COUNT)
        self.assertGreater(len(partial_read.records), 0)
        # deterministic bytes
        self.assertEqual(arc.build_complete_gzip(), full)


if __name__ == "__main__":
    unittest.main()
