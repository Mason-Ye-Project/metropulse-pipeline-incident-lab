"""Run-id allocation must be collision-free even with gaps in existing runs."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from metropulse_lab import paths


class NewRunIdTests(unittest.TestCase):
    def _make_runs(self, ws: Path, names: list[str]) -> None:
        root = paths.runs_root(ws)
        for name in names:
            (root / name).mkdir(parents=True)

    def test_first_run_id(self):
        with tempfile.TemporaryDirectory() as ws:
            self.assertEqual(paths.new_run_id(Path(ws), "MP-01"), "run-mp01-0001")

    def test_gap_does_not_collide(self):
        # Regression: with 0001 and 0003 present, counting would return 0003 and
        # collide. Max-suffix+1 must return 0004.
        with tempfile.TemporaryDirectory() as ws:
            self._make_runs(Path(ws), ["run-mp01-0001", "run-mp01-0003"])
            got = paths.new_run_id(Path(ws), "MP-01")
            self.assertEqual(got, "run-mp01-0004")
            self.assertFalse((paths.runs_root(Path(ws)) / got).exists())

    def test_ignores_other_incidents_and_nondigits(self):
        with tempfile.TemporaryDirectory() as ws:
            self._make_runs(Path(ws), ["run-mp01-0002", "run-mp02-0009", "run-mp01-junk"])
            self.assertEqual(paths.new_run_id(Path(ws), "MP-01"), "run-mp01-0003")


if __name__ == "__main__":
    unittest.main()
