"""Unit tests for path containment, safe cleanup, and the run lease."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from metropulse_lab import safety


class SafetyTests(unittest.TestCase):
    def test_resolve_within_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            with self.assertRaises(safety.UnsafePathError):
                safety.resolve_within(base, "..", "escape")

    def test_resolve_within_rejects_absolute(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(safety.UnsafePathError):
                safety.resolve_within(Path(d), "/etc/passwd")

    def test_resolve_within_allows_child(self):
        with tempfile.TemporaryDirectory() as d:
            got = safety.resolve_within(Path(d), "runs", "run-mp01-0001")
            self.assertTrue(str(got).startswith(str(Path(d).resolve())))

    def test_validate_run_dir_refuses_runs_root(self):
        with tempfile.TemporaryDirectory() as d:
            runs = Path(d) / "runs"
            runs.mkdir()
            with self.assertRaises(safety.UnsafePathError):
                safety.validate_run_dir(runs, runs)

    def test_validate_run_dir_refuses_non_child(self):
        with tempfile.TemporaryDirectory() as d:
            runs = Path(d) / "runs"
            runs.mkdir()
            stray = Path(d) / "stray"
            stray.mkdir()
            with self.assertRaises(safety.UnsafePathError):
                safety.validate_run_dir(stray, runs)

    def test_validate_run_dir_refuses_bad_name(self):
        with tempfile.TemporaryDirectory() as d:
            runs = Path(d) / "runs"
            runs.mkdir()
            bad = runs / "notarun"
            bad.mkdir()
            with self.assertRaises(safety.UnsafePathError):
                safety.validate_run_dir(bad, runs)

    def test_safe_rmtree_removes_only_run(self):
        with tempfile.TemporaryDirectory() as d:
            runs = Path(d) / "runs"
            runs.mkdir()
            run = runs / "run-mp01-0001"
            run.mkdir()
            (run / "f.txt").write_text("x")
            other = runs / "run-mp02-0001"
            other.mkdir()
            safety.safe_rmtree_run(run, runs)
            self.assertFalse(run.exists())
            self.assertTrue(other.exists())

    def test_reset_refuses_empty_path(self):
        with tempfile.TemporaryDirectory() as d:
            runs = Path(d) / "runs"
            runs.mkdir()
            with self.assertRaises(safety.UnsafePathError):
                safety.validate_run_dir(Path(""), runs)

    def test_lease_is_exclusive(self):
        with tempfile.TemporaryDirectory() as d:
            run = Path(d)
            lease1 = safety.RunLease(run)
            lease1.acquire("op1")
            lease2 = safety.RunLease(run)
            with self.assertRaises(safety.UnsafePathError):
                lease2.acquire("op2")
            lease1.release()
            lease2.acquire("op2")  # now free
            lease2.release()


if __name__ == "__main__":
    unittest.main()
