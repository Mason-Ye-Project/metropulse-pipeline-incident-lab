"""The operation lease must be acquired on the real command path and fail closed.

Regression for a Gate 1 defect: the context-managed command path used
``with RunLease(run_dir):`` but ``__enter__`` never acquired, so mutating
commands ran without exclusivity.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from metropulse_lab import commands
from metropulse_lab.safety import RunLease, UnsafePathError


class LeaseContextManagerTests(unittest.TestCase):
    def test_context_manager_actually_acquires(self):
        with tempfile.TemporaryDirectory() as d:
            run_dir = Path(d)
            lock = run_dir / ".op.lock"
            self.assertFalse(lock.exists())
            with RunLease(run_dir, "op"):
                self.assertTrue(lock.exists(), "lease not acquired inside the with-block")
            self.assertFalse(lock.exists(), "lease not released after the with-block")

    def test_second_context_manager_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            run_dir = Path(d)
            with RunLease(run_dir, "first"):
                with self.assertRaises(UnsafePathError):
                    with RunLease(run_dir, "second"):
                        pass
            # First lease released normally.
            self.assertFalse((run_dir / ".op.lock").exists())


class LeaseCommandPathTests(unittest.TestCase):
    def test_normal_command_leaves_no_lock(self):
        with tempfile.TemporaryDirectory() as ws:
            commands.create(commands._Args(incident="MP-01", workspace=ws))
            run_dir = Path(ws) / "runs" / "run-mp01-0001"
            self.assertFalse((run_dir / ".op.lock").exists(),
                             "create left a stale lease behind")
            commands.inject(commands._Args(run="run-mp01-0001", workspace=ws))
            self.assertFalse((run_dir / ".op.lock").exists(),
                             "inject left a stale lease behind")

    def test_command_fails_closed_on_foreign_lease(self):
        with tempfile.TemporaryDirectory() as ws:
            commands.create(commands._Args(incident="MP-01", workspace=ws))
            run_dir = Path(ws) / "runs" / "run-mp01-0001"
            foreign = run_dir / ".op.lock"
            foreign.write_text("held-by-another-op")
            # A mutating command must refuse and preserve the foreign lock.
            with self.assertRaises(UnsafePathError):
                commands.inject(commands._Args(run="run-mp01-0001", workspace=ws))
            self.assertTrue(foreign.exists(), "foreign lease was not preserved (not fail-closed)")
            self.assertEqual(foreign.read_text(), "held-by-another-op")

    def test_recover_lease_unwedges_run(self):
        with tempfile.TemporaryDirectory() as ws:
            commands.create(commands._Args(incident="MP-01", workspace=ws))
            run_dir = Path(ws) / "runs" / "run-mp01-0001"
            (run_dir / ".op.lock").write_text("stale")
            # recover-lease removes only this run's lease...
            out = commands.recover_lease(commands._Args(run="run-mp01-0001", workspace=ws))
            self.assertTrue(out["data"]["lease_removed"])
            self.assertFalse((run_dir / ".op.lock").exists())
            # ...and the run is runnable again.
            commands.inject(commands._Args(run="run-mp01-0001", workspace=ws))
            # A second recover-lease with no stale lock is a no-op, not an error.
            out2 = commands.recover_lease(commands._Args(run="run-mp01-0001", workspace=ws))
            self.assertFalse(out2["data"]["lease_removed"])

    def test_recover_lease_refuses_unsafe_run(self):
        with tempfile.TemporaryDirectory() as ws:
            (Path(ws) / "runs").mkdir(parents=True)
            with self.assertRaises(UnsafePathError):
                commands.recover_lease(commands._Args(run="../../etc", workspace=ws))


if __name__ == "__main__":
    unittest.main()
