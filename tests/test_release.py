"""Release smoke: doctor health and a full MP-01 solve in a clean temp workspace."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from metropulse_lab import commands, config


class ReleaseSmokeTests(unittest.TestCase):
    def test_license_and_scope_notice_present(self):
        lab_root = Path(__file__).resolve().parents[1]
        license_text = (lab_root / "LICENSE").read_text(encoding="utf-8")
        notice_text = (lab_root / "NOTICE.md").read_text(encoding="utf-8")

        self.assertIn("MIT License", license_text)
        self.assertIn("Copyright (c) 2026 MASON YE", license_text)
        for excluded_asset in ("book", "cover", "KDP metadata"):
            self.assertIn(excluded_asset, notice_text)

    def test_doctor_ok(self):
        result = commands.doctor(commands._Args())
        self.assertEqual(result["exit_code"], config.EXIT_OK)
        self.assertTrue(result["data"]["window_functions"])
        self.assertFalse(result["data"]["network_required"])

    def test_full_solution_path_verifies(self):
        with tempfile.TemporaryDirectory() as ws:
            start = commands.start(commands._Args(incident="MP-01", workspace=ws))
            run_id = start["data"]["run_id"]
            rr = commands.reference_recovery(commands._Args(run=run_id, workspace=ws))
            self.assertEqual(rr["exit_code"], config.EXIT_OK)
            self.assertEqual(rr["data"]["verify"], "PASS")

    def test_reset_is_safe_and_scoped(self):
        with tempfile.TemporaryDirectory() as ws:
            commands.create(commands._Args(incident="MP-01", workspace=ws))
            reset = commands.reset(commands._Args(run="run-mp01-0001", workspace=ws))
            self.assertEqual(reset["exit_code"], config.EXIT_OK)
            self.assertFalse((Path(ws) / "runs" / "run-mp01-0001").exists())


if __name__ == "__main__":
    unittest.main()
