"""Deterministic clock and fixture generation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from metropulse_lab import baseline
from metropulse_lab.clock import LogicalClock, format_utc, parse_utc


class ClockTests(unittest.TestCase):
    def test_parse_format_roundtrip(self):
        self.assertEqual(format_utc(parse_utc("2026-04-13T06:00:00Z")),
                         "2026-04-13T06:00:00Z")

    def test_logical_clock_monotonic(self):
        c = LogicalClock("2026-04-13T00:00:00Z", step_seconds=60)
        t0 = c.now()
        t1 = c.tick()
        self.assertLess(t0, t1)

    def test_clock_is_wallclock_independent(self):
        # Two clocks seeded identically produce identical sequences.
        a = LogicalClock("2026-04-13T00:00:00Z")
        b = LogicalClock("2026-04-13T00:00:00Z")
        self.assertEqual([a.tick() for _ in range(5)], [b.tick() for _ in range(5)])


class FixtureDeterminismTests(unittest.TestCase):
    def test_baseline_inputs_are_deterministic(self):
        with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
            a = baseline.write_baseline_inputs(Path(d1))
            b = baseline.write_baseline_inputs(Path(d2))
            self.assertEqual(a, b)

    def test_gate_control_matches_true_boardings(self):
        # In the healthy baseline the per-route-hour gate totals equal the true
        # onboard boardings (correct session deltas), so reconciliation is exact.
        true_boardings = baseline._true_route_hour_boardings()
        gate_rows = baseline._gate_count_events()
        gate_totals: dict[tuple, int] = {}
        for r in gate_rows:
            route, service_date, hour = r["trip_stop_window"].split(":")
            key = (service_date, route, int(hour))
            gate_totals[key] = gate_totals.get(key, 0) + r["passenger_count"]
        self.assertEqual(gate_totals, true_boardings)


if __name__ == "__main__":
    unittest.main()
