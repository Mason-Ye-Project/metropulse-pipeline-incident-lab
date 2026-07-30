"""Contract tests: canonical catalog, protocol conformance, core import purity."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from metropulse_lab import config
from metropulse_lab.incidents import registry
from metropulse_lab.incidents.contract import Incident

PACKAGE_ROOT = Path(__file__).resolve().parent.parent.parent / "metropulse_lab"

# Packages that must never be imported by the core (transfer-mapping only).
FORBIDDEN_IMPORTS = (
    "duckdb", "psycopg", "psycopg2", "sqlalchemy", "pyspark", "kafka",
    "airflow", "dbt", "pyiceberg", "iceberg", "requests", "boto3",
)
# Root-cause words that must not appear in a reader-facing incident title.
FORBIDDEN_TITLE_WORDS = ("cumulative", "per-event", "counter_as_delta", "delta")


class CatalogContractTests(unittest.TestCase):
    def test_exactly_25_canonical_ids_in_order(self):
        self.assertEqual(len(registry.CANONICAL_IDS), 25)
        self.assertEqual(list(registry.CANONICAL_IDS),
                         [f"MP-{n:02d}" for n in range(1, 26)])

    def test_implemented_incidents_follow_protocol(self):
        for incident_id in registry.implemented_ids():
            incident = registry.load(incident_id)
            self.assertIsInstance(incident, Incident,
                                  f"{incident_id} does not implement the lifecycle protocol")
            self.assertEqual(incident.metadata.incident_id, incident_id)
            self.assertIn(incident.metadata.recovery_mode, ("REPLAY", "ROLLBACK", "NO_REPLAY"))
            self.assertIn(incident.metadata.core_evidence_kind,
                          ("REAL_SQLITE", "DETERMINISTIC_SIMULATOR", "MIXED_LABELED"))

    def test_reader_titles_hide_root_cause(self):
        for incident_id in registry.implemented_ids():
            title = registry.load(incident_id).metadata.reader_title.lower()
            for word in FORBIDDEN_TITLE_WORDS:
                self.assertNotIn(word, title,
                                 f"{incident_id} title reveals root cause: {word!r}")

    def test_core_imports_no_forbidden_packages(self):
        offenders = []
        for py in PACKAGE_ROOT.rglob("*.py"):
            text = py.read_text(encoding="utf-8")
            for mod in FORBIDDEN_IMPORTS:
                if re.search(rf"^\s*(import|from)\s+{re.escape(mod)}\b", text, re.MULTILINE):
                    offenders.append(f"{py.name}:{mod}")
        self.assertEqual(offenders, [], f"forbidden imports in core: {offenders}")

    def test_no_network_imports_in_core(self):
        offenders = []
        for py in PACKAGE_ROOT.rglob("*.py"):
            text = py.read_text(encoding="utf-8")
            if re.search(r"^\s*(import|from)\s+(urllib\.request|http\.client|socket)\b",
                         text, re.MULTILINE):
                offenders.append(py.name)
        self.assertEqual(offenders, [], f"network imports in core: {offenders}")

    def test_forbidden_domain_tokens_defined(self):
        # The guard list itself must exist so privacy tests can enforce it.
        for token in ("TinyShop", "Northstar", "BrokenMart"):
            self.assertIn(token, config.FORBIDDEN_DOMAIN_TOKENS)


if __name__ == "__main__":
    unittest.main()
