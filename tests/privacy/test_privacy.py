"""Privacy / release-hygiene scans over committed source and fixtures."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from metropulse_lab import config

LAB_ROOT = Path(__file__).resolve().parent.parent.parent
PACKAGE_ROOT = LAB_ROOT / "metropulse_lab"
FIXTURES_ROOT = LAB_ROOT / "fixtures"

# Emails are only allowed under reserved .invalid / .example domains.
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
ALLOWED_EMAIL_SUFFIXES = (".invalid", ".example")


class PrivacyScanTests(unittest.TestCase):
    def _committed_text_files(self):
        for root in (PACKAGE_ROOT, FIXTURES_ROOT):
            for path in root.rglob("*"):
                if path.is_file() and path.suffix in (
                    ".py", ".csv", ".json", ".jsonl", ".sql", ".md", ".txt"
                ):
                    yield path

    def test_no_forbidden_domain_tokens_in_use(self):
        # The guard definition in config.py is allowed; a real use elsewhere is not.
        offenders = []
        for path in self._committed_text_files():
            if path.name == "config.py":
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            for token in config.FORBIDDEN_DOMAIN_TOKENS:
                if token in text:
                    offenders.append(f"{path.name}:{token}")
        self.assertEqual(offenders, [], f"forbidden domain tokens: {offenders}")

    def test_no_real_emails(self):
        offenders = []
        for path in self._committed_text_files():
            text = path.read_text(encoding="utf-8", errors="replace")
            for match in EMAIL_RE.findall(text):
                if not any(match.lower().endswith(s) for s in ALLOWED_EMAIL_SUFFIXES):
                    offenders.append(f"{path.name}:{match}")
        self.assertEqual(offenders, [], f"non-reserved emails: {offenders}")

    def test_no_home_directory_paths(self):
        offenders = []
        for path in self._committed_text_files():
            text = path.read_text(encoding="utf-8", errors="replace")
            if "/Users/" in text or "/home/" in text:
                offenders.append(path.name)
        self.assertEqual(offenders, [], f"home-directory paths committed: {offenders}")

    def test_no_plausible_secrets(self):
        secret_re = re.compile(r"(AKIA[0-9A-Z]{16}|-----BEGIN [A-Z ]*PRIVATE KEY-----)")
        offenders = []
        for path in self._committed_text_files():
            text = path.read_text(encoding="utf-8", errors="replace")
            if secret_re.search(text):
                offenders.append(path.name)
        self.assertEqual(offenders, [], f"plausible secrets: {offenders}")


if __name__ == "__main__":
    unittest.main()
