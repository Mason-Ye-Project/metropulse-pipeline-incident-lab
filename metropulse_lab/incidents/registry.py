"""Incident registry.

``planning/incident_taxonomy.md`` is the sole authority for the canonical IDs and
their order. The registry loads the implemented ``mp_XX`` handler modules and
exposes the canonical list so contract tests can assert the one-to-one binding.
"""

from __future__ import annotations

import importlib
from typing import Any

# Canonical MP-01 .. MP-25 in taxonomy order.
CANONICAL_IDS: tuple[str, ...] = tuple(f"MP-{n:02d}" for n in range(1, 26))


def _module_name(incident_id: str) -> str:
    return f"metropulse_lab.incidents.{incident_id.replace('-', '_').lower()}"


def is_valid_id(incident_id: str) -> bool:
    return incident_id in CANONICAL_IDS


def load(incident_id: str):
    """Return the INCIDENT object for an implemented incident, or raise."""
    if not is_valid_id(incident_id):
        raise KeyError(f"unknown incident id: {incident_id!r}")
    try:
        module = importlib.import_module(_module_name(incident_id))
    except ModuleNotFoundError as exc:
        raise NotImplementedError(f"{incident_id} is not implemented yet") from exc
    return module.INCIDENT


def implemented_ids() -> list[str]:
    out = []
    for incident_id in CANONICAL_IDS:
        try:
            importlib.import_module(_module_name(incident_id))
            out.append(incident_id)
        except ModuleNotFoundError:
            continue
    return out


def list_incidents() -> list[dict[str, Any]]:
    implemented = set(implemented_ids())
    rows = []
    for incident_id in CANONICAL_IDS:
        entry: dict[str, Any] = {"incident_id": incident_id,
                                 "implemented": incident_id in implemented}
        if incident_id in implemented:
            meta = load(incident_id).metadata
            entry["reader_title"] = meta.reader_title
            entry["family"] = meta.family
            entry["recovery_mode"] = meta.recovery_mode
        rows.append(entry)
    return rows
