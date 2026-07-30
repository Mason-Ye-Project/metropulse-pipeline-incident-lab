"""MetroPulse deterministic lookup adapter (MP-17 support).

A local, in-process key -> attribute lookup with a call counter and a fixed
logical cost per call. The adapter deliberately measures only two things: how
many lookup requests were issued, and a fixed number of logical cost units per
request. There are NO sleeps and NO claimed network or database latency, so the
resulting evidence is fully deterministic and never pretends to be a production
timing or a per-request billing figure.

Two access shapes are offered:

* ``get_one`` issues one request per call — the shape whose request count tracks
  the number of fact rows;
* ``get_bulk`` resolves every distinct key the batch needs in a single request —
  the shape whose request count tracks the number of distinct keys.
"""

from __future__ import annotations

from typing import Any, Iterable

from ._batchD_common import EVIDENCE_ORIGIN


class CountingLookup:
    def __init__(self, backing: dict[str, Any], cost_units_per_call: int = 1) -> None:
        self._backing = dict(backing)
        self.cost_units_per_call = int(cost_units_per_call)
        self.calls = 0
        self.cost_units = 0
        self.missing = 0

    def get_one(self, key: str) -> Any:
        self.calls += 1
        self.cost_units += self.cost_units_per_call
        value = self._backing.get(key)
        if value is None:
            self.missing += 1
        return value

    def get_bulk(self, keys: Iterable[str]) -> dict[str, Any]:
        """One batched request that resolves all distinct keys at once."""
        self.calls += 1
        self.cost_units += self.cost_units_per_call
        distinct = sorted(set(keys))
        out: dict[str, Any] = {}
        for key in distinct:
            value = self._backing.get(key)
            if value is None:
                self.missing += 1
            out[key] = value
        return out

    def counters(self) -> dict[str, Any]:
        return {
            "evidence_origin": EVIDENCE_ORIGIN,
            "requests": self.calls,
            "cost_units": self.cost_units,
            "unresolved_keys": self.missing,
        }
