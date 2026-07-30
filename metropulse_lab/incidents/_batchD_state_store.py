"""MetroPulse deterministic keyed-store simulator (MP-19 support).

A pure-Python model of a keyed, event-time store observed once per logical hour.
At a fixed input rate the model reports, per snapshot: how many keys are retained
and an estimated byte size. With a retention boundary the retained set plateaus;
without one it grows monotonically. It also emits the terminal business result so
a restart can be shown to reproduce the same output regardless of retention.

This is NOT Flink, Spark, or any streaming engine. Byte sizes are estimates
(retained keys x bytes-per-key), not measured heap, and the per-hour snapshot is
a logical count, not a wall-clock duration.
"""

from __future__ import annotations

from typing import Any, Optional

from ._batchD_common import EVIDENCE_ORIGIN


class KeyedStoreSimulator:
    def __init__(self, new_keys_per_hour: int, bytes_per_key: int) -> None:
        self.new_keys_per_hour = int(new_keys_per_hour)
        self.bytes_per_key = int(bytes_per_key)

    def retained_keys_at(self, hour_index: int, retain_hours: Optional[int]) -> int:
        """Cumulative retained keys after ``hour_index`` (1-based) hours.

        ``retain_hours=None`` keeps every key ever seen (no boundary). A finite
        ``retain_hours`` keeps only keys from the most recent window.
        """
        hour_index = int(hour_index)
        if retain_hours is None:
            active_hours = hour_index
        else:
            active_hours = min(hour_index, int(retain_hours))
        return self.new_keys_per_hour * active_hours

    def snapshots(self, hours: int, retain_hours: Optional[int]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        prev_bytes = 0
        for hour in range(1, int(hours) + 1):
            keys = self.retained_keys_at(hour, retain_hours)
            store_bytes = keys * self.bytes_per_key
            out.append({
                "snapshot_index": hour,
                "retained_keys": keys,
                "store_bytes": store_bytes,
                "snapshot_bytes": store_bytes,
                "growth_bytes": store_bytes - prev_bytes,
            })
            prev_bytes = store_bytes
        return out

    @staticmethod
    def growth_slope(snapshots: list[dict[str, Any]]) -> int:
        """Total byte growth from the first to the last snapshot."""
        if len(snapshots) < 2:
            return 0
        return snapshots[-1]["store_bytes"] - snapshots[0]["store_bytes"]

    @staticmethod
    def is_bounded(snapshots: list[dict[str, Any]], tail: int = 3) -> bool:
        """True when the last ``tail`` snapshots stopped growing (plateau)."""
        if len(snapshots) <= tail:
            return False
        window = snapshots[-tail:]
        return all(s["store_bytes"] == window[0]["store_bytes"] for s in window)

    def terminal_entries(self, hours: int, retain_hours: Optional[int]) -> list[dict[str, Any]]:
        """Retained entries in the final snapshot (for the state-entry ledger).

        Keys are numbered by the hour they were opened; only keys within the
        retention window survive to the final snapshot.
        """
        hours = int(hours)
        entries: list[dict[str, Any]] = []
        for hour in range(1, hours + 1):
            if retain_hours is not None and hour <= hours - int(retain_hours):
                continue  # evicted before the final snapshot
            for k in range(self.new_keys_per_hour):
                idx = (hour - 1) * self.new_keys_per_hour + k
                entries.append({
                    "state_key": f"window-{idx:04d}",
                    "event_time": f"2026-04-15T{hour % 24:02d}:00:00Z",
                    "expiry_time": (None if retain_hours is None
                                    else f"2026-04-15T{(hour + int(retain_hours)) % 24:02d}:00:00Z"),
                    "byte_estimate": self.bytes_per_key,
                })
        return entries

    def window_results(self, hours: int) -> dict[str, int]:
        """Terminal business result per window: independent of retention.

        Evicting an ended window's key does not change its already-emitted value,
        so both the retained and the bounded run — and a restart of either —
        produce this same mapping.
        """
        results: dict[str, int] = {}
        total_keys = int(hours) * self.new_keys_per_hour
        for idx in range(total_keys):
            # A deterministic terminal value for the window (lab value).
            results[f"window-{idx:04d}"] = (idx % 7) + 1
        return results

    @staticmethod
    def descriptor() -> dict[str, str]:
        return {"evidence_origin": EVIDENCE_ORIGIN}
