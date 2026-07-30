"""MetroPulse deterministic capacity guard (MP-16 support).

A pure-Python guard that computes how many rows and how many serialized bytes a
step would assemble in a single worker, then refuses when that projection would
exceed a hard byte bound. The guard is the safety control the reference repair
installs: any result assembled into one process must first prove its projected
size against a contracted limit.

Every value here is a LOGICAL projection (row count x bytes-per-row). It is not
real process memory, not laptop milliseconds, and not a distributed-engine
artifact. The guard never materializes the projected rows; it only arithmetic-
checks the projection so the check itself is cheap and deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass

from ._batchD_common import EVIDENCE_ORIGIN


@dataclass(frozen=True)
class GuardResult:
    allowed: bool
    projected_rows: int
    bytes_per_row: int
    projected_bytes: int
    limit_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "evidence_origin": EVIDENCE_ORIGIN,
            "allowed": self.allowed,
            "projected_rows": self.projected_rows,
            "bytes_per_row": self.bytes_per_row,
            "projected_bytes": self.projected_bytes,
            "limit_bytes": self.limit_bytes,
        }


class CapacityExceeded(Exception):
    """Raised when a projected single-worker assembly exceeds the hard bound."""

    def __init__(self, result: GuardResult) -> None:
        self.result = result
        super().__init__(
            f"projected {result.projected_bytes} bytes exceeds the "
            f"{result.limit_bytes}-byte assembly bound"
        )


class CapacityGuard:
    """Refuse to assemble a result larger than ``limit_bytes`` in one worker."""

    def __init__(self, limit_bytes: int) -> None:
        self.limit_bytes = int(limit_bytes)

    def evaluate(self, projected_rows: int, bytes_per_row: int) -> GuardResult:
        projected_rows = int(projected_rows)
        bytes_per_row = int(bytes_per_row)
        projected_bytes = projected_rows * bytes_per_row
        return GuardResult(
            allowed=projected_bytes <= self.limit_bytes,
            projected_rows=projected_rows,
            bytes_per_row=bytes_per_row,
            projected_bytes=projected_bytes,
            limit_bytes=self.limit_bytes,
        )

    def assemble_or_refuse(self, projected_rows: int, bytes_per_row: int) -> GuardResult:
        result = self.evaluate(projected_rows, bytes_per_row)
        if not result.allowed:
            raise CapacityExceeded(result)
        return result
