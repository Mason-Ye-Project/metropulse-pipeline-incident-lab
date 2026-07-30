"""MP-08 local fake paginated API (deterministic, no network).

A fixed ledger of pages, each carrying a continuation marker to the next page.
The authoritative terminal condition is a null next-marker on the last page. A
correct consumer follows markers until the terminal condition and collects every
row. A faulty consumer with an off-by-one stop condition drops exactly the final
page.

The ledger is generated from fixed constants, so every traversal is
byte-deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

BATCH_SIZE = 5  # rows per response (the "page size")
FIRST_MARKER = "M-000"


@dataclass(frozen=True)
class ApiResponse:
    request_marker: str
    rows: list[dict[str, Any]]
    next_marker: str | None       # None means the authoritative terminal state
    response_code: int = 200


class FakeTripsEndpoint:
    """A deterministic multi-response endpoint of scheduled trips."""

    def __init__(self, total_rows: int, batch_size: int = BATCH_SIZE) -> None:
        self.total_rows = total_rows
        self.batch_size = batch_size
        self._pages = self._build_pages()

    def _build_pages(self) -> list[ApiResponse]:
        rows = [
            {
                "trip_id": f"SCHTRIP_{i:04d}",
                "route_id": f"R{(i % 4) + 1}",
                "service_date": "2026-04-15",
                "scheduled_start_utc": f"2026-04-15T{6 + (i % 12):02d}:00:00Z",
            }
            for i in range(1, self.total_rows + 1)
        ]
        pages: list[ApiResponse] = []
        # Always at least one response, even for an empty endpoint.
        chunks = [rows[i:i + self.batch_size]
                  for i in range(0, len(rows), self.batch_size)] or [[]]
        for idx, chunk in enumerate(chunks):
            marker = f"M-{idx:03d}"
            next_marker = f"M-{idx + 1:03d}" if idx + 1 < len(chunks) else None
            pages.append(ApiResponse(request_marker=marker, rows=chunk,
                                     next_marker=next_marker))
        return pages

    def get(self, marker: str) -> ApiResponse:
        for page in self._pages:
            if page.request_marker == marker:
                return page
        raise KeyError(f"unknown request marker: {marker}")

    def declared_total(self) -> int:
        """The full-endpoint control count (authoritative)."""
        return self.total_rows

    def response_count(self) -> int:
        return len(self._pages)

    def final_marker(self) -> str:
        return self._pages[-1].request_marker


@dataclass(frozen=True)
class Traversal:
    rows: list[dict[str, Any]]
    request_log: list[dict[str, Any]]   # per response: marker, next_marker, code, rows, followed
    reached_terminal: bool


def consume_correct(endpoint: FakeTripsEndpoint) -> Traversal:
    """Follow markers until the authoritative terminal (null next-marker)."""
    rows: list[dict[str, Any]] = []
    log: list[dict[str, Any]] = []
    marker: str | None = FIRST_MARKER
    while marker is not None:
        resp = endpoint.get(marker)
        rows.extend(resp.rows)
        followed = resp.next_marker is not None
        log.append({"request_marker": resp.request_marker,
                    "next_marker": resp.next_marker,
                    "response_code": resp.response_code,
                    "rows_returned": len(resp.rows),
                    "followed": followed})
        marker = resp.next_marker
    return Traversal(rows=rows, request_log=log, reached_terminal=True)


def consume_faulty(endpoint: FakeTripsEndpoint) -> Traversal:
    """Off-by-one stop condition.

    The loop bounds itself by an assumed count and uses ``> 1`` where it should
    use ``> 0``, so it stops one request short and never issues the request the
    final continuation marker points to. The final page is therefore never
    fetched or persisted, and the recorded request chain never reaches the
    authoritative terminal (a null next-marker)."""
    rows: list[dict[str, Any]] = []
    log: list[dict[str, Any]] = []
    marker: str | None = FIRST_MARKER
    pages_left = endpoint.response_count()   # the consumer's assumption
    while marker is not None and pages_left > 1:   # bug: should be > 0
        resp = endpoint.get(marker)
        rows.extend(resp.rows)
        log.append({"request_marker": resp.request_marker,
                    "next_marker": resp.next_marker,
                    "response_code": resp.response_code,
                    "rows_returned": len(resp.rows),
                    "followed": resp.next_marker is not None})
        marker = resp.next_marker
        pages_left -= 1
    reached = bool(log) and log[-1]["next_marker"] is None
    return Traversal(rows=rows, request_log=log, reached_terminal=reached)


def missing_final_rows(endpoint: FakeTripsEndpoint) -> list[dict[str, Any]]:
    """The rows on the final page — what the faulty consumer drops."""
    return list(endpoint.get(endpoint.final_marker()).rows)
