"""MP-06 deterministic archive simulator (standard-library only).

Builds a complete gzip archive of a stop-event batch, deterministically removes
trailing bytes to model a tail-cut upload/copy, and reads back the readable
prefix of a partial stream without raising. The end-of-stream marker and the
producer size/record manifest are what prove completeness — object existence
alone does not.

Everything here is byte-deterministic across platforms: the gzip header uses a
fixed mtime of 0, Python writes a fixed OS byte, and the payload is generated
from fixed constants.
"""

from __future__ import annotations

import gzip
import json
import zlib
from dataclasses import dataclass
from typing import Any

# The 02:00 stop-event batch. Twenty-four ordered arrivals/departures; the final
# batch (the last six sequences) is what a tail cut drops.
_STATIONS = ("STN_001", "STN_002", "STN_003", "STN_004", "STN_005", "STN_006")
_TRIPS = ("TRIP_R1_0200", "TRIP_R2_0200")
RECORD_COUNT = 24
# How many trailing bytes the faulty upload loses. Chosen so the readable prefix
# still holds most records but the final batch and the end-of-stream trailer are
# gone.
TAIL_DROP_BYTES = 48


def stop_event_records() -> list[dict[str, Any]]:
    """The full, correct batch of stop events (deterministic)."""
    records: list[dict[str, Any]] = []
    base_minute = 0
    for i in range(1, RECORD_COUNT + 1):
        trip = _TRIPS[(i - 1) % len(_TRIPS)]
        station = _STATIONS[(i - 1) % len(_STATIONS)]
        event_type = "ARRIVAL" if i % 2 == 1 else "DEPARTURE"
        minute = base_minute + (i - 1) * 2
        occurred = f"2026-04-15T02:{minute // 60:02d}:{minute % 60:02d}Z"
        records.append({
            "stop_event_id": f"AEV_{i:04d}",
            "trip_id": trip,
            "station_id": station,
            "event_type": event_type,
            "occurred_at_utc": occurred,
            "source_sequence": i,
        })
    return records


def records_to_payload(records: list[dict[str, Any]]) -> bytes:
    """Serialize records to newline-delimited JSON bytes (stable key order)."""
    lines = [json.dumps(r, sort_keys=True, ensure_ascii=True) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8")


def build_complete_gzip(records: list[dict[str, Any]] | None = None) -> bytes:
    """A complete gzip archive with a fixed header (deterministic bytes)."""
    if records is None:
        records = stop_event_records()
    payload = records_to_payload(records)
    return gzip.compress(payload, compresslevel=9, mtime=0)


def truncate_tail(data: bytes, drop_bytes: int = TAIL_DROP_BYTES) -> bytes:
    """Deterministically remove the last ``drop_bytes`` bytes."""
    if drop_bytes <= 0 or drop_bytes >= len(data):
        raise ValueError("drop_bytes out of range")
    return data[:-drop_bytes]


@dataclass(frozen=True)
class ArchiveReadResult:
    records: list[dict[str, Any]]
    end_marker_reached: bool          # gzip end-of-stream marker present
    integrity_ok: bool                # end marker + CRC32 + declared size all agree
    decoded_bytes: int
    stored_crc32: int | None
    computed_crc32: int
    stored_size: int | None


def _read_trailer(data: bytes) -> tuple[int | None, int | None]:
    """Return (stored_crc32, stored_isize) from a complete gzip stream, else Nones."""
    if len(data) < 8:
        return None, None
    crc = int.from_bytes(data[-8:-4], "little")
    isize = int.from_bytes(data[-4:], "little")
    return crc, isize


def read_archive(data: bytes) -> ArchiveReadResult:
    """Read an archive, tolerating a partial stream.

    A naive consumer that only checks object existence and reads the decodable
    prefix will accept a partial stream. This models exactly that: it decodes as
    far as it can, keeps only whole JSON lines, and reports whether the
    end-of-stream marker and the size/CRC trailer actually validated.
    """
    decomp = zlib.decompressobj(31)  # 31 = gzip header + trailer handling
    decoded = decomp.decompress(data)
    try:
        decoded += decomp.flush()
    except zlib.error:
        pass
    end_marker_reached = decomp.eof

    # Keep only complete lines that parse as JSON.
    text = decoded.decode("utf-8", errors="ignore")
    records: list[dict[str, Any]] = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    computed_crc = zlib.crc32(decoded) & 0xFFFFFFFF
    stored_crc, stored_isize = (None, None)
    if end_marker_reached:
        stored_crc, stored_isize = _read_trailer(data)
    integrity_ok = bool(
        end_marker_reached
        and stored_crc is not None
        and stored_crc == computed_crc
        and stored_isize == len(decoded)
    )
    return ArchiveReadResult(
        records=records,
        end_marker_reached=end_marker_reached,
        integrity_ok=integrity_ok,
        decoded_bytes=len(decoded),
        stored_crc32=stored_crc,
        computed_crc32=computed_crc,
        stored_size=stored_isize,
    )
