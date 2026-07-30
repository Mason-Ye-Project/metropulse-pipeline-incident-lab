"""MetroPulse deterministic planner-cost model (MP-20 support).

A SEPARATELY LABELED cost illustration. It shows the consequence of the inner
size a cost chooser *believes* (from a recorded size profile) versus the inner
size that is actually present. When the believed size is small the chooser may
pick a repeated-probe join that is cheap while the table is small but expensive
once the real table is large.

This model is explicitly NOT:

* an ``EXPLAIN ANALYZE`` or any real optimizer decision;
* PostgreSQL, DuckDB, or any other engine's output;
* elapsed production time, buffer reads, or a memory measurement.

It is a deterministic MetroPulse illustration used only to explain why a wrong
size belief changes the chosen join method and its cost. The engine-of-record
evidence for MP-20 is the real SQLite ``sqlite_stat1`` content and query plan
captured beside it.
"""

from __future__ import annotations

from ._batchD_common import EVIDENCE_ORIGIN

# Below this believed inner size an unaided chooser may prefer a repeated probe.
REPEATED_PROBE_THRESHOLD = 500


def estimate_join(outer_rows: int, believed_inner_rows: int,
                  actual_inner_rows: int, has_index: bool) -> dict[str, object]:
    outer_rows = int(outer_rows)
    believed_inner_rows = int(believed_inner_rows)
    actual_inner_rows = int(actual_inner_rows)

    if believed_inner_rows <= REPEATED_PROBE_THRESHOLD and not has_index:
        chosen = "repeated_probe"
        planned_cost = outer_rows * max(1, believed_inner_rows)
        actual_cost = outer_rows * max(1, actual_inner_rows)
    else:
        chosen = "merge_or_index"
        planned_cost = outer_rows + believed_inner_rows
        actual_cost = outer_rows + actual_inner_rows

    return {
        "evidence_origin": EVIDENCE_ORIGIN,
        "note": "labeled cost model, not a real optimizer decision or a timing",
        "outer_rows": outer_rows,
        "believed_inner_rows": believed_inner_rows,
        "actual_inner_rows": actual_inner_rows,
        "has_index": bool(has_index),
        "chosen_method": chosen,
        "planned_cost_units": planned_cost,
        "actual_cost_units": actual_cost,
        # Integer size-belief error (actual per believed). No floats used as an oracle.
        "size_belief_error_x": actual_inner_rows // max(1, believed_inner_rows),
    }
