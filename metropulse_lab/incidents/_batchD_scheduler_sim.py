"""MetroPulse deterministic scheduler simulator (MP-18 support).

Projects how many orchestration work-items a chosen grain would produce, admits
a work manifest only up to a configured budget, records any rejected projected
count, and otherwise runs a small number of coarse batches. It never starts a
real process, thread, or subprocess: the "manifest" is rows in ``scheduler_run``
plus a returned projection. Every figure is a logical count.
"""

from __future__ import annotations

from math import prod
from typing import Any

from .. import db
from ..context import RunContext
from ._batchD_common import EVIDENCE_ORIGIN


class SchedulerSimulator:
    def __init__(self, budget: int) -> None:
        self.budget = int(budget)

    @staticmethod
    def project_units(dims: dict[str, int]) -> int:
        """Product of the chosen grain's dimensions (e.g. stations x windows x metrics)."""
        return int(prod(int(v) for v in dims.values())) if dims else 0

    def admit_fine(self, ctx: RunContext, projected_units: int,
                   dims: dict[str, int]) -> dict[str, Any]:
        """Project a fine (one-work-item-per-data-unit) grain against the budget.

        Records a single ``scheduler_run`` row describing the rejected projection.
        Nothing is actually enqueued when the projection is over budget.
        """
        projected_units = int(projected_units)
        over_budget = projected_units > self.budget
        admitted = 0 if over_budget else projected_units
        rejected = projected_units - admitted
        sr_id = f"SR-{ctx.run_id}-fine"
        state = "REJECTED_OVER_BUDGET" if over_budget else "ADMITTED"
        reason = f"fine-grain projection={projected_units} budget={self.budget}"
        with db.transaction(ctx.control):
            ctx.control.execute(
                "INSERT OR REPLACE INTO scheduler_run "
                "(scheduler_run_id, logical_run_date, creation_reason, state) VALUES (?, ?, ?, ?)",
                (sr_id, ctx.clock.now(), reason, state))
        return {
            "evidence_origin": EVIDENCE_ORIGIN,
            "projected_units": projected_units,
            "budget": self.budget,
            "over_budget": over_budget,
            "admitted_units": admitted,
            "rejected_units": rejected,
            "queue_depth": admitted,
            "grain_dims": dict(dims),
        }

    def run_coarse(self, ctx: RunContext, partitions: list[str]) -> dict[str, Any]:
        """Enqueue one coarse work-item per partition (recoverable, within budget)."""
        partitions = list(partitions)
        with db.transaction(ctx.control):
            ctx.control.execute(
                "DELETE FROM scheduler_run WHERE scheduler_run_id LIKE ?",
                (f"SR-{ctx.run_id}-coarse-%",))
            for idx, part in enumerate(sorted(partitions)):
                ctx.control.execute(
                    "INSERT OR REPLACE INTO scheduler_run "
                    "(scheduler_run_id, logical_run_date, creation_reason, state) "
                    "VALUES (?, ?, ?, 'COARSE_ADMITTED')",
                    (f"SR-{ctx.run_id}-coarse-{idx:03d}", ctx.clock.now(),
                     f"coarse partition {part}"))
        return {
            "evidence_origin": EVIDENCE_ORIGIN,
            "partitions": sorted(partitions),
            "admitted_units": len(partitions),
            "budget": self.budget,
            "queue_depth": len(partitions),
            "within_budget": len(partitions) <= self.budget,
        }
