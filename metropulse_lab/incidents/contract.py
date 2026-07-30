"""Incident contract: metadata and the common lifecycle protocol.

Every ``mp_XX`` module exposes an ``Incident`` object that implements this
protocol (lab_architecture.md section 7). One primary root cause per incident.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ..context import RunContext
from ..reports import StepReport


@dataclass(frozen=True)
class IncidentMetadata:
    incident_id: str
    family: str
    reader_title: str  # symptom only; never names the root cause
    primary_invariant_id: str
    core_evidence_kind: str  # REAL_SQLITE | DETERMINISTIC_SIMULATOR | MIXED_LABELED
    recovery_mode: str  # REPLAY | ROLLBACK | NO_REPLAY
    fault_operations: tuple[str, ...]
    expected_detection_ids: tuple[str, ...]
    transfer_mappings: tuple[str, ...]
    nearest_old_scenario: str
    old_scenario_difference: str
    # Tables the fault + recovery are allowed to change. The reference lifecycle
    # asserts every other output table's domain content is unchanged.
    affected_tables: tuple[str, ...] = ()
    params: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Incident(Protocol):
    metadata: IncidentMetadata

    def build_baseline(self, ctx: RunContext) -> None:
        """Optional hook run by ``create`` after the shared baseline pipeline.

        Self-contained incidents use this to materialize their own CLEAN scenario
        (extra source families, control state, or incident-private tables) so
        ``baseline_ok`` can verify a healthy starting point before ``inject``.
        A no-op is fine for incidents whose scenario is the standard baseline.
        """
        ...

    def baseline_ok(self, ctx: RunContext) -> bool:
        """True when the clean baseline satisfies this incident's invariant."""
        ...

    def inject(self, ctx: RunContext) -> StepReport: ...
    def run_faulty(self, ctx: RunContext) -> StepReport: ...
    def detect(self, ctx: RunContext) -> StepReport: ...
    def collect_evidence(self, ctx: RunContext) -> StepReport: ...
    def contain(self, ctx: RunContext) -> StepReport: ...
    def repair(self, ctx: RunContext) -> StepReport: ...
    def recover(self, ctx: RunContext) -> StepReport: ...
    def verify(self, ctx: RunContext) -> StepReport: ...
