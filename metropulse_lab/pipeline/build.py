"""Pipeline orchestration: receive -> stage -> model -> publish.

A single ``build_all`` call runs the full pipeline under whatever fault profile
is currently armed. ``create`` runs it clean; ``run`` runs it with the incident's
fault armed; ``recover`` reuses the stage functions for a bounded rebuild.
"""

from __future__ import annotations

from typing import Any

from ..context import RunContext
from . import receive, stage, model, publish


def publication_id_for(ctx: RunContext, tag: str) -> str:
    return f"PUB-{ctx.run_id}-{tag}"


def build_all(ctx: RunContext, tag: str) -> dict[str, Any]:
    publication_id = publication_id_for(ctx, tag)
    receive_summary = receive.receive(ctx)
    stage_summary = stage.stage(ctx)
    model_summary = model.model(ctx, publication_id)
    publish_summary = publish.publish(ctx, publication_id)
    return {
        "publication_id": publication_id,
        "receive": receive_summary,
        "stage": stage_summary,
        "model": model_summary,
        "publish": publish_summary,
    }
