from __future__ import annotations

from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from .schemas import AdvanceIn, BuildIn, ResetIn, WorkspaceIn
from .scheduler import SchedulerError, Step, StepType, scheduler

app = FastAPI(
    title="CI Scheduler Drill — single executor with priority inheritance",
    version="1.0.0",
    description=(
        "Submit builds made of acquire/release/compile steps. Each tick the "
        "ready build with the highest **effective** priority advances one "
        "step. Waiters propagate priority to workspace holders; a request "
        "that would form a wait cycle fails."
    ),
)


def _error(exc: Exception) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


# --------------------------------------------------------------------------- #
# Workspaces
# --------------------------------------------------------------------------- #
@app.post("/workspaces", status_code=201, tags=["workspaces"])
def create_workspace(payload: WorkspaceIn) -> dict:
    try:
        ws = scheduler.create_workspace(payload.id)
    except SchedulerError as exc:
        raise _error(exc)
    return {"id": ws.id, "held_by": ws.held_by, "waiters": list(ws.waiters)}


@app.get("/workspaces", tags=["workspaces"])
def list_workspaces() -> list[dict]:
    return scheduler.list_workspaces()


# --------------------------------------------------------------------------- #
# Builds
# --------------------------------------------------------------------------- #
@app.post("/builds", status_code=201, tags=["builds"])
def submit_build(payload: BuildIn) -> dict:
    steps = [
        Step(
            type=StepType(s.type),
            workspace=s.workspace,
            duration_ms=s.duration_ms,
        )
        for s in payload.steps
    ]
    try:
        scheduler.submit_build(payload.id, payload.name, payload.base_priority, steps)
    except SchedulerError as exc:
        raise _error(exc)
    return scheduler.build_view(payload.id)


@app.get("/builds", tags=["builds"])
def list_builds() -> list[dict]:
    return scheduler.list_builds()


@app.get("/builds/{build_id}", tags=["builds"])
def get_build(build_id: str) -> dict:
    try:
        return scheduler.build_view(build_id)
    except SchedulerError as exc:
        raise _error(exc)


@app.post("/builds/{build_id}/cancel", tags=["builds"])
def cancel_build(build_id: str) -> dict:
    try:
        scheduler.cancel_build(build_id)
    except SchedulerError as exc:
        raise _error(exc)
    return scheduler.build_view(build_id)


# --------------------------------------------------------------------------- #
# Scheduling
# --------------------------------------------------------------------------- #
@app.post("/tick", tags=["scheduler"])
def tick_once() -> dict:
    """Advance exactly one ready build by one step (single executor)."""
    return scheduler.tick()


@app.post("/advance", tags=["scheduler"])
def advance(payload: AdvanceIn) -> list[dict]:
    """Advance up to ``steps`` ticks; stops early when nothing is ready."""
    return scheduler.advance(payload.steps)


# --------------------------------------------------------------------------- #
# Observability
# --------------------------------------------------------------------------- #
@app.get("/events", tags=["observability"])
def events(limit: Optional[int] = Query(default=None, ge=1)) -> list[dict]:
    """Per-step results and every state transition, oldest first."""
    return scheduler.list_events(limit)


@app.get("/snapshot", tags=["observability"])
def snapshot() -> dict:
    """Workspaces + builds including effective priority and wait targets."""
    return scheduler.snapshot()


@app.post("/reset", tags=["system"])
def reset(payload: ResetIn) -> dict:
    scheduler.reset(payload.workspace_ids)
    return scheduler.snapshot()
