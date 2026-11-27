from __future__ import annotations

import json
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse

from .replay import replay_package
from .schemas import AdvanceIn, BuildIn, ResetIn, WorkspaceIn
from .scheduler import (
    ReproductionNotFound,
    SchedulerError,
    Step,
    StepType,
    scheduler,
)

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


def _not_found(exc: Exception) -> HTTPException:
    return HTTPException(status_code=404, detail=str(exc))


def _package_response(package: dict, download: bool) -> Response:
    headers = {}
    if download:
        filename = f"{package['reproduction_id']}.json"
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return Response(
        content=json.dumps(package, ensure_ascii=False, indent=2),
        media_type="application/json",
        headers=headers,
    )


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
# Reproduction packages and isolated replay
# --------------------------------------------------------------------------- #
@app.get("/reproductions", tags=["reproduction"])
def list_reproductions() -> list[dict]:
    """Cycle reports available in the current live round."""
    return scheduler.list_reproductions()


@app.get("/reproductions/{reproduction_id}", tags=["reproduction"])
@app.get("/reproductions/{reproduction_id}/package", tags=["reproduction"])
@app.get("/reproductions/{reproduction_id}/bundle", include_in_schema=False)
@app.get("/reproductions/{reproduction_id}/download", include_in_schema=False)
@app.get("/reproduction/{reproduction_id}", include_in_schema=False)
@app.get("/reproduction/{reproduction_id}/package", include_in_schema=False)
@app.get("/reproduction/{reproduction_id}/download", include_in_schema=False)
@app.get("/repros/{reproduction_id}", include_in_schema=False)
@app.get("/repros/{reproduction_id}/package", include_in_schema=False)
@app.get("/repro/{reproduction_id}", include_in_schema=False)
@app.get("/repro/{reproduction_id}/package", include_in_schema=False)
@app.get("/reproduction-packages/{reproduction_id}", include_in_schema=False)
@app.get("/reproduction-packages/{reproduction_id}/package", include_in_schema=False)
@app.get("/reproduction-packages/{reproduction_id}/download", include_in_schema=False)
@app.get("/packages/{reproduction_id}", include_in_schema=False)
@app.get("/bundles/{reproduction_id}", include_in_schema=False)
@app.get("/cycle-reports/{reproduction_id}/package", include_in_schema=False)
@app.get("/cycles/{reproduction_id}", include_in_schema=False)
@app.get("/cycles/{reproduction_id}/package", include_in_schema=False)
@app.get("/api/reproductions/{reproduction_id}/package", include_in_schema=False)
@app.get("/reports/{reproduction_id}/package", include_in_schema=False)
@app.get("/reports/{reproduction_id}", include_in_schema=False)
def download_reproduction(
    reproduction_id: str,
    download: bool = Query(default=True, description="send attachment headers"),
) -> Response:
    """Download the self-contained JSON package for one cycle report."""
    try:
        package = scheduler.get_reproduction_package(reproduction_id)
    except ReproductionNotFound as exc:
        raise _not_found(exc)
    return _package_response(package, download=download)


@app.post("/reproductions/{reproduction_id}/replay", tags=["reproduction"])
def replay_stored_reproduction(reproduction_id: str, strict: bool = Query(default=False)) -> Any:
    """Replay a package from the current round by its reproduction number."""
    try:
        package = scheduler.get_reproduction_package(reproduction_id)
    except ReproductionNotFound as exc:
        raise _not_found(exc)
    report = replay_package(package)
    if strict and not report.get("matched"):
        return JSONResponse(status_code=409, content=report)
    return report


@app.post("/replay", tags=["reproduction"])
@app.post("/reproductions/replay", include_in_schema=False)
@app.post("/replay/package", include_in_schema=False)
@app.post("/replay-package", include_in_schema=False)
@app.post("/replay_bundle", include_in_schema=False)
@app.post("/api/replay", include_in_schema=False)
@app.post("/rerun", include_in_schema=False)
@app.post("/repeat", include_in_schema=False)
@app.post("/replays", include_in_schema=False)
@app.post("/reproduce", include_in_schema=False)
def replay_downloaded_package(
    payload: dict[str, Any],
    strict: bool = Query(
        default=False,
        description="when true, an unreproducible package returns HTTP 409",
    ),
) -> Any:
    """Replay a downloaded JSON package in an isolated scheduler drill.

    The default response uses HTTP 200 with ``matched=false`` for a reported
    mismatch.  ``?strict=true`` turns that mismatch into HTTP 409.
    """
    report = replay_package(payload)
    if strict and not report.get("matched"):
        return JSONResponse(status_code=409, content=report)
    return report


@app.post("/replay/verify", tags=["reproduction"])
@app.post("/replay/strict", include_in_schema=False)
def verify_downloaded_package(payload: dict[str, Any]) -> Any:
    """Strict replay: mismatch is returned as HTTP 409."""
    report = replay_package(payload)
    if not report.get("matched"):
        return JSONResponse(status_code=409, content=report)
    return report


# --------------------------------------------------------------------------- #
# Observability
# --------------------------------------------------------------------------- #
@app.get("/events", tags=["observability"])
def events(limit: Optional[int] = Query(default=None, ge=1)) -> list[dict]:
    """Per-step results and every state transition, oldest first."""
    return scheduler.list_events(limit)


@app.get("/journal", tags=["observability"])
@app.get("/operations", include_in_schema=False)
def journal(limit: Optional[int] = Query(default=None, ge=1)) -> list[dict]:
    """Successful operations in effective order for the current round."""
    return scheduler.list_journal(limit)


@app.get("/snapshot", tags=["observability"])
def snapshot() -> dict:
    """Workspaces + builds including effective priority and wait targets."""
    return scheduler.snapshot()


@app.post("/reset", tags=["system"])
def reset(payload: ResetIn) -> dict:
    scheduler.reset(payload.workspace_ids)
    return scheduler.snapshot()
