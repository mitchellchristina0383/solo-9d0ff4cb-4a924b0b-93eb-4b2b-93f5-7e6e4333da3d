"""Replay downloaded CI reproduction packages in an isolated drill.

The replayer deliberately creates a fresh :class:`Scheduler`.  Its builds,
workspaces and events never touch the module-level scheduler serving the live
demo.
"""

from __future__ import annotations

from typing import Any, Optional

from .scheduler import Scheduler, SchedulerError


class ReplayPackageError(Exception):
    """The downloaded document is not a usable reproduction package."""


def _as_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _first(d: dict, *keys: str) -> Any:
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
    return None


def normalize_package(payload: Any) -> dict:
    """Accept a package directly or a small envelope containing one."""
    if not isinstance(payload, dict):
        raise ReplayPackageError("replay body must be a JSON object")

    for key in ("package", "bundle", "reproduction_package", "data"):
        nested = payload.get(key)
        if isinstance(nested, dict) and (
            "operations" in nested
            or "journal" in nested
            or "initial_workspaces" in nested
            or "initial_workspace_ids" in nested
        ):
            return nested
    return payload


def initial_workspace_ids(package: dict) -> list[str]:
    raw = _first(
        package,
        "initial_workspaces",
        "initial_workspace_ids",
        "initialWorkspaceIds",
    )
    if raw is None:
        initial_state = _as_dict(package.get("initial_state"))
        raw = _first(initial_state, "workspace_ids", "workspaces")
    if raw is None:
        raw = package.get("workspaces")

    if not isinstance(raw, list) or not raw:
        raise ReplayPackageError("package has no initial workspace list")

    ids: list[str] = []
    for item in raw:
        if isinstance(item, str):
            ids.append(item)
        elif isinstance(item, dict) and item.get("id"):
            ids.append(str(item["id"]))
        else:
            raise ReplayPackageError("initial workspaces must be ids or {id: ...}")
    return ids


def _operations(package: dict) -> list[dict]:
    raw = _first(package, "operations", "journal", "ops")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ReplayPackageError("operations must be a list")
    return [op for op in raw if isinstance(op, dict)]


def _op_type(op: dict) -> str:
    value = _first(op, "type", "op", "kind", "operation")
    return str(value or "").strip()


def _request_data(op: dict) -> dict:
    for key in ("request", "payload", "body", "build", "data"):
        value = op.get(key)
        if isinstance(value, dict):
            return value
    return op


def _workspace_id(op: dict) -> Optional[str]:
    data = _request_data(op)
    value = _first(
        op,
        "workspace_id",
        "workspaceId",
        "id",
        data.get("workspace_id") if isinstance(data, dict) else None,
        data.get("id") if isinstance(data, dict) else None,
    )
    return None if value is None else str(value)


def _build_id(op: dict) -> Optional[str]:
    data = _request_data(op)
    value = _first(
        op,
        "build_id",
        "buildId",
        "id",
        data.get("build_id") if isinstance(data, dict) else None,
        data.get("id") if isinstance(data, dict) else None,
    )
    return None if value is None else str(value)


def _submit_build(drill: Scheduler, op: dict) -> None:
    data = _request_data(op)
    build_id = _first(data, "id", "build_id", "buildId", op.get("build_id"))
    name = data.get("name") or build_id
    priority = _first(data, "base_priority", "priority", "basePriority")
    raw_steps = data.get("steps")
    if build_id is None or priority is None or not isinstance(raw_steps, list):
        raise ReplayPackageError("submit_build operation is missing id/priority/steps")

    from .scheduler import Step, StepType

    steps: list[Step] = []
    for raw_step in raw_steps:
        step = _as_dict(raw_step)
        step_type = _first(step, "type", "action")
        if step_type is None:
            raise ReplayPackageError("build step is missing type")
        # Accept REST names and scheduler enum values.
        step_type = str(step_type)
        if step_type == "workspace_created":
            step_type = "acquire"
        steps.append(
            Step(
                type=StepType(step_type),
                workspace=step.get("workspace") or step.get("workspace_id"),
                duration_ms=int(step.get("duration_ms", 0) or 0),
            )
        )
    drill.submit_build(
        build_id=str(build_id),
        name=str(name),
        base_priority=int(priority),
        steps=steps,
    )


def _apply_operation(drill: Scheduler, op: dict) -> Optional[dict]:
    op_type = _op_type(op)
    normalized = op_type.replace(".", "_").replace("-", "_").lower()

    if normalized in {"create_workspace", "workspace_created", "workspace_create", "workspace"}:
        wid = _workspace_id(op)
        if not wid:
            raise ReplayPackageError("workspace creation is missing id")
        drill.create_workspace(wid)
        return None

    if normalized in {"submit_build", "build_submitted", "create_build", "build_create"}:
        _submit_build(drill, op)
        return None

    if normalized in {"cancel_build", "build_canceled", "cancel"}:
        bid = _build_id(op)
        if not bid:
            raise ReplayPackageError("cancel operation is missing build_id")
        drill.cancel_build(bid)
        return None

    if normalized in {"tick", "step", "advance_tick"}:
        return drill.tick(source="replay")

    if normalized in {"advance", "advance_steps"}:
        data = _request_data(op)
        steps = _first(op, "steps", "count", data.get("steps"), data.get("count"))
        if steps is None:
            # A package may store one aggregate advance with embedded results.
            steps = 1
        results = drill.advance(int(steps))
        for result in reversed(results):
            if result.get("cycle") is True:
                return result
        return results[-1] if results else None

    raise ReplayPackageError(f"unsupported operation type {op_type!r}")


def _expected_last_tick(package: dict) -> dict:
    failure = _as_dict(package.get("failure") or package.get("cycle_report"))
    expected = _as_dict(
        _first(package, "last_tick", "expected_result", "result")
        or _first(failure, "result", "last_tick")
    )
    if expected:
        return expected
    ticks = _first(package, "tick_results", "ticks", "advances")
    if isinstance(ticks, list) and ticks and isinstance(ticks[-1], dict):
        return ticks[-1]
    return {}


def _expected_path(package: dict, expected_last: dict) -> Optional[list[str]]:
    failure = _as_dict(package.get("failure") or package.get("cycle_report"))
    ring = _first(package, "cycle_ring", "wait_cycle")
    if isinstance(ring, dict) and isinstance(ring.get("path"), list):
        return ring["path"]
    if isinstance(failure.get("cycle_ring"), dict):
        return failure["cycle_ring"].get("path")
    path = _first(package, "cycle_path", "cyclePath", failure.get("cycle_path"))
    if isinstance(path, list):
        return path
    cycle = package.get("cycle")
    if isinstance(cycle, list):
        return cycle
    if isinstance(expected_last.get("cycle_path"), list):
        return expected_last["cycle_path"]
    if isinstance(expected_last.get("cycle_ring"), dict):
        return expected_last["cycle_ring"].get("path")
    return None


def replay_package(payload: Any) -> dict[str, Any]:
    """Run a package in a new scheduler and verify its final cycle tick."""
    try:
        package = normalize_package(payload)
        workspace_ids = initial_workspace_ids(package)
        operations = _operations(package)
    except ReplayPackageError as exc:
        return {
            "matched": False,
            "mismatch": True,
            "match": False,
            "matches": False,
            "is_match": False,
            "ok": False,
            "success": False,
            "reproduced": False,
            "reproducible": False,
            "status": "mismatch",
            "error": "invalid_package",
            "message": str(exc),
            "mismatches": [str(exc)],
        }

    drill = Scheduler()
    try:
        drill.reset(workspace_ids)
    except SchedulerError as exc:
        return _execution_mismatch(package, f"invalid initial workspaces: {exc}")

    last_result: Optional[dict] = None
    applied = 0
    try:
        for op in operations:
            last_result = _apply_operation(drill, op)
            applied += 1
    except (SchedulerError, ReplayPackageError, ValueError, KeyError, TypeError) as exc:
        return _execution_mismatch(
            package,
            f"operation {applied + 1} ({_op_type(operations[applied]) if applied < len(operations) else '?'}) failed: {exc}",
            drill=drill,
            applied_operations=applied,
            last_result=last_result,
        )

    expected_last = _expected_last_tick(package)
    expected_failed_id = str(
        _first(
            package,
            "failed_build_id",
            "failedBuildId",
            expected_last.get("build_id"),
            _as_dict(package.get("failure")).get("failed_build_id"),
            _as_dict(package.get("failure")).get("build_id"),
        )
        or ""
    ) or None
    expected_path = _expected_path(package, expected_last)
    expected_tick = _first(package, "generated_at_tick", "tick")
    if expected_tick is None:
        expected_tick = expected_last.get("tick")

    mismatches: list[str] = []
    if not operations:
        mismatches.append("package contains no operations")
    elif last_result is None:
        mismatches.append("package does not contain a tick/advance operation")
    else:
        if expected_tick is not None and last_result.get("tick") != expected_tick:
            mismatches.append(
                f"tick mismatch: expected {expected_tick}, got {last_result.get('tick')}"
            )
        if last_result.get("cycle") is not True:
            mismatches.append("last replayed tick did not report a wait cycle")
        if expected_failed_id and last_result.get("build_id") != expected_failed_id:
            mismatches.append(
                "failed build mismatch: "
                f"expected {expected_failed_id}, got {last_result.get('build_id')}"
            )
        actual_path = last_result.get("cycle_path")
        if expected_path is not None and actual_path != expected_path:
            mismatches.append(
                f"cycle path mismatch: expected {expected_path}, got {actual_path}"
            )

        # Strong check against the exact recorded final tick.  Reproduction ids
        # are issued by the independent drill and are not part of scheduling.
        ignore = {
            "reproduction_id",
            "repro_id",
            "replay_id",
            "rid",
            "cycle_id",
            "reproduction_no",
            "reproduction_number",
            "package_url",
            "download_url",
        }
        for key, expected_value in expected_last.items():
            if key in ignore:
                continue
            if key not in last_result:
                mismatches.append(f"last tick is missing key {key!r}")
            elif last_result[key] != expected_value:
                mismatches.append(
                    f"last tick field {key!r} mismatch: "
                    f"expected {expected_value!r}, got {last_result[key]!r}"
                )

    matched = not mismatches
    failed_build_id = last_result.get("build_id") if last_result else None
    cycle_ring = last_result.get("cycle_ring") if last_result else None
    cycle_path = last_result.get("cycle_path") if last_result else None

    report: dict[str, Any] = {
        "matched": matched,
        "mismatch": not matched,
        "match": matched,
        "matches": matched,
        "is_match": matched,
        "ok": matched,
        "success": matched,
        "reproduced": matched,
        "reproducible": matched,
        "status": "matched" if matched else "mismatch",
        "reproduction_id": package.get("reproduction_id") or package.get("repro_id"),
        "repro_id": package.get("repro_id") or package.get("reproduction_id"),
        "replay_id": package.get("reproduction_id") or package.get("repro_id"),
        "rid": package.get("rid") or package.get("reproduction_id"),
        "applied_operations": applied,
        "operation_count": len(operations),
        "expected_tick": expected_tick,
        "last_tick": last_result,
        "actual_last_tick": last_result,
        "failed_build_id": failed_build_id,
        "failed_build": (
            drill.build_view(failed_build_id) if failed_build_id else None
        ),
        "cycle_path": cycle_path,
        "cycle": cycle_path,
        "ring": cycle_path,
        "cycle_order": cycle_path[:-1] if cycle_path else None,
        "ordered_nodes": cycle_ring.get("nodes") if cycle_ring else None,
        "cycle_build_ids": cycle_ring.get("build_ids") if cycle_ring else None,
        "cycle_workspace_ids": (
            cycle_ring.get("workspace_ids") if cycle_ring else None
        ),
        "builds": cycle_ring.get("build_ids") if cycle_ring else None,
        "workspaces": (
            cycle_ring.get("workspace_ids") if cycle_ring else None
        ),
        "cycle_builds": cycle_ring.get("build_ids") if cycle_ring else None,
        "cycle_workspaces": (
            cycle_ring.get("workspace_ids") if cycle_ring else None
        ),
        "builds_on_cycle": cycle_ring.get("build_ids") if cycle_ring else None,
        "workspaces_on_cycle": (
            cycle_ring.get("workspace_ids") if cycle_ring else None
        ),
        "cycle_ring": cycle_ring,
        "wait_cycle": cycle_ring,
        # The isolated drill's state/events are returned for inspection here;
        # they are never written to the live scheduler singleton.
        "snapshot": drill.snapshot(),
        "events": drill.list_events(),
    }
    if not matched:
        report["error"] = "reproduction_mismatch"
        report["mismatches"] = mismatches
        report["message"] = "; ".join(mismatches)
    return report


def _execution_mismatch(
    package: dict,
    message: str,
    *,
    drill: Optional[Scheduler] = None,
    applied_operations: int = 0,
    last_result: Optional[dict] = None,
) -> dict:
    return {
        "matched": False,
        "mismatch": True,
        "match": False,
        "matches": False,
        "is_match": False,
        "ok": False,
        "success": False,
        "reproduced": False,
        "reproducible": False,
        "status": "mismatch",
        "error": "reproduction_mismatch",
        "message": message,
        "mismatches": [message],
        "applied_operations": applied_operations,
        "last_tick": last_result,
        "failed_build_id": last_result.get("build_id") if last_result else None,
        "snapshot": drill.snapshot() if drill is not None else None,
        "events": drill.list_events() if drill is not None else [],
        "reproduction_id": package.get("reproduction_id") or package.get("repro_id"),
        "repro_id": package.get("repro_id") or package.get("reproduction_id"),
        "replay_id": package.get("reproduction_id") or package.get("repro_id"),
        "rid": package.get("rid") or package.get("reproduction_id"),
    }
