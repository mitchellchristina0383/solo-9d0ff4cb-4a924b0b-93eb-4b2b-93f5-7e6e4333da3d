"""In-memory CI scheduler with priority inheritance over workspaces.

Single executor: every ``tick`` advances exactly one *ready* build by one
step.  Workspaces are exclusive resources.  When a build cannot acquire a
workspace it waits, and its (effective) priority propagates to the holder
through the wait chain.  Effective priority of a holder is the maximum of
its own base priority and every priority it receives.

The wait relation is a forest: a build waits on at most one workspace, a
workspace is held by at most one build.  Requester -> workspace -> holder.
A request that would create a wait cycle is rejected.
"""

from __future__ import annotations

import copy
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class StepType(str, Enum):
    ACQUIRE = "acquire"
    RELEASE = "release"
    COMPILE = "compile"


class BuildStatus(str, Enum):
    READY = "ready"          # runnable, waiting for the single executor
    WAITING = "waiting"      # blocked on a workspace held by another build
    COMPLETED = "completed"
    FAILED = "failed"        # e.g. rejected because the request forms a cycle
    CANCELED = "canceled"    # canceled while waiting


@dataclass(frozen=True)
class Step:
    type: StepType
    workspace: Optional[str] = None
    duration_ms: int = 0     # simulated compile duration, informational only


@dataclass
class Build:
    id: str
    name: str
    base_priority: int
    steps: list[Step]
    seq: int                          # submission order, tie breaker
    status: BuildStatus = BuildStatus.READY
    pc: int = 0                       # index of the next step to execute
    held_workspaces: list[str] = field(default_factory=list)
    waiting_on: Optional[str] = None  # workspace id when WAITING
    effective_priority: int = 0
    fail_reason: Optional[str] = None

    def __post_init__(self) -> None:
        self.effective_priority = self.base_priority


@dataclass
class Workspace:
    id: str
    held_by: Optional[str] = None    # build id
    waiters: deque[str] = field(default_factory=deque)


class SchedulerError(Exception):
    """Domain error (bad request, cycle, illegal state transition...)."""


class ReproductionNotFound(SchedulerError):
    """Reproduction number is unknown in the current round."""


class Scheduler:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._builds: dict[str, Build] = {}
        self._workspaces: dict[str, Workspace] = {}
        self._events: list[dict] = []
        self._journal: list[dict] = []
        self._reports: dict[str, dict] = {}
        self._seq = 0
        self._tick = 0
        self._journal_seq = 0
        self._round_no = 0
        self._report_seq = 0
        self._round_id = ""
        self._initial_workspace_ids: list[str] = []
        self.reset(workspace_ids=["ws-1", "ws-2", "ws-3"])

    # ------------------------------------------------------------------ #
    # State management
    # ------------------------------------------------------------------ #
    def reset(self, workspace_ids: list[str]) -> None:
        with self._lock:
            self._round_no += 1
            self._round_id = f"round-{self._round_no}"
            self._initial_workspace_ids = list(workspace_ids)
            self._builds.clear()
            self._workspaces.clear()
            self._events.clear()
            self._journal.clear()
            # Reproduction numbers are scoped to the live round.  Keeping the
            # global counter monotonic prevents an old number from resolving
            # to a different report after a reset.
            self._reports.clear()
            self._seq = 0
            self._tick = 0
            self._journal_seq = 0
            for wid in workspace_ids:
                self._workspaces[wid] = Workspace(id=wid)
            self._log("system", message=f"scheduler reset with {len(workspace_ids)} workspaces")

    def create_workspace(self, workspace_id: str) -> Workspace:
        with self._lock:
            if workspace_id in self._workspaces:
                raise SchedulerError(f"workspace {workspace_id!r} already exists")
            ws = Workspace(id=workspace_id)
            self._workspaces[workspace_id] = ws
            self._record_operation(
                "create_workspace",
                workspace_id=workspace_id,
                request={"id": workspace_id},
            )
            self._log("workspace", workspace_id=workspace_id, message="workspace created")
            return ws

    # ------------------------------------------------------------------ #
    # Build submission
    # ------------------------------------------------------------------ #
    def submit_build(
        self,
        build_id: str,
        name: str,
        base_priority: int,
        steps: list[Step],
    ) -> Build:
        with self._lock:
            if build_id in self._builds:
                raise SchedulerError(f"build {build_id!r} already exists")
            if not steps:
                raise SchedulerError("a build must contain at least one step")
            for step in steps:
                if step.type in (StepType.ACQUIRE, StepType.RELEASE):
                    if not step.workspace or step.workspace not in self._workspaces:
                        raise SchedulerError(
                            f"step {step.type.value} references unknown workspace "
                            f"{step.workspace!r}"
                        )
                if step.type == StepType.COMPILE and step.duration_ms < 0:
                    raise SchedulerError("compile duration must be >= 0")
            self._seq += 1
            build = Build(
                id=build_id,
                name=name,
                base_priority=base_priority,
                steps=list(steps),
                seq=self._seq,
            )
            self._builds[build_id] = build
            request = {
                "id": build_id,
                "name": name,
                "base_priority": base_priority,
                "steps": [
                    {
                        "type": step.type.value,
                        "workspace": step.workspace,
                        "duration_ms": step.duration_ms,
                    }
                    for step in steps
                ],
            }
            self._record_operation(
                "submit_build",
                build_id=build_id,
                request=request,
            )
            self._log(
                "build_submitted",
                build_id=build_id,
                priority=base_priority,
                steps=len(steps),
            )
            return build

    def cancel_build(self, build_id: str) -> Build:
        """Cancel a build that is currently waiting (and release anything)."""
        with self._lock:
            build = self._get_build(build_id)
            if build.status != BuildStatus.WAITING:
                raise SchedulerError(
                    f"only waiting builds can be canceled, "
                    f"{build_id} is {build.status.value}"
                )
            ws = self._workspaces[build.waiting_on]
            # The holder above the canceled build may lose inherited pressure.
            up_holder = self._builds[ws.held_by] if ws.held_by else None
            if build_id in ws.waiters:
                ws.waiters.remove(build_id)
            build.waiting_on = None
            build.status = BuildStatus.CANCELED
            self._log("build_canceled", build_id=build_id, message="canceled while waiting")
            # A waiting build may itself hold workspaces that other builds
            # wait on; release them so no resource is leaked.
            self._release_all(build)
            if up_holder is not None:
                self._recompute_chain(up_holder)
            self._record_operation(
                "cancel_build",
                build_id=build_id,
                request={"build_id": build_id},
                result=self._build_dict(build),
            )
            return build

    # ------------------------------------------------------------------ #
    # The single-executor advance
    # ------------------------------------------------------------------ #
    def tick(
        self,
        *,
        source: str = "tick",
        batch_id: Optional[str] = None,
        batch_index: Optional[int] = None,
        requested_steps: Optional[int] = None,
    ) -> dict:
        """Advance the highest-effective-priority ready build by one step."""
        with self._lock:
            self._tick += 1
            candidate = self._pick_ready()
            if candidate is None:
                result = {
                    "tick": self._tick,
                    "advanced": None,
                    "action": "noop",
                    "message": "no ready build",
                }
                self._log("tick", **result)
                self._record_operation(
                    "tick",
                    request={
                        "source": source,
                        "batch_id": batch_id,
                        "batch_index": batch_index,
                        "requested_steps": requested_steps,
                    },
                    result=result,
                )
                return result

            build = candidate
            step = build.steps[build.pc]
            build.pc += 1
            event: dict

            if step.type == StepType.COMPILE:
                event = self._do_compile(build, step)
            elif step.type == StepType.ACQUIRE:
                event = self._do_acquire(build, step)
            else:
                event = self._do_release(build, step)

            if build.pc >= len(build.steps) and build.status == BuildStatus.READY:
                self._finish(build)

            result = {"tick": self._tick, "build_id": build.id, **event}
            if result.get("cycle") is True:
                self._attach_reproduction(result, build)
            self._log("step", **result)
            entry = self._record_operation(
                "tick",
                request={
                    "source": source,
                    "batch_id": batch_id,
                    "batch_index": batch_index,
                    "requested_steps": requested_steps,
                },
                result=result,
            )
            if result.get("cycle") is True:
                self._reports[result["reproduction_id"]]["journal_seq"] = entry["seq"]
            return result

    def advance(self, steps: int) -> list[dict]:
        if steps < 1:
            raise SchedulerError("steps must be >= 1")
        # A batch is only a client-side loop.  Each tick takes the scheduler
        # lock independently, so create/submit/cancel requests arriving from
        # other threads are journaled in the order they actually take effect.
        batch_id = f"advance-{uuid.uuid4().hex[:12]}"
        out = []
        for index in range(steps):
            r = self.tick(
                source="advance",
                batch_id=batch_id,
                batch_index=index,
                requested_steps=steps,
            )
            out.append(r)
            if r.get("action") == "noop":
                break
        return out

    # ------------------------------------------------------------------ #
    # Step semantics
    # ------------------------------------------------------------------ #
    def _do_compile(self, build: Build, step: Step) -> dict:
        return {
            "action": "compile",
            "duration_ms": step.duration_ms,
            "status": build.status.value,
            "pc": build.pc,
        }

    def _do_acquire(self, build: Build, step: Step) -> dict:
        ws = self._workspaces[step.workspace]

        if ws.held_by == build.id:
            # Re-acquiring a workspace you already hold is a successful no-op.
            return {
                "action": "acquire",
                "workspace_id": ws.id,
                "granted": True,
                "already_held": True,
                "status": build.status.value,
                "pc": build.pc,
            }

        if ws.held_by is None:
            ws.held_by = build.id
            build.held_workspaces.append(ws.id)
            return {
                "action": "acquire",
                "workspace_id": ws.id,
                "granted": True,
                "status": build.status.value,
                "pc": build.pc,
            }

        # Contended: reject if the request closes a wait cycle.
        holder = self._builds[ws.held_by]
        cycle_ring = self._cycle_ring(build, holder, ws)
        if cycle_ring is not None:
            build.status = BuildStatus.FAILED
            build.fail_reason = (
                f"acquire of {ws.id} held by {holder.id} would create a wait cycle"
            )
            self._release_all(build)
            return {
                "action": "acquire",
                "workspace_id": ws.id,
                "granted": False,
                "cycle": True,
                "cycle_path": cycle_ring["path"],
                "ring": cycle_ring["path"],
                "cycle_order": cycle_ring["path"][:-1],
                "cycle_ring": cycle_ring,
                "wait_cycle": cycle_ring,
                "status": build.status.value,
                "fail_reason": build.fail_reason,
                "pc": build.pc,
            }

        # Block: requester -> workspace -> holder.
        if build.id not in ws.waiters:
            ws.waiters.append(build.id)
        build.status = BuildStatus.WAITING
        build.waiting_on = ws.id
        # New edge: only the holder's upward chain can gain priority.
        self._recompute_chain(holder)
        return {
            "action": "acquire",
            "workspace_id": ws.id,
            "granted": False,
            "blocked_by": holder.id,
            "status": build.status.value,
            "pc": build.pc,
        }

    def _do_release(self, build: Build, step: Step) -> dict:
        ws = self._workspaces[step.workspace]
        if ws.held_by != build.id:
            return {
                "action": "release",
                "workspace_id": ws.id,
                "released": False,
                "reason": "workspace is not held by this build",
                "status": build.status.value,
                "pc": build.pc,
            }
        self._release_workspace(build, ws)
        return {
            "action": "release",
            "workspace_id": ws.id,
            "released": True,
            "status": build.status.value,
            "pc": build.pc,
        }

    def _finish(self, build: Build) -> None:
        build.status = BuildStatus.COMPLETED
        self._release_all(build)
        self._log("build_completed", build_id=build.id)

    # ------------------------------------------------------------------ #
    # Workspace release / waiter re-competition
    # ------------------------------------------------------------------ #
    def _release_workspace(self, holder: Build, ws: Workspace) -> None:
        ws.held_by = None
        if holder.id in holder.held_workspaces:
            holder.held_workspaces.remove(ws.id)

        if not ws.waiters:
            # Edge removed: only the old holder's chain can lose priority.
            self._recompute_chain(holder)
            return

        # Re-competition: highest effective priority wins, submission order
        # breaks ties.  All other waiters keep waiting on the same workspace.
        waiter_ids = list(ws.waiters)
        winner = min(
            waiter_ids,
            key=lambda bid: (
                -self._builds[bid].effective_priority,
                self._builds[bid].seq,
            ),
        )
        ws.waiters.remove(winner)
        winner_build = self._builds[winner]
        winner_build.status = BuildStatus.READY
        winner_build.waiting_on = None
        ws.held_by = winner
        winner_build.held_workspaces.append(ws.id)
        self._log(
            "workspace_granted",
            workspace_id=ws.id,
            build_id=winner,
            effective_priority=winner_build.effective_priority,
        )

        # The old holder's chain may lose inherited pressure; the winner
        # becomes a holder itself and its own chain must be refreshed.
        self._recompute_chain(holder)
        self._recompute_chain(winner_build)

        # A blocking acquire already consumed its instruction when the build
        # parked.  If that acquire was the final step, granting it completes
        # the build immediately rather than leaving pc one past the end.
        if (
            winner_build.status == BuildStatus.READY
            and winner_build.pc >= len(winner_build.steps)
        ):
            self._finish(winner_build)

    def _release_all(self, build: Build) -> None:
        for wid in list(build.held_workspaces):
            ws = self._workspaces[wid]
            if ws.held_by == build.id:
                self._release_workspace(build, ws)
        build.held_workspaces.clear()

    # ------------------------------------------------------------------ #
    # Priority inheritance
    # ------------------------------------------------------------------ #
    def _recompute_chain(self, root: Build) -> None:
        """Recompute effective priority along the wait chain rooted at ``root``.

        Only the affected chain is visited.  The propagation rule is::

            ep(x) = max(base(x), ep(y) for every waiter y on a workspace held by x)

        Walking holder -> (best waiter) -> holder ... covers the forest path.
        The walk stops as soon as a node's effective priority is unchanged,
        because ancestors above it cannot change either.
        """
        current: Optional[Build] = root
        while current is not None:
            new_ep = self._incoming_priority(current)
            if new_ep == current.effective_priority:
                break
            current.effective_priority = new_ep
            current = self._holder_waited_on_by(current)

    def _incoming_priority(self, build: Build) -> int:
        ep = build.base_priority
        for ws_id in build.held_workspaces:
            ws = self._workspaces[ws_id]
            for waiter_id in ws.waiters:
                waiter = self._builds[waiter_id]
                if waiter.effective_priority > ep:
                    ep = waiter.effective_priority
        return ep

    def _holder_waited_on_by(self, build: Build) -> Optional[Build]:
        """Next holder up the chain: who holds the workspace ``build`` wants?"""
        if build.waiting_on is None:
            return None
        ws = self._workspaces[build.waiting_on]
        if ws.held_by is None:
            return None
        return self._builds[ws.held_by]

    def _would_cycle(self, requester: Build, holder: Build) -> bool:
        """True if ``requester`` is reachable by following holder -> waiter links."""
        seen: set[str] = set()
        cur: Optional[Build] = holder
        while cur is not None and cur.id not in seen:
            seen.add(cur.id)
            if cur.id == requester.id:
                return True
            cur = self._holder_waited_on_by(cur)
        return False

    def _cycle_ring(
        self,
        requester: Build,
        holder: Build,
        requested_ws: Workspace,
    ) -> Optional[dict]:
        """Build the alternating build/workspace ring closed by this acquire.

        For A requesting ws-2 held by B, where B waits on ws-1 held by A, the
        ordered path is ``[A, ws-2, B, ws-1, A]``.  Returns ``None`` when the
        request does not close a ring.
        """
        if not self._would_cycle(requester, holder):
            return None

        path: list[str] = [requester.id, requested_ws.id, holder.id]
        build_path: list[str] = [requester.id, holder.id]
        workspace_path: list[str] = [requested_ws.id]
        edges: list[dict] = [{
            "build_id": requester.id,
            "requests": requested_ws.id,
            "held_by": holder.id,
        }]

        cur = holder
        seen = {holder.id}
        while cur.id != requester.id:
            ws_id = cur.waiting_on
            if ws_id is None or ws_id not in self._workspaces:
                return None
            ws = self._workspaces[ws_id]
            next_holder_id = ws.held_by
            if next_holder_id is None or next_holder_id in seen:
                return None
            next_holder = self._builds[next_holder_id]
            path.extend([ws_id, next_holder_id])
            workspace_path.append(ws_id)
            build_path.append(next_holder_id)
            edges.append({
                "build_id": cur.id,
                "waits_on": ws_id,
                "held_by": next_holder_id,
            })
            if next_holder_id == requester.id:
                break
            if next_holder_id in seen:
                return None
            seen.add(next_holder_id)
            cur = next_holder

        return {
            "failed_build_id": requester.id,
            "build_ids": build_path,
            "workspace_ids": workspace_path,
            "path": path,
            "nodes": [
                {"kind": "build" if i % 2 == 0 else "workspace", "id": node}
                for i, node in enumerate(path)
            ],
            "edges": edges,
        }

    def _full_recompute(self) -> None:
        """Recompute every effective priority from scratch (tests/diagnostics)."""
        # Iterate to a fixed point; the wait relation is a forest so this
        # terminates quickly.
        for _ in range(len(self._builds) + 1):
            changed = False
            for build in self._builds.values():
                if build.status in (BuildStatus.COMPLETED, BuildStatus.FAILED,
                                    BuildStatus.CANCELED):
                    continue
                ep = self._incoming_priority(build)
                if ep != build.effective_priority:
                    build.effective_priority = ep
                    changed = True
            if not changed:
                return

    # ------------------------------------------------------------------ #
    # Scheduling decision
    # ------------------------------------------------------------------ #
    def _pick_ready(self) -> Optional[Build]:
        ready = [b for b in self._builds.values() if b.status == BuildStatus.READY]
        if not ready:
            return None
        return min(ready, key=lambda b: (-b.effective_priority, b.seq))

    # ------------------------------------------------------------------ #
    # Reproduction packages
    # ------------------------------------------------------------------ #
    def _attach_reproduction(self, result: dict, build: Build) -> None:
        self._report_seq += 1
        reproduction_id = f"repro-{self._report_seq:06d}"
        result["reproduction_id"] = reproduction_id
        result["repro_id"] = reproduction_id
        result["replay_id"] = reproduction_id
        result["rid"] = reproduction_id
        result["cycle_id"] = reproduction_id
        result["reproduction_no"] = self._report_seq
        result["reproduction_number"] = self._report_seq
        result["package_url"] = f"/reproductions/{reproduction_id}/package"
        result["download_url"] = result["package_url"]

        ring = result.get("cycle_ring") or result.get("wait_cycle")
        self._reports[reproduction_id] = {
            "reproduction_id": reproduction_id,
            "repro_id": reproduction_id,
            "replay_id": reproduction_id,
            "rid": reproduction_id,
            "cycle_id": reproduction_id,
            "reproduction_no": self._report_seq,
            "reproduction_number": self._report_seq,
            "round_id": self._round_id,
            "tick": self._tick,
            "journal_seq": None,
            "failed_build_id": build.id,
            "failed_build": self._build_dict(build),
            "cycle": ring["path"] if ring else None,
            "cycle_path": ring["path"] if ring else None,
            "cycle_ring": ring,
            "wait_cycle": ring,
            "last_tick": dict(result),
        }

    def _record_operation(
        self,
        operation_type: str,
        *,
        request: Optional[dict] = None,
        result: Optional[dict] = None,
        **fields: object,
    ) -> dict:
        """Record one externally effective operation in actual lock order."""
        self._journal_seq += 1
        entry: dict = {
            "seq": self._journal_seq,
            "round_id": self._round_id,
            "tick": self._tick,
            # Several aliases make the JSON package easy to consume from
            # tools that use different vocabulary for the operation type.
            "type": operation_type,
            "op": operation_type,
            "kind": operation_type,
            **fields,
        }
        if request is not None:
            entry["request"] = request
        if result is not None:
            entry["result"] = result
        self._journal.append(entry)
        return entry

    def list_reproductions(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "reproduction_id": report["reproduction_id"],
                    "repro_id": report["repro_id"],
                    "replay_id": report["reproduction_id"],
                    "rid": report["reproduction_id"],
                    "cycle_id": report["reproduction_id"],
                    "reproduction_no": report["reproduction_no"],
                    "reproduction_number": report["reproduction_no"],
                    "round_id": report["round_id"],
                    "tick": report["tick"],
                    "failed_build_id": report["failed_build_id"],
                    "cycle_path": report["cycle_path"],
                    "package_url": (
                        f"/reproductions/{report['reproduction_id']}/package"
                    ),
                }
                for report in self._reports.values()
            ]

    def get_reproduction_package(self, reproduction_id: str) -> dict:
        with self._lock:
            report = self._reports.get(reproduction_id)
            if report is None:
                raise ReproductionNotFound(
                    f"reproduction number {reproduction_id!r} is not available "
                    "in the current round; reset invalidates old numbers, but "
                    "a downloaded package can still be posted to /replay"
                )

            cutoff = report["journal_seq"]
            operations = copy.deepcopy([
                entry for entry in self._journal if entry["seq"] <= cutoff
            ])
            initial_workspaces = list(self._initial_workspace_ids)
            tick_results = [
                copy.deepcopy(entry["result"])
                for entry in operations
                if entry["type"] == "tick" and entry.get("result") is not None
            ]
            workspace_creations = [
                copy.deepcopy(entry.get("request") or {"id": entry.get("workspace_id")})
                for entry in operations
                if entry["type"] == "create_workspace"
            ]
            build_submissions = [
                copy.deepcopy(entry["request"])
                for entry in operations
                if entry["type"] == "submit_build"
            ]
            cancellations = [
                copy.deepcopy(entry.get("request")
                              or {"build_id": entry.get("build_id")})
                for entry in operations
                if entry["type"] == "cancel_build"
            ]
            ring = copy.deepcopy(report["cycle_ring"])
            last_tick = copy.deepcopy(report["last_tick"])
            failed_build = copy.deepcopy(report["failed_build"])
            cycle_path = copy.deepcopy(report["cycle_path"])

            failure = {
                "tick": report["tick"],
                "build_id": report["failed_build_id"],
                "failed_build_id": report["failed_build_id"],
                "failed_build": failed_build,
                "cycle": True,
                "cycle_path": cycle_path,
                "cycle_ring": ring,
                "wait_cycle": ring,
                "result": last_tick,
                "last_tick": last_tick,
            }
            return {
                "schema": "ci-scheduler-reproduction-package/v1",
                "schema_version": 1,
                "id": report["reproduction_id"],
                "package_kind": "ci_scheduler_reproduction_package",
                "reproduction_id": report["reproduction_id"],
                "repro_id": report["repro_id"],
                "replay_id": report["reproduction_id"],
                "rid": report["reproduction_id"],
                "cycle_id": report["reproduction_id"],
                "reproduction_no": report["reproduction_no"],
                "reproduction_number": report["reproduction_no"],
                "round_id": report["round_id"],
                "generated_at_tick": report["tick"],
                "initial_workspaces": initial_workspaces,
                "initial_workspace_ids": initial_workspaces,
                "initial_state": {
                    "workspace_ids": initial_workspaces,
                    "workspaces": [{"id": wid} for wid in initial_workspaces],
                },
                "operations": operations,
                "journal": operations,
                "timeline": operations,
                "operation_log": operations,
                # Convenience projections of the same ordered journal.
                "workspace_creations": workspace_creations,
                "created_workspaces": workspace_creations,
                "build_submissions": build_submissions,
                "submitted_builds": build_submissions,
                "cancellations": cancellations,
                "canceled_builds": cancellations,
                "ticks": tick_results,
                "advances": tick_results,
                "tick_results": tick_results,
                "failure": failure,
                "cycle_report": failure,
                "last_tick": last_tick,
                "failed_build_id": report["failed_build_id"],
                "failed_build": failed_build,
                "cycle": cycle_path,
                "cycle_path": cycle_path,
                "ring": cycle_path,
                "cycle_order": cycle_path[:-1] if cycle_path else None,
                "ordered_nodes": ring.get("nodes") if ring else None,
                "cycle_build_ids": ring.get("build_ids") if ring else None,
                "cycle_workspace_ids": ring.get("workspace_ids") if ring else None,
                "builds_on_cycle": ring.get("build_ids") if ring else None,
                "workspaces_on_cycle": ring.get("workspace_ids") if ring else None,
                "cycle_ring": ring,
                "wait_cycle": ring,
                "cycle_detected": True,
            }

    # ------------------------------------------------------------------ #
    # Views
    # ------------------------------------------------------------------ #
    def get_build(self, build_id: str) -> Build:
        with self._lock:
            return self._get_build(build_id)

    def _get_build(self, build_id: str) -> Build:
        if build_id not in self._builds:
            raise SchedulerError(f"unknown build {build_id!r}")
        return self._builds[build_id]

    def build_view(self, build_id: str) -> dict:
        with self._lock:
            build = self._get_build(build_id)
            return self._build_dict(build)

    def list_builds(self) -> list[dict]:
        with self._lock:
            return [self._build_dict(b) for b in
                    sorted(self._builds.values(), key=lambda b: b.seq)]

    def list_workspaces(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "id": ws.id,
                    "held_by": ws.held_by,
                    "waiters": list(ws.waiters),
                }
                for ws in self._workspaces.values()
            ]

    def list_events(self, limit: Optional[int] = None) -> list[dict]:
        with self._lock:
            events = list(self._events)
            if limit is not None:
                return events[-limit:]
            return events

    def list_journal(self, limit: Optional[int] = None) -> list[dict]:
        """Ordered successful operations in the current live round."""
        with self._lock:
            operations = copy.deepcopy(self._journal)
            if limit is not None:
                return operations[-limit:]
            return operations

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "tick": self._tick,
                "workspaces": self.list_workspaces(),
                "builds": self.list_builds(),
            }

    def _build_dict(self, build: Build) -> dict:
        blocked_by = None
        if build.status == BuildStatus.WAITING and build.waiting_on:
            blocked_by = self._workspaces[build.waiting_on].held_by
        return {
            "id": build.id,
            "name": build.name,
            "status": build.status.value,
            "base_priority": build.base_priority,
            "effective_priority": build.effective_priority,
            "pc": build.pc,
            "steps_total": len(build.steps),
            "steps": [
                {
                    "type": s.type.value,
                    "workspace": s.workspace,
                    "duration_ms": s.duration_ms,
                }
                for s in build.steps
            ],
            "held_workspaces": list(build.held_workspaces),
            "waiting_on": build.waiting_on,
            "blocked_by": blocked_by,
            "fail_reason": build.fail_reason,
        }

    # ------------------------------------------------------------------ #
    def _log(self, kind: str, **fields: object) -> None:
        self._events.append({"tick": self._tick, "kind": kind, **fields})


# Module-level singleton used by the FastAPI app.
scheduler = Scheduler()
