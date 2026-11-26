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

import threading
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


class Scheduler:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._builds: dict[str, Build] = {}
        self._workspaces: dict[str, Workspace] = {}
        self._events: list[dict] = []
        self._seq = 0
        self._tick = 0
        self.reset(workspace_ids=["ws-1", "ws-2", "ws-3"])

    # ------------------------------------------------------------------ #
    # State management
    # ------------------------------------------------------------------ #
    def reset(self, workspace_ids: list[str]) -> None:
        with self._lock:
            self._builds.clear()
            self._workspaces.clear()
            self._events.clear()
            self._seq = 0
            self._tick = 0
            for wid in workspace_ids:
                self._workspaces[wid] = Workspace(id=wid)
            self._log("system", message=f"scheduler reset with {len(workspace_ids)} workspaces")

    def create_workspace(self, workspace_id: str) -> Workspace:
        with self._lock:
            if workspace_id in self._workspaces:
                raise SchedulerError(f"workspace {workspace_id!r} already exists")
            ws = Workspace(id=workspace_id)
            self._workspaces[workspace_id] = ws
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
            return build

    # ------------------------------------------------------------------ #
    # The single-executor advance
    # ------------------------------------------------------------------ #
    def tick(self) -> dict:
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
            self._log("step", **result)
            return result

    def advance(self, steps: int) -> list[dict]:
        if steps < 1:
            raise SchedulerError("steps must be >= 1")
        out = []
        for _ in range(steps):
            r = self.tick()
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
        if self._would_cycle(build, holder):
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
