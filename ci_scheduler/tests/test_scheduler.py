"""End-to-end tests through the FastAPI surface (single executor,
priority inheritance, multi-level waits, cycles, cancellation)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.scheduler import BuildStatus, scheduler


@pytest.fixture()
def client() -> TestClient:
    scheduler.reset(["ws-1", "ws-2", "ws-3"])
    return TestClient(app)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _compile(ms: int = 100) -> dict:
    return {"type": "compile", "duration_ms": ms}


def _acquire(ws: str) -> dict:
    return {"type": "acquire", "workspace": ws}


def _release(ws: str) -> dict:
    return {"type": "release", "workspace": ws}


def _submit(client: TestClient, build_id: str, priority: int, steps: list[dict],
            name: str | None = None) -> dict:
    r = client.post("/builds", json={
        "id": build_id,
        "name": name or build_id,
        "base_priority": priority,
        "steps": steps,
    })
    assert r.status_code == 201, r.text
    return r.json()


def _build(client: TestClient, build_id: str) -> dict:
    return client.get(f"/builds/{build_id}").json()


def _tick(client: TestClient) -> dict:
    return client.post("/tick").json()


def _run_until(client: TestClient, build_id: str, status: str,
               max_ticks: int = 100) -> list[dict]:
    """Tick until ``build_id`` reaches ``status`` (or no build is ready)."""
    results = []
    for _ in range(max_ticks):
        if _build(client, build_id)["status"] == status:
            return results
        r = _tick(client)
        results.append(r)
        if r.get("action") == "noop":
            break
    assert _build(client, build_id)["status"] == status, (
        f"{build_id} never reached {status}; last results: {results[-3:]}"
    )
    return results


def _run_until_holds(client: TestClient, build_id: str, workspace: str) -> None:
    for _ in range(100):
        if workspace in _build(client, build_id)["held_workspaces"]:
            return
        r = _tick(client)
        assert r.get("action") != "noop", f"{build_id} never acquired {workspace}"
    raise AssertionError(f"{build_id} never acquired {workspace}")


def _run_until_noop(client: TestClient, max_ticks: int = 200) -> list[dict]:
    results = []
    for _ in range(max_ticks):
        r = _tick(client)
        if r.get("action") == "noop":
            return results
        results.append(r)
    raise AssertionError("scheduler never idled")


# --------------------------------------------------------------------------- #
# Scheduling basics
# --------------------------------------------------------------------------- #
def test_highest_effective_priority_ready_build_runs_first(client: TestClient) -> None:
    _submit(client, "low", 1, [_compile(10), _compile(10)])
    _submit(client, "high", 10, [_compile(5)])

    r = _tick(client)
    assert r["build_id"] == "high" and r["action"] == "compile"
    r = _tick(client)
    assert r["build_id"] == "low"


def test_single_executor_advances_one_step_per_tick(client: TestClient) -> None:
    _submit(client, "b", 5, [_compile(1), _compile(1), _compile(1)])
    for expected_pc in (1, 2, 3):
        r = _tick(client)
        assert r["build_id"] == "b" and r["pc"] == expected_pc
    assert _build(client, "b")["status"] == BuildStatus.COMPLETED.value
    assert _tick(client)["action"] == "noop"


def test_tie_breaks_by_submission_order(client: TestClient) -> None:
    _submit(client, "first", 5, [_compile(1)])
    _submit(client, "second", 5, [_compile(1)])
    assert _tick(client)["build_id"] == "first"
    assert _tick(client)["build_id"] == "second"


# --------------------------------------------------------------------------- #
# Priority inheritance
# --------------------------------------------------------------------------- #
def test_basic_priority_inheritance_on_blocked_acquire(client: TestClient) -> None:
    # Phase 1: only A is present, so it acquires ws-1 and parks there.
    _submit(client, "A", 2, [
        _acquire("ws-1"), _compile(10), _compile(10), _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")

    # Phase 2: high-priority B blocks on ws-1; ready competitor C sits at 5.
    _submit(client, "B", 9, [_acquire("ws-1"), _compile(10), _release("ws-1")])
    _submit(client, "C", 5, [_compile(10)])
    _run_until(client, "B", BuildStatus.WAITING.value)

    a, b = _build(client, "A"), _build(client, "B")
    assert b["waiting_on"] == "ws-1" and b["blocked_by"] == "A"
    assert a["effective_priority"] == 9 and a["base_priority"] == 2

    # A (ep 9) must be picked over ready C (base/effective 5).
    r = _tick(client)
    assert r["build_id"] == "A" and r["action"] == "compile"


def test_holder_takes_max_of_own_and_received_priority(client: TestClient) -> None:
    # A's base priority is 5. Waiters outrank A so their acquires are
    # scheduled immediately; the first waiter (6) only lifts A slightly,
    # proving effective = max(base, incoming) rather than blind assignment.
    _submit(client, "A", 5, [
        _acquire("ws-1"),
        _compile(1), _compile(1), _compile(1), _compile(1),
        _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")

    _submit(client, "mid-waiter", 6, [
        _acquire("ws-1"), _compile(1), _release("ws-1"),
    ])
    _run_until(client, "mid-waiter", BuildStatus.WAITING.value)
    assert _build(client, "A")["effective_priority"] == 6
    assert _build(client, "A")["base_priority"] == 5

    _submit(client, "hi-waiter", 8, [
        _acquire("ws-1"), _compile(1), _release("ws-1"),
    ])
    _run_until(client, "hi-waiter", BuildStatus.WAITING.value)
    # incoming 8 replaces the weaker pressure 6
    assert _build(client, "A")["effective_priority"] == 8

    # and when the strongest waiter disappears, pressure falls back to 6
    client.post("/builds/hi-waiter/cancel")
    assert _build(client, "A")["effective_priority"] == 6


def test_multi_level_wait_chain_inherits_transitively(client: TestClient) -> None:
    """Two-hop chain F(7) -> ws-2 -> B(4) -> ws-1 -> A(1).

    Priority must propagate both hops: B.eff = A.eff = 7.  A ready
    competitor N(6) proves A only wins the executor because of inheritance.
    """
    scheduler.reset(["ws-1", "ws-2"])

    # A grabs ws-1 and parks in compile padding.
    _submit(client, "A", 1, [
        _acquire("ws-1"),
        _compile(1), _compile(1), _compile(1), _compile(1), _compile(1),
        _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")

    # B grabs ws-2 (free) and then blocks on ws-1 held by A.
    _submit(client, "B", 4, [
        _acquire("ws-2"), _acquire("ws-1"),
        _compile(1), _release("ws-1"), _release("ws-2"),
    ])
    _run_until_holds(client, "B", "ws-2")
    _run_until(client, "B", BuildStatus.WAITING.value)
    b = _build(client, "B")
    assert b["waiting_on"] == "ws-1" and b["blocked_by"] == "A"
    assert b["held_workspaces"] == ["ws-2"]
    # A already feels B's base priority 4.
    assert _build(client, "A")["effective_priority"] == 4

    # F blocks on ws-2 held by B -> two-hop propagation.
    _submit(client, "F", 7, [_acquire("ws-2"), _compile(1), _release("ws-2")])
    _run_until(client, "F", BuildStatus.WAITING.value)

    a, b, f = _build(client, "A"), _build(client, "B"), _build(client, "F")
    assert f["blocked_by"] == "B"
    assert b["effective_priority"] == 7
    assert a["effective_priority"] == 7

    # Ready competitor N at base 6: A (ep 7) must still go first.
    _submit(client, "N", 6, [_compile(1), _compile(1)])
    assert _tick(client)["build_id"] == "A"

    # Drain A until it releases ws-1; B is granted and becomes ready while
    # still inheriting F's 7 through ws-2.
    _run_until(client, "B", BuildStatus.READY.value)
    b = _build(client, "B")
    assert set(b["held_workspaces"]) == {"ws-1", "ws-2"}
    assert b["effective_priority"] == 7

    # B (ep 7) still outruns N (6).
    assert _tick(client)["build_id"] == "B"

    # Let everything finish; F is unblocked and completes too.
    _run_until_noop(client)
    assert _build(client, "B")["status"] == BuildStatus.COMPLETED.value
    assert _build(client, "F")["status"] == BuildStatus.COMPLETED.value
    for ws in client.get("/workspaces").json():
        assert ws["held_by"] is None and ws["waiters"] == []


def test_priority_drops_when_waiting_chain_disappears(client: TestClient) -> None:
    _submit(client, "A", 1, [
        _acquire("ws-1"), _compile(1), _compile(1), _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")
    # L queues first so it actually reaches the wait queue despite base 4 > 1.
    _submit(client, "L", 4, [_acquire("ws-1"), _compile(1), _release("ws-1")])
    _run_until(client, "L", BuildStatus.WAITING.value)
    _submit(client, "H", 9, [_acquire("ws-1"), _compile(1), _release("ws-1")])
    _run_until(client, "H", BuildStatus.WAITING.value)
    assert _build(client, "A")["effective_priority"] == 9

    # H cancels: A's inherited pressure falls back to L's 4.
    assert client.post("/builds/H/cancel").status_code == 200
    assert _build(client, "H")["status"] == BuildStatus.CANCELED.value
    assert _build(client, "A")["effective_priority"] == 4

    # A releases: L wins the workspace and finishes.
    _run_until_noop(client)
    assert _build(client, "L")["status"] == BuildStatus.COMPLETED.value
    ws1 = next(w for w in client.get("/workspaces").json() if w["id"] == "ws-1")
    assert ws1["held_by"] is None and ws1["waiters"] == []


# --------------------------------------------------------------------------- #
# Cycles
# --------------------------------------------------------------------------- #
def test_cycle_request_makes_requester_fail(client: TestClient) -> None:
    """Cross hold: A holds ws-1, B holds ws-2 and blocks on ws-1; A then asks
    for ws-2 and is the requester that closes the ring, so A fails and
    releases ws-1, which unblocks B."""
    # Phase 1: A alone takes ws-1 and parks behind compile padding.
    _submit(client, "A", 1, [
        _acquire("ws-1"),
        _compile(1), _compile(1), _compile(1),
        _acquire("ws-2"),
        _compile(10), _release("ws-2"), _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")

    # Phase 2: B alone takes ws-2; its next step wants ws-1 held by A.
    _submit(client, "B", 8, [
        _acquire("ws-2"), _acquire("ws-1"),
        _compile(10), _release("ws-1"), _release("ws-2"),
    ])
    _run_until_holds(client, "B", "ws-2")
    _run_until(client, "B", BuildStatus.WAITING.value)
    b = _build(client, "B")
    assert b["waiting_on"] == "ws-1" and b["blocked_by"] == "A"
    # A already inherits B's priority 8.
    assert _build(client, "A")["effective_priority"] == 8

    # Phase 3: A burns its padding, then asks ws-2 held by B -> it closes the
    # A -> B -> A ring and must fail.
    r_events = _run_until(client, "A", BuildStatus.FAILED.value)
    last = r_events[-1]
    assert last["action"] == "acquire" and last["granted"] is False
    assert last["cycle"] is True
    a = _build(client, "A")
    assert "cycle" in a["fail_reason"]

    # A released ws-1 on failure: waiting B is granted and runs to completion.
    _run_until(client, "B", BuildStatus.READY.value)
    assert "ws-1" in _build(client, "B")["held_workspaces"]
    _run_until_noop(client)
    assert _build(client, "B")["status"] == BuildStatus.COMPLETED.value
    for ws in client.get("/workspaces").json():
        assert ws["held_by"] is None and ws["waiters"] == []


def test_three_node_cycle_is_detected(client: TestClient) -> None:
    """Ring A -> ws-2 -> B -> ws-3 -> C -> ws-1 -> A, assembled one holder at
    a time. A is the requester that closes the ring, so A fails."""
    scheduler.reset(["ws-1", "ws-2", "ws-3"])

    # Phase 1: A alone takes ws-1 and parks there.
    _submit(client, "A", 1, [
        _acquire("ws-1"),
        _compile(1), _compile(1),
        _acquire("ws-2"),
        _compile(1), _release("ws-2"), _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")

    # Phase 2: C alone takes ws-3 (its second step wants ws-1, held by A).
    _submit(client, "C", 7, [
        _acquire("ws-3"), _acquire("ws-1"),
        _compile(1), _release("ws-1"), _release("ws-3"),
    ])
    _run_until_holds(client, "C", "ws-3")

    # Phase 3: B alone takes ws-2 (its second step wants ws-3, held by C).
    _submit(client, "B", 8, [
        _acquire("ws-2"), _acquire("ws-3"),
        _compile(1), _release("ws-3"), _release("ws-2"),
    ])
    _run_until_holds(client, "B", "ws-2")

    # B asks ws-3 held by C -> B waits; C inherits B's priority 8.
    _run_until(client, "B", BuildStatus.WAITING.value)
    assert _build(client, "B")["blocked_by"] == "C"

    # C asks ws-1 held by A -> C waits (chain so far: C -> A, B -> C).
    _run_until(client, "C", BuildStatus.WAITING.value)
    assert _build(client, "C")["blocked_by"] == "A"

    # A burns padding then asks ws-2 held by B: closes A->B->C->A.
    r_events = _run_until(client, "A", BuildStatus.FAILED.value)
    last = r_events[-1]
    assert last["action"] == "acquire" and last["granted"] is False
    assert last["cycle"] is True
    a = _build(client, "A")
    assert "cycle" in a["fail_reason"]

    # A released ws-1 on failure -> waiting C is granted and becomes ready;
    # B is still queued on ws-3 which C keeps holding.
    c = _build(client, "C")
    assert c["status"] == BuildStatus.READY.value
    assert set(c["held_workspaces"]) == {"ws-3", "ws-1"}
    assert _build(client, "B")["status"] == BuildStatus.WAITING.value
    ws1 = next(w for w in client.get("/workspaces").json() if w["id"] == "ws-1")
    assert ws1["held_by"] == "C" and ws1["waiters"] == []


# --------------------------------------------------------------------------- #
# Cancellation
# --------------------------------------------------------------------------- #
def test_cancel_waiting_build_recompetes_and_releases_pressure(client: TestClient) -> None:
    _submit(client, "A", 2, [
        _acquire("ws-1"), _compile(10), _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")
    # L first so it actually joins the wait queue (base 4 > A's 2).
    _submit(client, "L", 4, [_acquire("ws-1"), _compile(10), _release("ws-1")])
    _run_until(client, "L", BuildStatus.WAITING.value)
    _submit(client, "H", 9, [_acquire("ws-1"), _compile(10), _release("ws-1")])
    _run_until(client, "H", BuildStatus.WAITING.value)
    assert _build(client, "A")["effective_priority"] == 9

    r = client.post("/builds/H/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == BuildStatus.CANCELED.value
    assert _build(client, "A")["effective_priority"] == 4
    ws1 = next(w for w in client.get("/workspaces").json() if w["id"] == "ws-1")
    assert ws1["waiters"] == ["L"]

    # canceling a non-waiting build is rejected
    assert client.post("/builds/A/cancel").status_code == 400
    assert client.post("/builds/H/cancel").status_code == 400


def test_cancel_waiting_holder_unwinds_inherited_priority(client: TestClient) -> None:
    """A waiting build that itself holds a workspace: canceling it must
    release that workspace and unwind the priority it propagated upward."""
    scheduler.reset(["ws-1", "ws-2"])
    # A holds ws-1 and parks.
    _submit(client, "A", 1, [
        _acquire("ws-1"), _compile(1), _compile(1),
        _compile(1), _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")
    # B grabs ws-2, then waits on ws-1 (A).
    _submit(client, "B", 4, [
        _acquire("ws-2"), _acquire("ws-1"),
        _compile(1), _release("ws-1"), _release("ws-2"),
    ])
    _run_until_holds(client, "B", "ws-2")
    _run_until(client, "B", BuildStatus.WAITING.value)
    # F(7) waits on ws-2 held by B -> A inherits 7 through B.
    _submit(client, "F", 7, [_acquire("ws-2"), _compile(1), _release("ws-2")])
    _run_until(client, "F", BuildStatus.WAITING.value)
    assert _build(client, "A")["effective_priority"] == 7
    assert _build(client, "B")["effective_priority"] == 7

    # Cancel B: ws-2 goes to F (becomes ready), and A's pressure disappears.
    r = client.post("/builds/B/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == BuildStatus.CANCELED.value
    assert _build(client, "A")["effective_priority"] == 1
    f = _build(client, "F")
    assert f["status"] == BuildStatus.READY.value and f["held_workspaces"] == ["ws-2"]
    ws2 = next(w for w in client.get("/workspaces").json() if w["id"] == "ws-2")
    assert ws2["held_by"] == "F" and ws2["waiters"] == []


# --------------------------------------------------------------------------- #
# Release / completion semantics
# --------------------------------------------------------------------------- #
def test_release_recompetes_waiters_by_effective_priority(client: TestClient) -> None:
    _submit(client, "A", 1, [
        _acquire("ws-1"), _compile(10), _release("ws-1"),
    ])
    _run_until_holds(client, "A", "ws-1")
    _submit(client, "lo", 2, [_acquire("ws-1"), _compile(10), _release("ws-1")])
    _run_until(client, "lo", BuildStatus.WAITING.value)
    _submit(client, "hi", 3, [_acquire("ws-1"), _compile(10), _release("ws-1")])
    _run_until(client, "hi", BuildStatus.WAITING.value)

    _run_until(client, "hi", BuildStatus.READY.value)  # A releases -> hi wins
    assert "ws-1" in _build(client, "hi")["held_workspaces"]
    assert _build(client, "lo")["status"] == BuildStatus.WAITING.value


def test_completed_build_returns_all_workspaces(client: TestClient) -> None:
    _submit(client, "A", 5, [
        _acquire("ws-1"), _acquire("ws-2"),
        _compile(10), _release("ws-1"), _release("ws-2"),
    ])
    _run_until(client, "A", BuildStatus.COMPLETED.value)
    assert _build(client, "A")["held_workspaces"] == []
    for ws in client.get("/workspaces").json():
        assert ws["held_by"] is None and ws["waiters"] == []


# --------------------------------------------------------------------------- #
# Observability & validation
# --------------------------------------------------------------------------- #
def test_events_and_snapshot_expose_effective_priority_and_waits(client: TestClient) -> None:
    _submit(client, "A", 2, [_acquire("ws-1"), _release("ws-1")])
    _run_until_holds(client, "A", "ws-1")
    _submit(client, "B", 8, [_acquire("ws-1"), _release("ws-1")])
    _run_until(client, "B", BuildStatus.WAITING.value)

    snap = client.get("/snapshot").json()
    assert {b["id"] for b in snap["builds"]} == {"A", "B"}
    b = next(b for b in snap["builds"] if b["id"] == "B")
    assert b["effective_priority"] == 8 and b["waiting_on"] == "ws-1"
    a = next(b for b in snap["builds"] if b["id"] == "A")
    assert a["effective_priority"] == 8
    kinds = {e["kind"] for e in client.get("/events").json()}
    assert {"build_submitted", "step", "system"} <= kinds
    step_events = [e for e in client.get("/events").json() if e["kind"] == "step"]
    assert all("action" in e and "tick" in e for e in step_events)


def test_incremental_recompute_matches_full_recompute(client: TestClient) -> None:
    """Churned interleaving: incremental chain recomputation must always agree
    with a from-scratch fixed-point recomputation."""
    import random

    rng = random.Random(42)
    scheduler.reset(["ws-1", "ws-2", "ws-3", "ws-4"])
    for i, pr in enumerate((2, 3, 4)):
        _submit(client, f"holder{i}", pr, [
            _acquire(f"ws-{i+1}"),
            _compile(5), _compile(5), _compile(5), _compile(5),
            _release(f"ws-{i+1}"),
        ])
    for j in range(12):
        ws = f"ws-{rng.randint(1, 4)}"
        _submit(client, f"w{j}", rng.randint(1, 10),
                [_acquire(ws), _compile(5), _release(ws)])

    def eps() -> dict[str, int]:
        return {b["id"]: b["effective_priority"]
                for b in client.get("/snapshot").json()["builds"]}

    for _ in range(80):
        _tick(client)
        inc = eps()
        scheduler._full_recompute()
        assert inc == eps()
        waiting = [b["id"] for b in client.get("/snapshot").json()["builds"]
                   if b["status"] == BuildStatus.WAITING.value]
        if waiting and rng.random() < 0.4:
            client.post(f"/builds/{rng.choice(waiting)}/cancel")
            inc = eps()
            scheduler._full_recompute()
            assert inc == eps()


def test_incoming_below_base_priority_does_not_lower_effective() -> None:
    """White-box: a waiter whose base priority is below the holder's own base
    must not lower the holder's effective priority.

    Through the API such a low-priority waiter can starve before its acquire
    is scheduled (the holder keeps winning the single executor), so the wait
    edge is constructed directly against the scheduler internals.
    """
    from app.scheduler import Build, BuildStatus, Step, StepType

    s = scheduler
    s.reset(["ws-1"])
    holder = Build(
        id="H", name="H", base_priority=10,
        steps=[Step(StepType.COMPILE)], seq=1,
    )
    waiter = Build(
        id="W", name="W", base_priority=2,
        steps=[Step(StepType.COMPILE)], seq=2,
    )
    s._builds["H"] = holder
    s._builds["W"] = waiter
    ws = s._workspaces["ws-1"]
    ws.held_by = "H"
    holder.held_workspaces.append("ws-1")
    ws.waiters.append("W")
    waiter.status = BuildStatus.WAITING
    waiter.waiting_on = "ws-1"

    s._recompute_chain(holder)
    assert holder.effective_priority == 10  # max(10, 2), not lowered to 2


def test_validation_errors(client: TestClient) -> None:
    r = client.post("/builds", json={
        "id": "x", "name": "x", "base_priority": 1,
        "steps": [{"type": "acquire", "workspace": "nope"}],
    })
    assert r.status_code == 400
    r = client.post("/builds", json={
        "id": "y", "name": "y", "base_priority": 1, "steps": [],
    })
    assert r.status_code == 422
    assert client.post("/workspaces", json={"id": "ws-1"}).status_code == 400
