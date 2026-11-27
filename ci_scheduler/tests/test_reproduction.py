"""Tests for cycle reproduction numbers, JSON packages, and isolated replay."""

from __future__ import annotations

import copy

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.scheduler import Scheduler, Step, StepType, scheduler


def acq(ws: str) -> dict:
    return {"type": "acquire", "workspace": ws}


def rel(ws: str) -> dict:
    return {"type": "release", "workspace": ws}


def comp(ms: int = 1) -> dict:
    return {"type": "compile", "duration_ms": ms}


@pytest.fixture()
def client() -> TestClient:
    scheduler.reset(["ws-1", "ws-2"])
    return TestClient(app)


def submit(client: TestClient, bid: str, priority: int, steps: list[dict]) -> None:
    response = client.post(
        "/builds",
        json={
            "id": bid,
            "name": bid,
            "base_priority": priority,
            "steps": steps,
        },
    )
    assert response.status_code == 201, response.text


def make_two_node_cycle(client: TestClient, *, use_advance: bool = False) -> tuple[str, dict]:
    """Create A -> ws-2 -> B -> ws-1 -> A and return rid/final result."""
    submit(client, "A", 1, [acq("ws-1"), comp(), acq("ws-2")])
    assert client.post("/tick").json()["action"] == "acquire"  # A holds ws-1

    submit(client, "B", 8, [acq("ws-2"), acq("ws-1")])
    assert client.post("/tick").json()["workspace_id"] == "ws-2"
    blocked = client.post("/tick").json()
    assert blocked["status"] == "waiting" and blocked["blocked_by"] == "A"

    if use_advance:
        response = client.post("/advance", json={"steps": 2})
        assert response.status_code == 200, response.text
        results = response.json()
        final = results[-1]
    else:
        client.post("/tick")  # A compile
        final = client.post("/tick").json()

    assert final["cycle"] is True
    rid = final["reproduction_id"]
    assert rid and final["repro_id"] == rid
    assert final["cycle_path"] == ["A", "ws-2", "B", "ws-1", "A"]
    return rid, final


def test_cycle_tick_gets_reproduction_number_and_ordered_package(
    client: TestClient,
) -> None:
    rid, final = make_two_node_cycle(client)

    listing = client.get("/reproductions").json()
    assert [item["reproduction_id"] for item in listing] == [rid]
    assert listing[0]["failed_build_id"] == "A"

    response = client.get(f"/reproductions/{rid}/package")
    assert response.status_code == 200
    assert "attachment" in response.headers["content-disposition"]
    assert rid in response.headers["content-disposition"]
    package = response.json()

    assert package["schema"].startswith("ci-scheduler-reproduction-package")
    assert package["initial_workspaces"] == ["ws-1", "ws-2"]
    assert package["failed_build_id"] == "A"
    assert package["cycle_path"] == ["A", "ws-2", "B", "ws-1", "A"]
    assert package["cycle"] == ["A", "ws-2", "B", "ws-1", "A"]
    assert package["cycle_ring"]["build_ids"] == ["A", "B", "A"]
    assert package["cycle_ring"]["workspace_ids"] == ["ws-2", "ws-1"]
    assert package["last_tick"]["tick"] == final["tick"]
    assert package["last_tick"]["cycle"] is True
    assert package["failed_build"]["status"] == "failed"

    types = [op["type"] for op in package["operations"]]
    assert types == [
        "submit_build",  # A
        "tick",          # A acquire ws-1
        "submit_build",  # B inserted before the next effective ticks
        "tick",          # B acquire ws-2
        "tick",          # B waits on ws-1
        "tick",          # A compile
        "tick",          # A closes the cycle
    ]
    assert [op["seq"] for op in package["operations"]] == list(range(1, 8))
    assert [op["tick"] for op in package["operations"] if op["type"] == "tick"] == [
        1, 2, 3, 4, 5
    ]
    assert [build["id"] for build in package["build_submissions"]] == ["A", "B"]
    assert package["workspace_creations"] == []
    assert package["cancellations"] == []
    assert package["ticks"][-1]["reproduction_id"] == rid


def test_advance_batch_cycle_result_carries_reproduction_number(client: TestClient) -> None:
    rid, final = make_two_node_cycle(client, use_advance=True)
    package = client.get(f"/reproductions/{rid}/package").json()
    advance_ticks = [
        op for op in package["operations"]
        if op["type"] == "tick" and op["request"]["source"] == "advance"
    ]
    assert [op["request"]["batch_index"] for op in advance_ticks] == [0, 1]
    assert len({op["request"]["batch_id"] for op in advance_ticks}) == 1


def test_package_includes_only_successful_creates_submissions_and_cancels(
    client: TestClient,
) -> None:
    client.post("/reset", json={"workspace_ids": ["ws-1"]})
    # Successful and failed workspace creation.
    assert client.post("/workspaces", json={"id": "ws-3"}).status_code == 201
    assert client.post("/workspaces", json={"id": "ws-3"}).status_code == 400

    # A waiting build that will be successfully canceled.
    submit(client, "H", 1, [acq("ws-1"), comp(10), rel("ws-1")])
    client.post("/tick")
    submit(client, "W", 5, [acq("ws-1"), comp(1), rel("ws-1")])
    client.post("/tick")  # W waits
    assert client.post("/builds/W/cancel").status_code == 200
    assert client.post("/builds/W/cancel").status_code == 400
    # Drain the unrelated holder H so it cannot win scheduling ticks below.
    client.post("/advance", json={"steps": 12})

    # Invalid submission is rejected and must not enter the package.
    bad = client.post("/builds", json={
        "id": "bad",
        "name": "bad",
        "base_priority": 1,
        "steps": [acq("does-not-exist")],
    })
    assert bad.status_code == 400

    # Add a second workspace and create a cycle on ws-2/ws-3.
    assert client.post("/workspaces", json={"id": "ws-2"}).status_code == 201
    submit(client, "C", 1, [acq("ws-2"), comp(), acq("ws-3")])
    client.post("/tick")
    submit(client, "D", 8, [acq("ws-3"), acq("ws-2")])
    client.post("/tick")
    client.post("/tick")
    client.post("/tick")
    final = client.post("/tick").json()
    assert final["cycle"] is True
    rid = final["reproduction_id"]

    package = client.get(f"/reproductions/{rid}/package").json()
    creation_ids = [item["id"] for item in package["workspace_creations"]]
    assert creation_ids == ["ws-3", "ws-2"]  # duplicate ws-3 omitted
    assert [b["id"] for b in package["build_submissions"]] == ["H", "W", "C", "D"]
    assert [c["build_id"] for c in package["cancellations"]] == ["W"]
    assert [op["type"] for op in package["operations"]].count("cancel_build") == 1


def test_interleaved_batch_is_journaled_in_effective_tick_order() -> None:
    drill = Scheduler()
    drill.reset(["ws-1"])
    drill.submit_build(
        "A", "A", 10, [
            Step(type=StepType.COMPILE, duration_ms=1),
            Step(type=StepType.COMPILE, duration_ms=1),
            Step(type=StepType.COMPILE, duration_ms=1),
        ]
    )

    original_tick = drill.tick
    state = {"calls": 0}

    def interleaved_tick(**kwargs):  # type: ignore[no-untyped-def]
        result = original_tick(**kwargs)
        if state["calls"] == 0:
            # This represents another HTTP request taking the scheduler lock
            # between two beats of the same /advance batch.
            drill.create_workspace("ws-interleaved")
            drill.submit_build(
                "I", "I", 1, [Step(type=StepType.COMPILE, duration_ms=1)]
            )
        state["calls"] += 1
        return result

    drill.tick = interleaved_tick  # type: ignore[method-assign]
    drill.advance(3)

    operations = drill._journal
    compact = [(op["seq"], op["type"], op.get("workspace_id") or op.get("build_id"))
               for op in operations]
    assert compact[:5] == [
        (1, "submit_build", "A"),
        (2, "tick", None),
        (3, "create_workspace", "ws-interleaved"),
        (4, "submit_build", "I"),
        (5, "tick", None),
    ]
    # The remaining beat follows, with no sequence gaps or reordering.
    assert [seq for seq, _, _ in compact] == list(range(1, len(compact) + 1))
    tick_ops = [op for op in operations if op["type"] == "tick"]
    assert [op["request"]["batch_index"] for op in tick_ops] == [0, 1, 2]
    assert len({op["request"]["batch_id"] for op in tick_ops}) == 1


def test_multiple_cycles_share_journal_but_packages_stop_at_their_fault(
    client: TestClient,
) -> None:
    rid1, _ = make_two_node_cycle(client)
    # Let B finish after A's failure releases ws-1.
    for _ in range(10):
        result = client.post("/tick").json()
        if result.get("action") == "noop":
            break
    assert client.get("/builds/B").json()["status"] == "completed"

    # Independent second cycle using ws-3/ws-? Existing reset has only ws-1/2.
    client.post("/workspaces", json={"id": "ws-3"})
    client.post("/workspaces", json={"id": "ws-4"})
    submit(client, "C", 1, [acq("ws-3"), comp(), acq("ws-4")])
    client.post("/tick")
    submit(client, "D", 8, [acq("ws-4"), acq("ws-3")])
    for _ in range(4):
        final = client.post("/tick").json()
        if final.get("cycle"):
            break
    else:
        raise AssertionError("second cycle was not reported")
    rid2 = final["reproduction_id"]
    assert rid1 != rid2

    pkg1 = client.get(f"/reproductions/{rid1}/package").json()
    pkg2 = client.get(f"/reproductions/{rid2}/package").json()

    assert pkg1["failed_build_id"] == "A"
    assert pkg2["failed_build_id"] == "C"
    assert pkg1["cycle_path"] == ["A", "ws-2", "B", "ws-1", "A"]
    assert pkg2["cycle_path"] == ["C", "ws-4", "D", "ws-3", "C"]
    assert not any(
        op.get("build_id") == "C" and op["type"] == "submit_build"
        for op in pkg1["operations"]
    )
    assert any(
        op.get("build_id") == "C" and op["type"] == "submit_build"
        for op in pkg2["operations"]
    )
    assert len(pkg2["operations"]) > len(pkg1["operations"])
    assert pkg1["operations"][-1]["result"]["reproduction_id"] == rid1
    assert pkg2["operations"][-1]["result"]["reproduction_id"] == rid2


def test_replay_downloaded_package_then_reset_invalidates_number_but_not_json(
    client: TestClient,
) -> None:
    rid, _ = make_two_node_cycle(client)
    package = client.get(f"/reproductions/{rid}/package").json()

    replayed = client.post("/replay", json=package)
    assert replayed.status_code == 200, replayed.text
    report = replayed.json()
    assert report["matched"] is True
    assert report["status"] == "matched"
    assert report["failed_build_id"] == "A"
    assert report["cycle_path"] == ["A", "ws-2", "B", "ws-1", "A"]
    assert report["cycle"] == report["cycle_path"]
    assert report["failed_build"]["status"] == "failed"
    assert report["snapshot"]["tick"] == package["last_tick"]["tick"]
    assert any(event["kind"] == "step" for event in report["events"])

    # Envelope form is accepted too.
    enveloped = client.post("/replay", json={"package": package}).json()
    assert enveloped["matched"] is True

    live_before = client.get("/snapshot").json()
    events_before = client.get("/events").json()
    client.post("/replay", json=package)
    assert client.get("/snapshot").json() == live_before
    assert client.get("/events").json() == events_before

    client.post("/reset", json={"workspace_ids": ["ws-9"]})
    assert client.get(f"/reproductions/{rid}/package").status_code == 404
    assert client.get(f"/repro/{rid}/package").status_code == 404

    # The downloaded JSON remains self-contained and replayable.
    after_reset = client.post("/replay", json=package).json()
    assert after_reset["matched"] is True
    assert after_reset["failed_build_id"] == "A"

    # The live scheduler was reset and can continue normally.
    live = client.get("/snapshot").json()
    assert live["tick"] == 0
    assert [ws["id"] for ws in live["workspaces"]] == ["ws-9"]
    submit(client, "fresh", 1, [comp()])
    tick = client.post("/tick").json()
    assert tick["build_id"] == "fresh"
    assert client.get("/builds/fresh").json()["status"] == "completed"


def test_replay_mismatch_is_reported(client: TestClient) -> None:
    rid, _ = make_two_node_cycle(client)
    package = client.get(f"/reproductions/{rid}/package").json()

    tampered = copy.deepcopy(package)
    tampered["failed_build_id"] = "B"
    response = client.post("/replay", json=tampered)
    assert response.status_code == 200
    report = response.json()
    assert report["matched"] is False
    assert report["match"] is False
    assert report["status"] == "mismatch"
    assert report["mismatches"]

    strict = client.post("/replay?strict=true", json=tampered)
    assert strict.status_code == 409
    assert strict.json()["matched"] is False

    verify = client.post("/replay/verify", json=tampered)
    assert verify.status_code == 409
    assert verify.json()["status"] == "mismatch"

    broken = copy.deepcopy(package)
    # Change A's first acquire from ws-1 to ws-2, making the replay diverge.
    broken["operations"][0]["request"]["steps"][0]["workspace"] = "ws-2"
    mismatch = client.post("/replay", json=broken).json()
    assert mismatch["matched"] is False


def test_three_node_cycle_package_replays_long_ring(client: TestClient) -> None:
    scheduler.reset(["ws-1", "ws-2", "ws-3"])
    submit(client, "A", 1, [acq("ws-1"), comp(), comp(), acq("ws-2")])
    client.post("/tick")  # A ws-1

    submit(client, "C", 7, [acq("ws-3"), acq("ws-1")])
    client.post("/tick")  # C ws-3
    client.post("/tick")  # C waits ws-1

    submit(client, "B", 8, [acq("ws-2"), acq("ws-3")])
    client.post("/tick")  # B ws-2
    client.post("/tick")  # B waits ws-3

    final = None
    for _ in range(4):
        final = client.post("/tick").json()
        if final.get("cycle"):
            break
    assert final and final["cycle"] is True
    expected = ["A", "ws-2", "B", "ws-3", "C", "ws-1", "A"]
    assert final["cycle_path"] == expected

    package = client.get(f"/reproductions/{final['reproduction_id']}/package").json()
    report = client.post("/replay", json=package).json()
    assert report["matched"] is True, report.get("mismatches")
    assert report["cycle_path"] == expected
