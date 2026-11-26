#!/usr/bin/env python3
"""End-to-end drill against a running ci-scheduler instance.

Scenario (two workspaces ws-1 / ws-2):

    F(7) -> ws-2 -> B(4) -> ws-1 -> A(1)

* A grabs ws-1, B grabs ws-2 then blocks on ws-1, F blocks on ws-2.
* Priority 7 must propagate both hops (A.eff == B.eff == 7).
* A ready competitor N(6) proves A only runs because of inheritance.
* A releases ws-1 -> B is granted and finishes, releasing ws-2 -> F runs.
* Then a separate cancellation drill: high-priority waiter cancels and the
  holder's effective priority drops back immediately.

Usage: python scripts/demo.py [base_url]
"""

from __future__ import annotations

import json
import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"


def post(client: httpx.Client, path: str, **body: object) -> dict:
    r = client.post(BASE + path, json=body or None)
    r.raise_for_status()
    return r.json()


def get(client: httpx.Client, path: str) -> object:
    r = client.get(BASE + path)
    r.raise_for_status()
    return r.json()


def acq(ws: str) -> dict:
    return {"type": "acquire", "workspace": ws}


def rel(ws: str) -> dict:
    return {"type": "release", "workspace": ws}


def comp(ms: int = 100) -> dict:
    return {"type": "compile", "duration_ms": ms}


def show(title: str, payload: object) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def wait_holds(client: httpx.Client, build_id: str, ws: str) -> None:
    for _ in range(100):
        b = get(client, f"/builds/{build_id}")
        if ws in b["held_workspaces"]:
            return
        post(client, "/tick")
    raise RuntimeError(f"{build_id} never acquired {ws}")


def wait_status(client: httpx.Client, build_id: str, status: str) -> None:
    for _ in range(100):
        if get(client, f"/builds/{build_id}")["status"] == status:
            return
        post(client, "/tick")
    raise RuntimeError(f"{build_id} never reached {status}")


def main() -> None:
    with httpx.Client(timeout=10) as client:
        post(client, "/reset", workspace_ids=["ws-1", "ws-2"])

        # ---- Phase 1: A holds ws-1 ------------------------------------
        post(client, "/builds", id="A", name="patch-build", base_priority=1,
             steps=[acq("ws-1"), comp(50), comp(50), comp(50), rel("ws-1")])
        wait_holds(client, "A", "ws-1")

        # ---- Phase 2: B holds ws-2, then blocks on ws-1 ---------------
        post(client, "/builds", id="B", name="integration", base_priority=4,
             steps=[acq("ws-2"), acq("ws-1"), comp(80),
                    rel("ws-1"), rel("ws-2")])
        wait_holds(client, "B", "ws-2")
        wait_status(client, "B", "waiting")

        # ---- Phase 3: F blocks on ws-2 -> two-hop inheritance ---------
        post(client, "/builds", id="F", name="hotfix", base_priority=7,
             steps=[acq("ws-2"), comp(30), rel("ws-2")])
        wait_status(client, "F", "waiting")

        snap = get(client, "/snapshot")
        show("two-hop wait chain F -> B -> A", snap)
        eps = {b["id"]: b["effective_priority"] for b in snap["builds"]}
        assert eps == {"A": 7, "B": 7, "F": 7}, eps
        waits = {b["id"]: (b["waiting_on"], b["blocked_by"])
                 for b in snap["builds"] if b["status"] == "waiting"}
        assert waits == {"B": ("ws-1", "A"), "F": ("ws-2", "B")}, waits

        # ---- Inheritance beats a ready competitor ---------------------
        post(client, "/builds", id="N", name="normal-job", base_priority=6,
             steps=[comp(10), comp(10)])
        r = post(client, "/tick")
        assert r["build_id"] == "A", r
        print(f"\n[tick] executor picked A (ep 7) over ready N (ep 6): {r}")

        # ---- Drain: A releases ws-1 -> B granted -> B finishes -> F ---
        for _ in range(100):
            r = post(client, "/tick")
            if r.get("action") == "noop":
                break
        for bid in ("A", "B", "F", "N"):
            assert get(client, f"/builds/{bid}")["status"] == "completed"
        show("chain drained, all completed", get(client, "/workspaces"))

        # ---- Cancellation drill ---------------------------------------
        post(client, "/reset", workspace_ids=["ws-1"])
        post(client, "/builds", id="H", name="holder", base_priority=2,
             steps=[acq("ws-1"), comp(10), comp(10), rel("ws-1")])
        wait_holds(client, "H", "ws-1")
        post(client, "/builds", id="lo", name="low", base_priority=4,
             steps=[acq("ws-1"), comp(10), rel("ws-1")])
        wait_status(client, "lo", "waiting")
        post(client, "/builds", id="hi", name="urgent", base_priority=9,
             steps=[acq("ws-1"), comp(10), rel("ws-1")])
        wait_status(client, "hi", "waiting")
        assert get(client, "/builds/H")["effective_priority"] == 9
        print("\nbefore cancel: H.effective =",
              get(client, "/builds/H")["effective_priority"])

        canceled = post(client, "/builds/hi/cancel")
        assert canceled["status"] == "canceled"
        assert get(client, "/builds/H")["effective_priority"] == 4
        print("after cancel(hi): H.effective =",
              get(client, "/builds/H")["effective_priority"],
              "(dropped back to lo's 4)")
        show("post-cancel snapshot", get(client, "/snapshot"))

        # canceling a non-waiting build is rejected
        bad = client.post(BASE + "/builds/H/cancel")
        assert bad.status_code == 400, bad.text
        print(f"cancel of running build rejected: HTTP {bad.status_code} {bad.json()}")

        print("\nDEMO OK: multi-level wait, inheritance, release re-competition "
              "and cancellation all verified over HTTP.")


if __name__ == "__main__":
    for attempt in range(20):
        try:
            main()
            break
        except httpx.HTTPError as exc:
            if attempt == 19:
                raise
            time.sleep(0.5)
