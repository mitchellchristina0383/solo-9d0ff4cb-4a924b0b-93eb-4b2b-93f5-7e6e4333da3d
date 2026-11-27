#!/usr/bin/env python3
"""End-to-end reproduction-package drill over real HTTP.

Verifies:
1. operations inserted while a large ``/advance`` batch is running are
   journaled between the actual ticks they interrupted;
2. a cycle tick receives a reproduction number and downloadable JSON package;
3. the downloaded package replays in an isolated drill;
4. after ``/reset`` the old number is 404 while the saved JSON still replays;
5. the live scheduler can continue normally after replay.

Usage:
    python scripts/repro_demo.py [base_url] [save_dir]
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SAVE_DIR = sys.argv[2] if len(sys.argv) > 2 else tempfile.mkdtemp(prefix="ci-repro-")
os.makedirs(SAVE_DIR, exist_ok=True)


def post(client: httpx.Client, path: str, **body: object) -> dict:
    response = client.post(BASE + path, json=body or None)
    response.raise_for_status()
    return response.json()


def get(client: httpx.Client, path: str) -> object:
    response = client.get(BASE + path)
    response.raise_for_status()
    return response.json()


def acq(ws: str) -> dict:
    return {"type": "acquire", "workspace": ws}


def comp(ms: int = 0) -> dict:
    return {"type": "compile", "duration_ms": ms}


def demonstrate_interleaving() -> None:
    print("\n=== interleaved /advance batch ===")
    httpx.post(BASE + "/reset", json={"workspace_ids": ["ws-bulk"]}, timeout=30)
    tick_count = 10_000
    response = httpx.post(
        BASE + "/builds",
        json={
            "id": "bulk",
            "name": "bulk",
            "base_priority": 100,
            "steps": [comp(0) for _ in range(tick_count)],
        },
        timeout=60,
    )
    response.raise_for_status()

    def advance_batch() -> list[dict]:
        response = httpx.post(
            BASE + "/advance", json={"steps": tick_count}, timeout=120
        )
        response.raise_for_status()
        return response.json()

    def create_workspace(i: int) -> None:
        # Independent clients are important: sharing one httpx.Client between
        # threads can serialize at the connection pool.  The small stagger
        # gives the server opportunities to schedule the request between beats.
        time.sleep((i % 20) * 0.002)
        response = httpx.post(
            BASE + "/workspaces",
            json={"id": f"ws-insert-{i:02d}"},
            timeout=30,
        )
        response.raise_for_status()

    with ThreadPoolExecutor(max_workers=21) as pool:
        advance = pool.submit(advance_batch)
        creators = [pool.submit(create_workspace, i) for i in range(40)]
        for future in creators:
            future.result()
        advanced = advance.result()

    assert len(advanced) == tick_count

    journal_response = httpx.get(BASE + "/journal", timeout=30)
    journal_response.raise_for_status()
    journal = journal_response.json()
    tick_seqs = [op["seq"] for op in journal if op["type"] == "tick"]
    insert_seqs = [
        op["seq"] for op in journal if op["type"] == "create_workspace"
    ]
    assert tick_seqs == sorted(tick_seqs)
    assert insert_seqs == sorted(insert_seqs)

    interleaved = []
    for seq in insert_seqs:
        before = [tick_seq for tick_seq in tick_seqs if tick_seq < seq]
        after = [tick_seq for tick_seq in tick_seqs if tick_seq > seq]
        if before and after:
            interleaved.append(seq)
    assert interleaved, "no inserted workspace was journaled between two ticks"
    print(
        f"journal entries={len(journal)}, ticks={len(tick_seqs)}, "
        f"creates={len(insert_seqs)}, inserts_between_ticks={len(interleaved)}"
    )
    print(f"example interleaved operation seq={interleaved[0]}")


def demonstrate_cycle_package_and_reset_replay(client: httpx.Client) -> None:
    print("\n=== cycle reproduction package ===")
    post(client, "/reset", workspace_ids=["ws-1", "ws-2"])

    post(
        client,
        "/builds",
        id="A",
        name="first",
        base_priority=1,
        steps=[acq("ws-1"), comp(1), acq("ws-2")],
    )
    post(client, "/tick")  # A holds ws-1

    post(
        client,
        "/builds",
        id="B",
        name="second",
        base_priority=8,
        steps=[acq("ws-2"), acq("ws-1")],
    )
    post(client, "/tick")  # B holds ws-2
    waiting = post(client, "/tick")  # B waits on ws-1
    assert waiting["status"] == "waiting"

    advanced = post(client, "/advance", steps=2)
    cycles = [result for result in advanced if result.get("cycle") is True]
    assert len(cycles) == 1, advanced
    final = cycles[0]
    rid = final["reproduction_id"]
    expected_path = ["A", "ws-2", "B", "ws-1", "A"]
    assert final["cycle_path"] == expected_path
    print(f"reproduction number: {rid}")
    print(f"cycle path: {' -> '.join(final['cycle_path'])}")

    response = client.get(f"{BASE}/reproductions/{rid}/package")
    response.raise_for_status()
    package = response.json()
    package_path = os.path.join(SAVE_DIR, f"{rid}.json")
    with open(package_path, "w", encoding="utf-8") as handle:
        json.dump(package, handle, ensure_ascii=False, indent=2)
    print(f"downloaded package: {package_path} ({os.path.getsize(package_path)} bytes)")

    replay = client.post(BASE + "/replay", json=package).json()
    assert replay["matched"] is True, replay.get("mismatches")
    assert replay["failed_build_id"] == "A"
    assert replay["cycle_path"] == expected_path
    print("pre-reset replay: matched=true, failed_build_id=A")

    live_before = get(client, "/snapshot")
    replay_again = client.post(BASE + "/replay", json=package).json()
    assert replay_again["matched"] is True
    assert get(client, "/snapshot") == live_before
    print("replay did not change live snapshot")

    post(client, "/reset", workspace_ids=["ws-9"])
    old_number = client.get(f"{BASE}/reproductions/{rid}/package")
    assert old_number.status_code == 404, old_number.status_code
    print("after reset, old reproduction number: HTTP 404")

    with open(package_path, encoding="utf-8") as handle:
        downloaded = json.load(handle)
    after_reset = client.post(BASE + "/replay", json=downloaded).json()
    assert after_reset["matched"] is True, after_reset.get("mismatches")
    assert after_reset["failed_build_id"] == "A"
    assert after_reset["cycle_path"] == expected_path
    print("saved JSON replay after reset: matched=true")

    post(
        client,
        "/builds",
        id="fresh",
        name="fresh-live-build",
        base_priority=1,
        steps=[comp(1)],
    )
    tick = post(client, "/tick")
    assert tick["build_id"] == "fresh"
    fresh = get(client, "/builds/fresh")
    assert fresh["status"] == "completed"
    print("live scheduler still advances: fresh=completed")


def main() -> None:
    demonstrate_interleaving()
    with httpx.Client(timeout=60) as client:
        demonstrate_cycle_package_and_reset_replay(client)
    print("\nREPRO DEMO OK")


if __name__ == "__main__":
    for attempt in range(30):
        try:
            main()
            break
        except httpx.HTTPError as exc:
            if attempt == 29:
                raise
            time.sleep(0.5)
