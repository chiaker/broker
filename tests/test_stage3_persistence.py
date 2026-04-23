"""Stage 3 — persistence and reconnection tests."""
from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.testclient import TestClient

from app.broker import InMemoryBroker
from app.main import app


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Test: journal survives broker restart
# ---------------------------------------------------------------------------

def test_journal_persists_across_broker_restart(tmp_path: Path) -> None:
    """Messages written before restart are readable after restart."""
    import app.journal as jmod
    import app.broker as bmod

    original = jmod.DATA_DIR
    jmod.DATA_DIR = tmp_path
    bmod.DATA_DIR = tmp_path

    try:
        destination = "/topic/test.restart"

        async def phase1():
            broker = InMemoryBroker()
            await broker.startup()
            await broker.publish(destination, "msg-before-restart")

        async def phase2():
            broker = InMemoryBroker()
            await broker.startup()
            # Force offset to 0 so the message is readable (saved offset = 0).
            await broker.subscribe(destination, subscription_id="stable-sub")
            broker._offsets["stable-sub"] = 0
            return await broker.poll("stable-sub", timeout_seconds=1.0)

        _run(phase1())
        msg = _run(phase2())

        assert msg is not None, "Expected message from journal after restart"
        assert msg.body == "msg-before-restart"
    finally:
        jmod.DATA_DIR = original
        bmod.DATA_DIR = original


# ---------------------------------------------------------------------------
# Test: offset is saved and resumed
# ---------------------------------------------------------------------------

def test_offset_saved_and_resumed(tmp_path: Path) -> None:
    """After unsubscribe, re-subscribing with the same ID resumes from saved offset."""
    import app.journal as jmod
    import app.broker as bmod

    original = jmod.DATA_DIR
    jmod.DATA_DIR = tmp_path
    bmod.DATA_DIR = tmp_path

    try:
        destination = "/topic/test.offset"

        async def run():
            broker = InMemoryBroker()

            await broker.publish(destination, "first")
            await broker.publish(destination, "second")

            await broker.subscribe(destination, subscription_id="resumable")
            broker._offsets["resumable"] = 0  # start from beginning

            msg1 = await broker.poll("resumable", timeout_seconds=1.0)
            assert msg1 is not None and msg1.body == "first"

            # Unsubscribe → offset 1 persisted to disk.
            await broker.unsubscribe("resumable")
            import json as _json
            saved = _json.loads((tmp_path / "offsets" / "resumable.txt").read_text())
            assert saved["offset"] == 1
            assert saved["destination"] == destination

            # Re-subscribe with same ID → loads offset 1.
            await broker.subscribe(destination, subscription_id="resumable")
            msg2 = await broker.poll("resumable", timeout_seconds=1.0)
            assert msg2 is not None and msg2.body == "second"

        _run(run())
    finally:
        jmod.DATA_DIR = original
        bmod.DATA_DIR = original


# ---------------------------------------------------------------------------
# Test: REST endpoint accepts subscription_id for reconnection
# ---------------------------------------------------------------------------

def test_rest_subscribe_with_stable_id() -> None:
    """POST /subscriptions with subscription_id returns the same ID."""
    client = TestClient(app)
    response = client.post(
        "/subscriptions",
        json={"destination": "/topic/stable", "subscription_id": "my-stable-id"},
    )
    response.raise_for_status()
    data = response.json()
    assert data["subscription_id"] == "my-stable-id"
    assert data["destination"] == "/topic/stable"


# ---------------------------------------------------------------------------
# Test: fanout still works (regression guard for stage 2)
# ---------------------------------------------------------------------------

def test_fanout_regression() -> None:
    """Both subscribers receive the same message after the journal refactor."""
    client = TestClient(app)
    destination = "/topic/fanout.regression"

    sub1 = client.post("/subscriptions", json={"destination": destination}).json()
    sub2 = client.post("/subscriptions", json={"destination": destination}).json()

    pub = client.post(
        "/topics/fanout.regression/publish",
        json={"body": "ping", "headers": {}},
    )
    pub.raise_for_status()
    assert pub.json()["delivered_to"] == 2

    m1 = client.get(f"/subscriptions/{sub1['subscription_id']}/poll", params={"timeout": 1.0})
    m2 = client.get(f"/subscriptions/{sub2['subscription_id']}/poll", params={"timeout": 1.0})

    assert m1.json()["body"] == "ping"
    assert m2.json()["body"] == "ping"
