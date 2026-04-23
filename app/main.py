from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from app.broker import InMemoryBroker
from app.schemas import (
    PollResponse,
    PublishRequest,
    PublishResponse,
    SubscriptionCreateRequest,
    SubscriptionCreateResponse,
    TopicCreateRequest,
)
from app.stomp import build_stomp_frame, parse_stomp_frame

broker = InMemoryBroker()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await broker.startup()
    yield


app = FastAPI(title="Broker Stage 3", lifespan=lifespan)


def _normalize_destination(value: str) -> str:
    return value if value.startswith("/topic/") else f"/topic/{value}"


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/topics")
async def list_topics() -> dict[str, list[str]]:
    return {"topics": await broker.list_topics()}


@app.post("/topics")
async def create_topic(payload: TopicCreateRequest) -> dict[str, str]:
    topic = _normalize_destination(payload.name)
    await broker.create_topic(topic)
    return {"topic": topic}


@app.delete("/topics/{topic_name}")
async def delete_topic(topic_name: str) -> dict[str, str]:
    topic = _normalize_destination(topic_name)
    await broker.delete_topic(topic)
    return {"topic": topic}


@app.post("/topics/{topic_name}/publish", response_model=PublishResponse)
async def publish(topic_name: str, payload: PublishRequest) -> PublishResponse:
    destination = _normalize_destination(topic_name)
    message, delivered_to = await broker.publish(destination, payload.body, payload.headers)
    return PublishResponse(message_id=message.message_id, delivered_to=delivered_to)


@app.post("/subscriptions", response_model=SubscriptionCreateResponse)
async def create_subscription(payload: SubscriptionCreateRequest) -> SubscriptionCreateResponse:
    destination = _normalize_destination(payload.destination)
    subscription = await broker.subscribe(destination, subscription_id=payload.subscription_id)
    return SubscriptionCreateResponse(
        subscription_id=subscription.subscription_id,
        destination=subscription.destination,
    )


@app.delete("/subscriptions/{subscription_id}")
async def delete_subscription(subscription_id: str) -> dict[str, str]:
    await broker.unsubscribe(subscription_id)
    return {"subscription_id": subscription_id}


@app.get("/subscriptions/{subscription_id}/poll", response_model=PollResponse | None)
async def poll(subscription_id: str, timeout: float = 5.0) -> PollResponse | None:
    message = await broker.poll(subscription_id, timeout_seconds=timeout)
    if message is None:
        return None
    return PollResponse(
        message_id=message.message_id,
        destination=message.destination,
        body=message.body,
        headers=message.headers,
    )


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    delivery_tasks: dict[str, asyncio.Task[None]] = {}
    broker_subscription_ids: dict[str, str] = {}
    try:
        while True:
            raw_frame = await websocket.receive_text()
            frame = parse_stomp_frame(raw_frame)

            if frame.command == "CONNECT":
                connected = build_stomp_frame("CONNECTED", {"version": "1.2"})
                await websocket.send_text(connected)
                continue

            if frame.command == "SEND":
                destination = frame.headers.get("destination")
                if not destination:
                    error = build_stomp_frame("ERROR", {"message": "destination required"})
                    await websocket.send_text(error)
                    continue
                await broker.publish(destination, frame.body, frame.headers)
                continue

            if frame.command == "SUBSCRIBE":
                destination = frame.headers.get("destination")
                subscription_id = frame.headers.get("id")
                if not destination or not subscription_id:
                    error = build_stomp_frame("ERROR", {"message": "destination and id required"})
                    await websocket.send_text(error)
                    continue
                subscription = await broker.subscribe(destination, subscription_id=subscription_id)
                broker_subscription_ids[subscription_id] = subscription.subscription_id
                delivery_tasks[subscription_id] = asyncio.create_task(
                    _delivery_loop(websocket, subscription_id, subscription.destination)
                )
                continue

            if frame.command == "UNSUBSCRIBE":
                subscription_id = frame.headers.get("id")
                if not subscription_id:
                    continue
                await _stop_subscription(subscription_id, delivery_tasks, broker_subscription_ids)
                continue

            if frame.command in {"ACK", "NACK"}:
                continue

            if frame.command == "DISCONNECT":
                break

            error = build_stomp_frame("ERROR", {"message": f"unsupported command {frame.command}"})
            await websocket.send_text(error)
    except WebSocketDisconnect:
        pass
    finally:
        for subscription_id in list(broker_subscription_ids):
            await _stop_subscription(subscription_id, delivery_tasks, broker_subscription_ids)
        await websocket.close()


async def _delivery_loop(websocket: WebSocket, subscription_id: str, destination: str) -> None:
    while True:
        message = await broker.poll(subscription_id, timeout_seconds=60.0)
        if message is None:
            continue
        frame = build_stomp_frame(
            "MESSAGE",
            {
                "subscription": subscription_id,
                "destination": destination,
                "message-id": message.message_id,
            },
            message.body,
        )
        await websocket.send_text(frame)


async def _stop_subscription(
    subscription_id: str,
    delivery_tasks: dict[str, asyncio.Task[None]],
    broker_subscription_ids: dict[str, str],
) -> None:
    broker_id = broker_subscription_ids.pop(subscription_id, None)
    if broker_id:
        await broker.unsubscribe(broker_id)
    task = delivery_tasks.pop(subscription_id, None)
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
