from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4


@dataclass(slots=True)
class BrokerMessage:
    message_id: str
    destination: str
    body: str
    headers: dict[str, str]
    published_at: str


@dataclass(slots=True)
class BrokerSubscription:
    subscription_id: str
    destination: str
    queue: asyncio.Queue[BrokerMessage]


class InMemoryBroker:
    def __init__(self) -> None:
        self._topics: set[str] = set()
        self._subscriptions_by_destination: dict[str, dict[str, BrokerSubscription]] = {}
        self._subscriptions_by_id: dict[str, BrokerSubscription] = {}
        self._lock = asyncio.Lock()

    async def list_topics(self) -> list[str]:
        async with self._lock:
            return sorted(self._topics)

    async def create_topic(self, destination: str) -> None:
        async with self._lock:
            self._topics.add(destination)
            self._subscriptions_by_destination.setdefault(destination, {})

    async def delete_topic(self, destination: str) -> None:
        async with self._lock:
            subscriptions = self._subscriptions_by_destination.pop(destination, {})
            self._topics.discard(destination)
            for subscription in subscriptions.values():
                self._subscriptions_by_id.pop(subscription.subscription_id, None)

    async def subscribe(self, destination: str, subscription_id: str | None = None) -> BrokerSubscription:
        async with self._lock:
            self._topics.add(destination)
            sid = subscription_id or str(uuid4())
            if sid in self._subscriptions_by_id:
                self._unsubscribe_unlocked(sid)
            subscription = BrokerSubscription(
                subscription_id=sid,
                destination=destination,
                queue=asyncio.Queue(),
            )
            self._subscriptions_by_id[sid] = subscription
            self._subscriptions_by_destination.setdefault(destination, {})[sid] = subscription
            return subscription

    async def unsubscribe(self, subscription_id: str) -> None:
        async with self._lock:
            self._unsubscribe_unlocked(subscription_id)

    def _unsubscribe_unlocked(self, subscription_id: str) -> None:
        subscription = self._subscriptions_by_id.pop(subscription_id, None)
        if subscription is None:
            return
        destination_map = self._subscriptions_by_destination.get(subscription.destination)
        if destination_map is not None:
            destination_map.pop(subscription_id, None)

    async def publish(self, destination: str, body: str, headers: dict[str, str] | None = None) -> tuple[BrokerMessage, int]:
        async with self._lock:
            self._topics.add(destination)
            message = BrokerMessage(
                message_id=str(uuid4()),
                destination=destination,
                body=body,
                headers=headers or {},
                published_at=datetime.now(timezone.utc).isoformat(),
            )
            subscriptions = list(self._subscriptions_by_destination.get(destination, {}).values())
            delivered_to = len(subscriptions)
            for subscription in subscriptions:
                subscription.queue.put_nowait(message)
            return message, delivered_to

    async def poll(self, subscription_id: str, timeout_seconds: float = 5.0) -> BrokerMessage | None:
        async with self._lock:
            subscription = self._subscriptions_by_id.get(subscription_id)
        if subscription is None:
            return None
        try:
            return await asyncio.wait_for(subscription.queue.get(), timeout=timeout_seconds)
        except TimeoutError:
            return None
