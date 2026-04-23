from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.journal import DATA_DIR, TopicJournal


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


class InMemoryBroker:
    def __init__(self) -> None:
        self._topics: set[str] = set()
        self._subscriptions_by_destination: dict[str, dict[str, BrokerSubscription]] = {}
        self._subscriptions_by_id: dict[str, BrokerSubscription] = {}
        self._journals: dict[str, TopicJournal] = {}
        self._offsets: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def startup(self) -> None:
        """Restore topics from persisted journal files."""
        topics_dir = DATA_DIR / "topics"
        if not topics_dir.exists():
            return
        import json
        for path in topics_dir.glob("*.jsonl"):
            try:
                with path.open(encoding="utf-8") as f:
                    first = f.readline().strip()
                if not first:
                    continue
                destination = json.loads(first).get("destination")
                if destination:
                    self._topics.add(destination)
                    self._subscriptions_by_destination.setdefault(destination, {})
                    self._journals[destination] = TopicJournal(destination)
            except (OSError, ValueError):
                pass

    # ── helpers ──────────────────────────────────────────────────────────────

    def _get_journal(self, destination: str) -> TopicJournal:
        if destination not in self._journals:
            self._journals[destination] = TopicJournal(destination)
        return self._journals[destination]

    def _offset_path(self, subscription_id: str) -> Path:
        return DATA_DIR / "offsets" / f"{subscription_id}.txt"

    def _load_offset(self, subscription_id: str, destination: str) -> int | None:
        """Return saved offset only when destination matches; ignore stale cross-topic offsets."""
        path = self._offset_path(subscription_id)
        if path.exists():
            try:
                data = json.loads(path.read_text())
                if data.get("destination") == destination:
                    return int(data["offset"])
            except (ValueError, KeyError):
                pass
        return None

    def _save_offset(self, subscription_id: str, offset: int, destination: str) -> None:
        path = self._offset_path(subscription_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"offset": offset, "destination": destination}))

    # ── topic operations ──────────────────────────────────────────────────────

    async def list_topics(self) -> list[str]:
        async with self._lock:
            return sorted(self._topics)

    async def create_topic(self, destination: str) -> None:
        async with self._lock:
            self._topics.add(destination)
            self._subscriptions_by_destination.setdefault(destination, {})
            self._get_journal(destination)

    async def delete_topic(self, destination: str) -> None:
        async with self._lock:
            subscriptions = self._subscriptions_by_destination.pop(destination, {})
            self._topics.discard(destination)
            self._journals.pop(destination, None)
            for sub in subscriptions.values():
                self._subscriptions_by_id.pop(sub.subscription_id, None)
                self._offsets.pop(sub.subscription_id, None)

    # ── subscription operations ───────────────────────────────────────────────

    async def subscribe(self, destination: str, subscription_id: str | None = None) -> BrokerSubscription:
        async with self._lock:
            self._topics.add(destination)
            sid = subscription_id or str(uuid4())
            if sid in self._subscriptions_by_id:
                self._unsubscribe_unlocked(sid)
            subscription = BrokerSubscription(subscription_id=sid, destination=destination)
            self._subscriptions_by_id[sid] = subscription
            self._subscriptions_by_destination.setdefault(destination, {})[sid] = subscription
            # Resume from saved offset if available; otherwise start at current end.
            saved = self._load_offset(sid, destination)
            self._offsets[sid] = saved if saved is not None else self._get_journal(destination).size
            return subscription

    async def unsubscribe(self, subscription_id: str) -> None:
        async with self._lock:
            sub = self._subscriptions_by_id.get(subscription_id)
            offset = self._offsets.get(subscription_id)
            if sub is not None and offset is not None:
                self._save_offset(subscription_id, offset, sub.destination)
            self._unsubscribe_unlocked(subscription_id)

    def _unsubscribe_unlocked(self, subscription_id: str) -> None:
        sub = self._subscriptions_by_id.pop(subscription_id, None)
        if sub is None:
            return
        dest_map = self._subscriptions_by_destination.get(sub.destination)
        if dest_map is not None:
            dest_map.pop(subscription_id, None)
        self._offsets.pop(subscription_id, None)

    # ── publish / poll ────────────────────────────────────────────────────────

    async def publish(
        self,
        destination: str,
        body: str,
        headers: dict[str, str] | None = None,
    ) -> tuple[BrokerMessage, int]:
        async with self._lock:
            self._topics.add(destination)
            message = BrokerMessage(
                message_id=str(uuid4()),
                destination=destination,
                body=body,
                headers=headers or {},
                published_at=datetime.now(timezone.utc).isoformat(),
            )
            delivered_to = len(self._subscriptions_by_destination.get(destination, {}))
            journal = self._get_journal(destination)

        await journal.append(
            {
                "message_id": message.message_id,
                "destination": message.destination,
                "body": message.body,
                "headers": message.headers,
                "published_at": message.published_at,
            }
        )
        return message, delivered_to

    async def poll(self, subscription_id: str, timeout_seconds: float = 5.0) -> BrokerMessage | None:
        async with self._lock:
            sub = self._subscriptions_by_id.get(subscription_id)
            if sub is None:
                return None
            offset = self._offsets.get(subscription_id, 0)
            journal = self._get_journal(sub.destination)

        record = await journal.read_next(offset, timeout_seconds)
        if record is None:
            return None

        new_offset = record["offset"] + 1
        async with self._lock:
            if subscription_id in self._subscriptions_by_id:
                self._offsets[subscription_id] = new_offset
                self._save_offset(subscription_id, new_offset, record["destination"])

        return BrokerMessage(
            message_id=record["message_id"],
            destination=record["destination"],
            body=record["body"],
            headers=record.get("headers", {}),
            published_at=record["published_at"],
        )
