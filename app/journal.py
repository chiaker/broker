from __future__ import annotations

import asyncio
import json
from pathlib import Path

DATA_DIR = Path("data")


class TopicJournal:
    """Append-only JSONL log for a single topic.

    Thread-safety: asyncio.Lock guards file writes and size counter.
    Readers are notified via a broadcast Event so poll() can long-wait.
    """

    def __init__(self, destination: str) -> None:
        safe = destination.strip("/").replace("/", "_").replace(".", "_")
        self._path = DATA_DIR / "topics" / f"{safe}.jsonl"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._size = self._count_lines()
        self._new_msg: asyncio.Event = asyncio.Event()

    def _count_lines(self) -> int:
        if not self._path.exists():
            return 0
        with self._path.open(encoding="utf-8") as f:
            return sum(1 for _ in f)

    async def append(self, record: dict) -> int:
        async with self._lock:
            offset = self._size
            entry = {**record, "offset": offset}
            with self._path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
            self._size += 1
        # Broadcast: replace the event so all current waiters wake up cleanly.
        old = self._new_msg
        self._new_msg = asyncio.Event()
        old.set()
        return offset

    async def read_next(self, offset: int, timeout: float) -> dict | None:
        """Return the record at *offset*, waiting up to *timeout* seconds."""
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while True:
            async with self._lock:
                if self._size > offset:
                    return self._read_at(offset)
                waiter = self._new_msg
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            try:
                await asyncio.wait_for(waiter.wait(), timeout=remaining)
            except TimeoutError:
                return None

    def _read_at(self, offset: int) -> dict | None:
        try:
            with self._path.open(encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if i == offset:
                        return json.loads(line.strip())
        except OSError:
            pass
        return None

    @property
    def size(self) -> int:
        return self._size
