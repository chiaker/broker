from __future__ import annotations

import time

import httpx


class BrokerRestClient:
    """REST client for the broker with optional reconnection on network errors."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        max_retries: int = 5,
        retry_delay: float = 1.0,
    ) -> None:
        self._base_url = base_url
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def _reconnect(self) -> None:
        self._client.close()
        self._client = httpx.Client(base_url=self._base_url, timeout=self._timeout)

    # ── topics ────────────────────────────────────────────────────────────────

    def create_topic(self, name: str) -> dict:
        response = self._client.post("/topics", json={"name": name})
        response.raise_for_status()
        return response.json()

    def list_topics(self) -> dict:
        response = self._client.get("/topics")
        response.raise_for_status()
        return response.json()

    # ── publish ───────────────────────────────────────────────────────────────

    def publish(self, destination: str, body: str, headers: dict[str, str] | None = None) -> dict:
        topic_name = destination.replace("/topic/", "", 1)
        response = self._client.post(
            f"/topics/{topic_name}/publish",
            json={"body": body, "headers": headers or {}},
        )
        response.raise_for_status()
        return response.json()

    # ── subscriptions ─────────────────────────────────────────────────────────

    def subscribe(self, destination: str, subscription_id: str | None = None) -> dict:
        """Subscribe to *destination*.

        Pass a stable *subscription_id* to resume from the last saved offset
        after a broker restart or client reconnection.
        """
        payload: dict = {"destination": destination}
        if subscription_id is not None:
            payload["subscription_id"] = subscription_id
        response = self._client.post("/subscriptions", json=payload)
        response.raise_for_status()
        return response.json()

    def poll(self, subscription_id: str, timeout: float = 5.0) -> dict | None:
        response = self._client.get(
            f"/subscriptions/{subscription_id}/poll", params={"timeout": timeout}
        )
        response.raise_for_status()
        return response.json()

    def unsubscribe(self, subscription_id: str) -> None:
        response = self._client.delete(f"/subscriptions/{subscription_id}")
        response.raise_for_status()

    # ── resilient poll ────────────────────────────────────────────────────────

    def poll_with_retry(
        self,
        subscription_id: str,
        destination: str,
        timeout: float = 5.0,
    ) -> dict | None:
        """Poll for a message, reconnecting transparently on network errors.

        If the broker restarted and lost the subscription (HTTP 404), the
        client re-subscribes with the same *subscription_id* so the broker
        resumes delivery from the persisted offset.
        """
        delay = self._retry_delay
        for attempt in range(self._max_retries):
            try:
                return self.poll(subscription_id, timeout)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    # Broker restarted — re-subscribe with the stable ID.
                    self.subscribe(destination, subscription_id=subscription_id)
                    continue
                raise
            except (
                httpx.ConnectError,
                httpx.ReadError,
                httpx.RemoteProtocolError,
                httpx.NetworkError,
            ):
                if attempt == self._max_retries - 1:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
                self._reconnect()
        return None
