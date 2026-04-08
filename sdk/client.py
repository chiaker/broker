from __future__ import annotations

import httpx


class BrokerRestClient:
    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def create_topic(self, name: str) -> dict:
        response = self._client.post("/topics", json={"name": name})
        response.raise_for_status()
        return response.json()

    def list_topics(self) -> dict:
        response = self._client.get("/topics")
        response.raise_for_status()
        return response.json()

    def publish(self, destination: str, body: str, headers: dict[str, str] | None = None) -> dict:
        topic_name = destination.replace("/topic/", "", 1)
        response = self._client.post(
            f"/topics/{topic_name}/publish",
            json={"body": body, "headers": headers or {}},
        )
        response.raise_for_status()
        return response.json()

    def subscribe(self, destination: str) -> dict:
        response = self._client.post("/subscriptions", json={"destination": destination})
        response.raise_for_status()
        return response.json()

    def poll(self, subscription_id: str, timeout: float = 5.0) -> dict | None:
        response = self._client.get(f"/subscriptions/{subscription_id}/poll", params={"timeout": timeout})
        response.raise_for_status()
        return response.json()

    def unsubscribe(self, subscription_id: str) -> None:
        response = self._client.delete(f"/subscriptions/{subscription_id}")
        response.raise_for_status()
