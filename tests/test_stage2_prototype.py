from fastapi.testclient import TestClient

from app.main import app


def test_rest_fanout_two_subscribers_receive_same_message() -> None:
    client = TestClient(app)
    destination = "/topic/news.sport"

    first = client.post("/subscriptions", json={"destination": destination})
    second = client.post("/subscriptions", json={"destination": destination})

    first.raise_for_status()
    second.raise_for_status()

    first_id = first.json()["subscription_id"]
    second_id = second.json()["subscription_id"]

    publish = client.post("/topics/news.sport/publish", json={"body": "goal", "headers": {"x-id": "1"}})
    publish.raise_for_status()
    assert publish.json()["delivered_to"] == 2

    first_msg = client.get(f"/subscriptions/{first_id}/poll", params={"timeout": 0.1})
    second_msg = client.get(f"/subscriptions/{second_id}/poll", params={"timeout": 0.1})

    first_msg.raise_for_status()
    second_msg.raise_for_status()

    assert first_msg.json()["body"] == "goal"
    assert second_msg.json()["body"] == "goal"
    assert first_msg.json()["destination"] == destination
    assert second_msg.json()["destination"] == destination


def test_stomp_websocket_publish_and_subscribe() -> None:
    client = TestClient(app)
    destination = "/topic/chat.general"

    with client.websocket_connect("/ws") as subscriber:
        subscriber.send_text("CONNECT\naccept-version:1.2\n\n\x00")
        connected = subscriber.receive_text()
        assert connected.startswith("CONNECTED")

        subscriber.send_text(f"SUBSCRIBE\nid:sub-1\ndestination:{destination}\nack:auto\n\n\x00")

        with client.websocket_connect("/ws") as publisher:
            publisher.send_text("CONNECT\naccept-version:1.2\n\n\x00")
            _ = publisher.receive_text()
            publisher.send_text(f"SEND\ndestination:{destination}\n\nhello-ws\x00")

        message = subscriber.receive_text()
        assert message.startswith("MESSAGE")
        assert "destination:/topic/chat.general" in message
        assert "hello-ws" in message
