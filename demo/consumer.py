"""Demo consumer 

Usage:
    py -3 demo/consumer.py consumer-1
    py -3 demo/consumer.py consumer-2
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from sdk.client import BrokerRestClient

BROKER_URL = "http://localhost:8000"
DESTINATION = "/topic/orders"
POLL_TIMEOUT = 5.0
RETRY_MIN = 2.0
RETRY_MAX = 30.0


def log(name: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [{name.upper():<14}] {msg}", flush=True)


def connect(client: BrokerRestClient, name: str, retry_delay: float) -> float:
    """Subscribe (or re-subscribe) with retries. Returns reset retry_delay."""
    while True:
        try:
            sub = client.subscribe(DESTINATION, subscription_id=name)
            log(name, f"Subscribed  offset resumed from disk  destination={sub['destination']}")
            return RETRY_MIN
        except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError, httpx.NetworkError):
            log(name, f"Broker unreachable — retry in {retry_delay:.0f}s")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, RETRY_MAX)
            client._reconnect()


def main() -> None:
    name = sys.argv[1] if len(sys.argv) > 1 else "consumer-1"
    client = BrokerRestClient(BROKER_URL)

    log(name, f"Connecting to broker at {BROKER_URL}")
    retry_delay = connect(client, name, RETRY_MIN)
    log(name, "Waiting for messages (Ctrl+C to stop)")
    print()

    received = 0
    try:
        while True:
            try:
                msg = client.poll(name, timeout=POLL_TIMEOUT)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    log(name, "Subscription lost (broker restarted) — re-subscribing...")
                    retry_delay = connect(client, name, retry_delay)
                    continue
                raise
            except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError, httpx.NetworkError):
                log(name, f"Broker unreachable — retry in {retry_delay:.0f}s")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, RETRY_MAX)
                client._reconnect()
                continue

            retry_delay = RETRY_MIN

            if msg is None:
                # Broker already waited POLL_TIMEOUT seconds — no extra sleep needed.
                # Small safety sleep prevents tight loop if broker misbehaves.
                time.sleep(0.1)
                continue

            received += 1
            try:
                body = json.loads(msg["body"])
                detail = (
                    f"order_id={body.get('order_id')}  "
                    f"product={body.get('product'):<12}  "
                    f"amount={body.get('amount', 0)/100:>8.2f} RUB  "
                    f"customer={body.get('customer')}"
                )
            except (json.JSONDecodeError, TypeError):
                detail = f"body={msg['body']!r}"

            log(name, f"<- msg #{received}  msg_id={msg['message_id'][:8]}...  {detail}")
            print()

    except KeyboardInterrupt:
        log(name, f"Stopped  total_received={received}")
        client.unsubscribe(name)
        log(name, "Unsubscribed  offset saved to disk")
    finally:
        client.close()


if __name__ == "__main__":
    main()
