"""Demo publisher — sends order events to /topic/orders every 2 seconds."""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from itertools import cycle
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))
from sdk.client import BrokerRestClient

BROKER_URL = "http://localhost:8000"
DESTINATION = "/topic/orders"
INTERVAL = 2.0
RETRY_MIN = 2.0
RETRY_MAX = 30.0

ORDERS = cycle([
    {"order_id": "ORD-001", "product": "Laptop",     "amount": 89900, "customer": "Alice"},
    {"order_id": "ORD-002", "product": "Mouse",       "amount":  1500, "customer": "Bob"},
    {"order_id": "ORD-003", "product": "Keyboard",    "amount":  4200, "customer": "Carol"},
    {"order_id": "ORD-004", "product": "Monitor",     "amount": 32000, "customer": "Dave"},
    {"order_id": "ORD-005", "product": "Headphones",  "amount":  8750, "customer": "Eve"},
])


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [PUBLISHER]  {msg}", flush=True)


def ensure_topic(client: BrokerRestClient, retry_delay: float) -> float:
    while True:
        try:
            client.create_topic("orders")
            log(f"Topic {DESTINATION} ready")
            return RETRY_MIN
        except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError, httpx.NetworkError):
            log(f"Broker unreachable — retry in {retry_delay:.0f}s")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, RETRY_MAX)
            client._reconnect()


def main() -> None:
    client = BrokerRestClient(BROKER_URL)
    log(f"Connecting to broker at {BROKER_URL}")

    retry_delay = ensure_topic(client, RETRY_MIN)
    log("Starting publish loop (Ctrl+C to stop)")
    print()

    seq = 0
    try:
        for order in ORDERS:
            seq += 1
            body = json.dumps({**order, "seq": seq})
            while True:
                try:
                    result = client.publish(
                        DESTINATION,
                        body=body,
                        headers={"content-type": "application/json", "source": "publisher"},
                    )
                    retry_delay = RETRY_MIN
                    break
                except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError, httpx.NetworkError):
                    log(f"Broker unreachable — retry in {retry_delay:.0f}s  (seq={seq:03d} not sent yet)")
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, RETRY_MAX)
                    client._reconnect()
                    retry_delay = ensure_topic(client, retry_delay)

            log(
                f"-> Published  seq={seq:03d}  order_id={order['order_id']}"
                f"  product={order['product']:<12}"
                f"  amount={order['amount']/100:>8.2f} RUB"
                f"  delivered_to={result['delivered_to']}"
                f"  msg_id={result['message_id'][:8]}..."
            )
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        log("Stopped by user")
    finally:
        client.close()


if __name__ == "__main__":
    main()
