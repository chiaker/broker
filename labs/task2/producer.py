import json
import time
import asyncio
import sys
import uuid
from datetime import datetime, timezone

BROKER = sys.argv[1]
QUEUE = "test_queue"
RATE = int(sys.argv[2])
MSG_SIZE = int(sys.argv[3])
DURATION = int(sys.argv[4])

payload = b"x" * (MSG_SIZE - 100)

async def main():
    print(f"Starting {BROKER} producer | Rate: {RATE} msg/sec | Size: {MSG_SIZE} bytes")

    if BROKER == "rabbit":
        import aio_pika
        connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        async with connection:
            channel = await connection.channel()
            await channel.declare_queue(QUEUE, durable=True)

            start = time.time()
            sent = 0
            while time.time() - start < DURATION:
                msg_id = str(uuid.uuid4())
                body = json.dumps({
                    "id": msg_id,
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "payload": payload.decode('latin1')
                }).encode()

                await channel.default_exchange.publish(
                    aio_pika.Message(body, delivery_mode=2),
                    routing_key=QUEUE
                )
                sent += 1
                await asyncio.sleep(1.0 / RATE)

            print(f"Rabbit producer finished. Sent: {sent}")

    else:  # redis
        import redis.asyncio as redis
        r = redis.from_url("redis://localhost")
        start = time.time()
        sent = 0
        while time.time() - start < DURATION:
            msg_id = str(uuid.uuid4())
            await r.xadd(QUEUE, {
                "id": msg_id,
                "ts": datetime.now(timezone.utc).isoformat(),
                "payload": payload.decode('latin1')
            })
            sent += 1
            await asyncio.sleep(1.0 / RATE)
        print(f"Redis producer finished. Sent: {sent}")

if __name__ == "__main__":
    asyncio.run(main())