import json
import time
import asyncio
import sys
from datetime import datetime, timezone

BROKER = sys.argv[1]
QUEUE = "test_queue"
DURATION = int(sys.argv[2])

received = 0
latencies = []
start_time = time.time()

async def rabbit_consumer():
    global received
    import aio_pika

    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)
        queue = await channel.declare_queue(QUEUE, durable=True)

        async def callback(message: aio_pika.abc.AbstractMessage):
            global received
            data = json.loads(message.body)
            
            # Исправление: делаем оба datetime aware
            ts_str = data["ts"]
            if ts_str.endswith('Z'):
                ts_str = ts_str[:-1] + '+00:00'
            elif '+' not in ts_str and 'Z' not in ts_str:
                ts_str += '+00:00'
            
            ts = datetime.fromisoformat(ts_str)
            
            # Приводим к aware, если нужно
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            
            latency = (datetime.now(timezone.utc) - ts).total_seconds() * 1000
            latencies.append(latency)
            received += 1
            await message.ack()

        await queue.consume(callback)
        await asyncio.sleep(DURATION)

async def redis_consumer():
    global received
    import redis.asyncio as redis
    r = redis.from_url("redis://localhost")
    consumer_name = f"consumer_{asyncio.current_task().get_name()}"

    try:
        await r.xgroup_create(QUEUE, "mygroup", id="0", mkstream=True)
    except:
        pass

    start = time.time()
    while time.time() - start < DURATION:
        messages = await r.xreadgroup("mygroup", consumer_name, {QUEUE: ">"}, count=10, block=100)
        for _, msgs in messages:
            for msg_id, data in msgs:
                ts_str = data[b"ts"].decode()
                if ts_str.endswith('Z'):
                    ts_str = ts_str[:-1] + '+00:00'
                elif '+' not in ts_str:
                    ts_str += '+00:00'
                
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                
                latency = (datetime.now(timezone.utc) - ts).total_seconds() * 1000
                latencies.append(latency)
                received += 1
                await r.xack(QUEUE, "mygroup", msg_id)

    try:
        await r.xgroup_destroy(QUEUE, "mygroup")
    except:
        pass

async def main():
    print(f"Starting {BROKER.upper()} consumer for {DURATION} seconds...")
    if BROKER == "rabbit":
        await rabbit_consumer()
    else:
        await redis_consumer()

    duration = time.time() - start_time
    print(f"\n{BROKER.upper()} CONSUMER FINISHED")
    print(f"Received: {received} messages")
    print(f"Throughput: {received / duration:.1f} msg/sec" if duration > 0 else "Throughput: N/A")

    if latencies:
        avg = sum(latencies) / len(latencies)
        sorted_lat = sorted(latencies)
        p95 = sorted_lat[int(len(latencies) * 0.95)]
        print(f"Avg latency: {avg:.2f} ms")
        print(f"p95 latency: {p95:.2f} ms")
    
        # Запись результатов в файл
    with open(f"results_{BROKER}_{DURATION}.txt", "a", encoding="utf-8") as f:
        f.write(f"\n{BROKER.upper()} | Duration: {DURATION}s\n")
        f.write(f"Received: {received} messages\n")
        f.write(f"Throughput: {received / duration:.1f} msg/sec\n")
        if latencies:
            avg = sum(latencies) / len(latencies)
            p95 = sorted(latencies)[int(len(latencies)*0.95)]
            f.write(f"Avg latency: {avg:.2f} ms\n")
            f.write(f"p95 latency: {p95:.2f} ms\n")

if __name__ == "__main__":
    asyncio.run(main())