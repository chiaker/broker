import asyncio
import websockets


async def run():
    subscriber = await websockets.connect("ws://127.0.0.1:8000/ws")
    publisher = await websockets.connect("ws://127.0.0.1:8000/ws")

    await subscriber.send("CONNECT\naccept-version:1.2\n\n\x00")
    sub_connected = await subscriber.recv()
    print("SUB CONNECTED:", sub_connected)

    await publisher.send("CONNECT\naccept-version:1.2\n\n\x00")
    pub_connected = await publisher.recv()
    print("PUB CONNECTED:", pub_connected)

    await subscriber.send(
        "SUBSCRIBE\nid:sub-1\ndestination:/topic/chat.general\nack:auto\n\n\x00"
    )

    await publisher.send("SEND\ndestination:/topic/chat.general\n\nhello-manual\x00")

    msg = await subscriber.recv()
    print("SUB MESSAGE:", msg)

    await publisher.send("DISCONNECT\n\n\x00")
    await subscriber.send("DISCONNECT\n\n\x00")

    await publisher.close()
    await subscriber.close()


if __name__ == "__main__":
    asyncio.run(run())