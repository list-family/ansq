import asyncio

import ansq


async def main():
    reader = await ansq.create_reader(
        topic="example_topic",
        channel="example_channel",
    )

    async for message in reader.messages():
        print(f"Message: {message.body}")
        await message.fin()

    await reader.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
