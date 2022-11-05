import asyncio

import ansq


async def main():
    writer = await ansq.create_writer()
    await writer.pub(
        topic="example_topic",
        message="Hello, world!",
    )
    await writer.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
