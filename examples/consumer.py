import asyncio
from ansq import open_connection


async def main():
    nsq = await open_connection()
    await nsq.pub('test_topic', 'test_message')

    await nsq.subscribe('test_topic', 'channel1', 2)
    processed_messages = 0
    async for message in nsq.messages():
        await message.fin()
        print('Message #{}: {}'.format(processed_messages, message.body))
        processed_messages += 1

        if processed_messages == 10:
            await nsq.cls()

    await nsq.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
