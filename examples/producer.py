import asyncio
from ansq import open_connection


async def main():
    nsq = await open_connection()
    print(await nsq.pub('test_topic', 'test_message'))
    print(await nsq.dpub('test_topic', 'test_message', 0))
    print(await nsq.mpub('test_topic', list('test_message')))

    await nsq.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
