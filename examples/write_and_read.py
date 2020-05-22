import asyncio
from ansq import open_connection


async def main():
    nsq = await open_connection()
    print(await nsq.pub('test_topic', 'test_message'))
    print(await nsq.dpub('test_topic', 'test_message', 3))
    print(await nsq.mpub('test_topic', list('test_message')))

    await nsq.subscribe('test_topic', 'channel1', 2)
    processed_messages = 0
    async for message in nsq.messages():
        await message.fin()
        print('Message #{}: {}'.format(processed_messages, message.body))
        processed_messages += 1

        if processed_messages == 10:
            break

    single_message = await nsq.wait_for_message()
    print(single_message)

    # Also it has real good repr
    print(repr(single_message))

    # Very long task
    # ...
    # We need to touch message or it will be timed out
    await single_message.touch()
    # Continue very long task
    # ...

    # Something went wrong in task
    # in except handler re-queue message
    await single_message.req()

    # Connection should be closed
    await nsq.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
