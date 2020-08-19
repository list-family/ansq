import asyncio

from ansq import open_connection


async def main():
    nsq = await open_connection()
    print(await nsq.pub("test_topic", "test_message"))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
    print(await nsq.dpub("test_topic", "test_message", 3))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
    print(await nsq.mpub("test_topic", list("test_message")))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>

    await nsq.subscribe("test_topic", "channel1", 2)
    processed_messages = 0
    async for message in nsq.messages():
        print(f"Message #{processed_messages}: {message}")
        # Message #0: test_message
        # Message #1: t
        # Message #2: e
        # Message #3: s
        # Message #4: t
        # ...
        # Message #10: test_message
        await message.fin()
        processed_messages += 1

        if processed_messages == 10:
            break

    single_message = await nsq.wait_for_message()
    print("Single message: " + str(single_message))
    # message.body is bytes,
    # __str__ method decodes bytes
    # Prints decoded message.body

    # Also it has real good repr
    print(repr(single_message))
    # <NSQMessage id="0d406ce4661af003", body=b'e', attempts=1,
    #     timestamp=1590162134305413767, timeout=60000,
    #     initialized_at=1590162194.8242455, is_timed_out=False,
    #     is_processed=False>

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


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
