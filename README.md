# ansq - Async NSQ
Written with native Asyncio NSQ package

## How to

## Examples

Write and read messages:
```python
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

```

Output:
```
<NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
<NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
<NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
Message #0: test_message
Message #1: message
Message #2: test_message1, timestamp=1590082994.094573
Message #3: test_message1, timestamp=1590082994.101988
Message #4: test_message
Message #5: message
Message #6: test_message
Message #7: test_message
Message #8: t
Message #9: test_message1, timestamp=1590086643.648776
e
<NSQMessage id="0d406ce4661af003", body="e", attempts=1, timestamp=1590162134305413767, timeout=60000, initialized_at=1590162194.8242455, is_timed_out=False, is_processed=False>
```
