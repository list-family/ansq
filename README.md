# ansq - AsynchronousIO NSQ
## How to

### Producer
```python
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

```

Output:
```
2020-05-21 20:28:32,392 - INFO - ansq: Connect to tcp://localhost:4150 established
2020-05-21 20:28:32,394 - INFO - ansq: Connection tcp://localhost:4150 is closed
<NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
<NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
<NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
```

### Consumer
```python
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

```

Output:
```
2020-05-21 20:39:28,872 - INFO - ansq: Connect to tcp://localhost:4150 established
2020-05-21 20:39:28,877 - INFO - ansq: Connection tcp://localhost:4150 is closed
Message #0: g
Message #1: e
Message #2: test_message
Message #3: test_message
Message #4: test_message
Message #5: t
Message #6: e
Message #7: s
Message #8: t
Message #9: _
```
