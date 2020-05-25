# ansq - Async NSQ
[![PyPI version](https://badge.fury.io/py/ansq.svg)](https://badge.fury.io/py/ansq)

Written with native Asyncio NSQ package

## Features
* Full TCP wrapper
* One connection for writer and reader
* Self-healing: when the NSQ connection is lost, reconnects, sends identify 
    and auth commands, subscribes to previous topic/channel
* Many helper-methods in each class

Roadmap:
* Docs
* Lookupd tool
* HTTP API wrapper
* Deflate, Snappy compressions
* TLSv1

## How to

## Examples

Write and read messages:
```python
import asyncio
from ansq import open_connection


async def main():
    nsq = await open_connection()
    print(await nsq.pub('test_topic', 'test_message'))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
    print(await nsq.dpub('test_topic', 'test_message', 3))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
    print(await nsq.mpub('test_topic', list('test_message')))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>

    await nsq.subscribe('test_topic', 'channel1', 2)
    processed_messages = 0
    async for message in nsq.messages():
        print('Message #{}: {}'.format(processed_messages, message.body))
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
    print(single_message)
    # Prints message.body

    # Also it has real good repr
    print(repr(single_message))
    # <NSQMessage id="0d406ce4661af003", body="e", attempts=1, 
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


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

```

Consumer with :
```python
import asyncio
from ansq import open_connection
from ansq.tcp.connection import NSQConnection


async def main(nsq: NSQConnection):
    await nsq.subscribe('test_topic', 'channel1', 2)
    while True:
        async for message in nsq.messages():
            print('Message: ' + message.body)
            # Something do with messages...
            # Then, mark as processed it
            await message.fin()

        # If you doesn't break the loop
        # and doesn't set auth_reconnect parameter to False,
        # but you have reached this point,
        # it's means that the NSQ connection is lost
        # and cannot reconnect.
        #
        # Errors in ansq package are logging with ERROR level,
        # so you can see errors in console.
        print('Connection status: ' + str(nsq.status))
        # Prints one of this:
        # Connection status: ConnectionStatus.CLOSING
        # Connection status: ConnectionStatus.CLOSED

        return


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    nsq_connection = loop.run_until_complete(open_connection())

    try:
        loop.run_until_complete(main(nsq_connection))
    except KeyboardInterrupt:
        pass

    # You should close connection correctly
    loop.run_until_complete(nsq_connection.close())

```
