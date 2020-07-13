# ansq - Async NSQ
[![PyPI version](https://badge.fury.io/py/ansq.svg)](https://badge.fury.io/py/ansq)
![Tests](https://github.com/list-family/ansq/workflows/Test/badge.svg)
[![Coverage](https://codecov.io/gh/list-family/ansq/branch/master/graph/badge.svg)](https://codecov.io/gh/list-family/ansq)  
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ansq)

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
### Consumer:
```python
import asyncio
from ansq import open_connection
from ansq.tcp.connection import NSQConnection
from ansq.tcp.types import NSQMessage


async def main():
    print(await ansq.pub('test_topic', 'test_message'))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
    print(await ansq.dpub('test_topic', 'test_message', 30))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>
    print(await ansq.mpub('test_topic', list('test_message')))
    # <NSQResponseSchema frame_type:FrameType.RESPONSE, body:b'OK', is_ok:True>

    await ansq.subscribe('test_topic', 'channel1', 2)

    @ansq.register_handler
    async def handler(message: NSQMessage):
        # message.body is bytes, __str__ method decodes bytes
        #
        # Something do with messages...
        # Then, mark as processed it
        print('Message: ' + str(message))
        await message.fin()

    while True:
        try:
            await handler()
        except Exception as e:
            print(e)

        # If you doesn't break the loop
        # and doesn't set auth_reconnect parameter to False,
        # but you have reached this point,
        # it's means that the NSQ connection is lost
        # and cannot reconnect.
        #
        # Errors in ansq package are logging with ERROR level,
        # so you can see errors in console.
        print('Connection status: ' + str(ansq.status))
        # Prints one of this:
        # Connection status: ConnectionStatus.CLOSING
        # Connection status: ConnectionStatus.CLOSED

        # You can reconnect here in try-except block
        # or just leave the function and finish the program.
        # It's all depends on the design of your application.
        try:
            await ansq.reconnect()
        except Exception as e:
            print(e)
            return


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    ansq: NSQConnection = loop.run_until_complete(open_connection())

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass

    # You should close connection correctly
    loop.run_until_complete(ansq.close())

```
