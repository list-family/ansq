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
