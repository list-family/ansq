from __future__ import annotations

import asyncio

from ansq import open_connection
from ansq.tcp.connection import NSQConnection


async def main(nsq: NSQConnection):
    await nsq.subscribe("test_topic", "channel1", 2)
    while True:
        async for message in nsq.messages():
            print("Message: " + str(message))
            # message.body is bytes,
            # __str__ method decodes bytes
            #
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
        print("Connection status: " + str(nsq.status))
        # Prints one of this:
        # Connection status: ConnectionStatus.CLOSING
        # Connection status: ConnectionStatus.CLOSED

        # You can reconnect here in try-except block
        # or just leave the function and finish the program.
        # It's all depends on the design of your application.
        return


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    nsq_connection = loop.run_until_complete(open_connection())

    try:
        loop.run_until_complete(main(nsq_connection))
    except KeyboardInterrupt:
        pass

    # You should close connection correctly
    loop.run_until_complete(nsq_connection.close())
