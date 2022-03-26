import asyncio
from typing import TYPE_CHECKING, Any, AsyncIterator, Mapping, Optional, Sequence

from ansq.tcp.connection import NSQConnection
from ansq.tcp.types import Client

if TYPE_CHECKING:
    from ansq.tcp.types import NSQMessage


class Reader(Client):
    """A consumer that provides an interface for reading messages from nsqd."""

    def __init__(
        self,
        topic: str,
        channel: str,
        nsqd_tcp_addresses: Sequence[str],
        connection_options: Mapping[str, Any] = None,
    ):
        super().__init__(
            nsqd_tcp_addresses=nsqd_tcp_addresses,
            connection_options=connection_options,
        )

        self._topic = topic
        self._channel = channel

        # Common message queue for all connections
        self._message_queue: "asyncio.Queue[Optional[NSQMessage]]" = asyncio.Queue()

    async def messages(self) -> AsyncIterator["NSQMessage"]:
        """Return messages from message queue."""
        while True:
            message = await self.wait_for_message()

            # One of connection is closed
            if message is None:
                # Don't close generator if auto-reconnect enabled
                if self._is_auto_reconnect_enabled:
                    continue
                else:
                    break

            yield message

    async def wait_for_message(self) -> Optional["NSQMessage"]:
        """Return a message from message queue."""
        return await self.message_queue.get()

    @property
    def topic(self) -> str:
        """Return a subscribed topic."""
        return self._topic

    @property
    def channel(self) -> str:
        """Return a subscribed channel."""
        return self._channel

    @property
    def message_queue(self) -> "asyncio.Queue[Optional['NSQMessage']]":
        """Return a message queue."""
        return self._message_queue

    @property
    def max_in_flight(self) -> int:
        """Update 'max_in_flight' number.

        Currently, it equals to number of current connections where every connection
        has RDY=1.
        """
        return len(self._connections)

    async def set_max_in_flight(self, count: int) -> None:
        """Update 'max_in_flight' number.

        The max_in_flight is the number of messages the reader can receive before
        nsqd expects a response. It effects how RDY state is managed. For more detail
        see the doc: https://nsq.io/clients/building_client_libraries.html#rdy-state
        """
        raise NotImplementedError("Update max_in_flight not implemented yet")

    async def _connect_to_nsqd(self, host: str, port: int) -> None:
        """Connect to nsqd by given host and port."""
        connection = NSQConnection(
            host=host,
            port=port,
            message_queue=self._message_queue,
            **self._connection_options,
        )

        if connection.id in self._connections:
            return

        await connection.connect()
        await connection.identify()
        await connection.subscribe(topic=self._topic, channel=self._channel)

        self._connections[connection.id] = connection

    @property
    def _is_auto_reconnect_enabled(self) -> bool:
        return self._connection_options.get("auto_reconnect", True)


async def create_reader(
    topic: str,
    channel: str,
    nsqd_tcp_addresses: Sequence[str],
    connection_options: Mapping[str, Any] = None,
) -> Reader:
    """Return created and connected reader."""
    reader = Reader(
        topic=topic,
        channel=channel,
        nsqd_tcp_addresses=nsqd_tcp_addresses,
        connection_options=connection_options,
    )
    await reader.connect()
    return reader
