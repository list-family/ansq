import random
from typing import TYPE_CHECKING, Any, Optional, Sequence

from ansq.tcp.connection import NSQConnection
from ansq.tcp.types import Client, ConnectionOptions

if TYPE_CHECKING:
    from ansq.typedefs import TCPResponse


class Writer(Client):
    """A producer that provides an interface for publishing messages to nsqd."""

    def __init__(
        self,
        nsqd_tcp_addresses: Optional[Sequence[str]] = None,
        connection_options: ConnectionOptions = ConnectionOptions(),
    ):
        super().__init__(
            nsqd_tcp_addresses=nsqd_tcp_addresses or [],
            connection_options=connection_options,
        )

        if not self._nsqd_tcp_addresses:
            self._nsqd_tcp_addresses = ["localhost:4150"]

    async def pub(self, topic: str, message: Any) -> "TCPResponse":
        """Publish a message to a topic to a random connection."""
        conn = self._get_random_open_connection()
        return await conn.pub(topic=topic, message=message)

    async def dpub(self, topic: str, message: Any, delay_time: int) -> "TCPResponse":
        """Publish a deferred message to a topic to a random connection."""
        conn = self._get_random_open_connection()
        return await conn.dpub(topic=topic, message=message, delay_time=delay_time)

    async def mpub(self, topic: str, *messages: Any) -> "TCPResponse":
        """Publish multiple messages to a topic to a random connection."""
        conn = self._get_random_open_connection()
        return await conn.mpub(topic, *messages)

    def _get_random_open_connection(self) -> NSQConnection:
        """Return a random open connection."""
        open_connections = tuple(
            conn for conn in self._connections.values() if conn.is_connected
        )
        return random.choice(open_connections)


async def create_writer(
    nsqd_tcp_addresses: Optional[Sequence[str]] = None,
    connection_options: ConnectionOptions = ConnectionOptions(),
) -> Writer:
    """Return created and connected writer."""
    writer = Writer(
        nsqd_tcp_addresses=nsqd_tcp_addresses, connection_options=connection_options
    )
    await writer.connect()
    return writer
