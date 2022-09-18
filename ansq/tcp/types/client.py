from typing import TYPE_CHECKING, Dict, Sequence, Tuple

import attr

from .connection import ConnectionOptions

if TYPE_CHECKING:
    from ansq.tcp.connection import NSQConnection


class Client:
    """Base class for reader and writer."""

    def __init__(
        self,
        nsqd_tcp_addresses: Sequence[str],
        connection_options: ConnectionOptions = ConnectionOptions(),
        debug: bool = False,
    ):
        self._nsqd_tcp_addresses = nsqd_tcp_addresses

        if debug:
            connection_options = attr.evolve(connection_options, debug=True)

        self.connection_options = connection_options

        self._connections: Dict[str, NSQConnection] = {}

    async def connect(self) -> None:
        """Connect to nsqd addresses."""
        for address in self._nsqd_tcp_addresses:
            try:
                host, port = address.split(":")
            except ValueError:
                raise ValueError(f"Invalid TCP address: {address}")
            await self.connect_to_nsqd(host=host, port=int(port))

    async def close(self) -> None:
        """Close all connections."""
        for connection in self.connections:
            await connection.close()

    async def connect_to_nsqd(self, host: str, port: int) -> "NSQConnection":
        """Connect and identify to nsqd by given host and port."""
        from ansq.tcp.connection import NSQConnection

        connection = NSQConnection(
            host=host, port=port, connection_options=self.connection_options
        )

        existing_connection = self._connections.get(connection.id)
        if existing_connection is not None:
            return existing_connection

        await connection.connect()
        await connection.identify()

        self.add_connection(connection)
        return connection

    def add_connection(self, connection: "NSQConnection") -> None:
        """Add connection to connections pool."""
        self._connections[connection.id] = connection

    def remove_connection(self, connection: "NSQConnection") -> None:
        """Remove connection from connections pool."""
        if connection.id in self._connections:
            del self._connections[connection.id]

    @property
    def connections(self) -> Tuple["NSQConnection", ...]:
        """Return a tuple of all instantiated connections."""
        return tuple(self._connections.values())
