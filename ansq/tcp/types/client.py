from typing import TYPE_CHECKING, Any, Dict, Mapping, Sequence, Tuple

if TYPE_CHECKING:
    from ansq.tcp.connection import NSQConnection


class Client:
    """Base class for reader and writer."""

    def __init__(
        self,
        nsqd_tcp_addresses: Sequence[str],
        connection_options: Mapping[str, Any] = None,
    ):
        self._nsqd_tcp_addresses = nsqd_tcp_addresses
        self._connection_options = (
            dict(connection_options) if connection_options is not None else {}
        )
        self._connections: Dict[str, NSQConnection] = {}

        if not self._nsqd_tcp_addresses:
            raise ValueError("nsqd_tcp_addresses must be specified")

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
        for connection in self._connections.values():
            await connection.close()

    async def connect_to_nsqd(self, host: str, port: int) -> "NSQConnection":
        """Connect and identify to nsqd by given host and port."""
        from ansq.tcp.connection import NSQConnection

        connection = NSQConnection(host=host, port=port, **self._connection_options)

        if connection.id in self._connections:
            return connection

        await connection.connect()
        await connection.identify()

        self._connections[connection.id] = connection
        return connection

    @property
    def connections(self) -> Tuple["NSQConnection", ...]:
        """Return a tuple of all instantiated connections."""
        return tuple(self._connections.values())
