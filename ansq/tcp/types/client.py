import abc
from typing import TYPE_CHECKING, Any, Dict, Mapping, Sequence, Tuple

if TYPE_CHECKING:
    from ansq.tcp.connection import NSQConnection


class Client(abc.ABC):
    """A basic interface for reader and writer."""

    def __init__(
        self,
        nsqd_tcp_addresses: Sequence[str],
        connection_options: Mapping[str, Any] = None,
    ):
        self._nsqd_tcp_addresses = nsqd_tcp_addresses
        self._connection_options = connection_options or {}
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
            await self._connect_to_nsqd(host=host, port=int(port))

    async def close(self) -> None:
        """Close all connections."""
        for connection in self._connections.values():
            await connection.close()

    @abc.abstractmethod
    async def _connect_to_nsqd(self, host: str, port: int) -> None:
        """Connect to nsqd by given host and port."""

    @property
    def connections(self) -> Tuple["NSQConnection", ...]:
        return tuple(self._connections.values())
