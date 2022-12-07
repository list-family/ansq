import asyncio
import contextlib
import random
from asyncio import AbstractEventLoop
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    List,
    NamedTuple,
    NoReturn,
    Optional,
    Sequence,
)

import attr

from ansq.http import NsqLookupd
from ansq.tcp.types import Client, ConnectionOptions
from ansq.utils import get_logger

if TYPE_CHECKING:
    from ansq.tcp.connection import NSQConnection
    from ansq.tcp.types import NSQMessage


class Reader(Client):
    """A consumer that provides an interface for reading messages from nsqd."""

    def __init__(
        self,
        topic: str,
        channel: str,
        nsqd_tcp_addresses: Optional[Sequence[str]] = None,
        lookupd_http_addresses: Optional[Sequence[str]] = None,
        lookupd_poll_interval: float = 60000,
        lookupd_poll_jitter: float = 0.3,
        connection_options: ConnectionOptions = ConnectionOptions(),
        loop: Optional[AbstractEventLoop] = None,
    ):
        if nsqd_tcp_addresses is None:
            nsqd_tcp_addresses = []

        super().__init__(
            nsqd_tcp_addresses=nsqd_tcp_addresses or [],
            connection_options=connection_options,
        )

        if not any((self._nsqd_tcp_addresses, lookupd_http_addresses)):
            self._nsqd_tcp_addresses = ["localhost:4150"]

        self._topic = topic
        self._channel = channel
        self._loop = loop or asyncio.get_event_loop()
        self._lookupd: Optional["Lookupd"] = None

        # Common message queue for all connections
        self._message_queue: "asyncio.Queue[Optional[NSQMessage]]" = asyncio.Queue()
        self.connection_options = attr.evolve(
            self.connection_options, message_queue=self._message_queue
        )

        # Init lookupd
        if lookupd_http_addresses:
            self._lookupd = Lookupd(
                reader=self,
                http_addresses=lookupd_http_addresses,
                poll_interval=lookupd_poll_interval,
                poll_jitter=lookupd_poll_jitter,
                loop=self._loop,
                debug=self.connection_options.debug,
            )

    async def connect(self) -> None:
        """Connect to nsqd addresses.

        Queries lookupd if specified.
        """
        await super().connect()

        if self._lookupd:
            # Do first lookup manually
            await self._lookupd.query_lookup()
            await self._lookupd.start_polling()

    async def messages(self) -> AsyncIterator["NSQMessage"]:
        """Return messages from message queue."""
        while True:
            message = await self.wait_for_message()

            # One of connection is closed
            if message is None:
                # Keep the generator alive if lookupd is enabled as the reader
                # would restore or discover new connections
                if self._lookupd is not None:
                    continue

                # Keep the generator alive if auto-reconnect is enabled, as the
                # connection would be restored later by itself
                if self._is_auto_reconnect_enabled:
                    continue

                # Otherwise close the generator
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
        """Return 'max_in_flight' number.

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

    async def connect_to_nsqd(self, host: str, port: int) -> "NSQConnection":
        """Connect, identify and subscribe to nsqd by given host and port."""
        connection = await super().connect_to_nsqd(host=host, port=port)
        if not connection.is_subscribed:
            await connection.subscribe(topic=self._topic, channel=self._channel)
        return connection

    @property
    def _is_auto_reconnect_enabled(self) -> bool:
        return self.connection_options.auto_reconnect

    async def close(self) -> None:
        """Close all connections."""
        if self._lookupd is not None:
            await self._lookupd.close()

        await super().close()


class Lookupd:
    """Lookupd wrapper helps to connect to found producers via lookupd services."""

    def __init__(
        self,
        reader: Reader,
        http_addresses: Sequence[str],
        poll_interval: float,
        poll_jitter: float,
        loop: Optional[AbstractEventLoop] = None,
        debug: bool = False,
    ):
        self._reader = reader
        self._poll_interval = poll_interval / 1000
        self._poll_jitter = poll_jitter
        self._loop = loop or asyncio.get_event_loop()
        self._query_lookupd_attempts = 0
        self._logger = get_logger(debug, "lookupd")
        self._debug = debug
        self._poll_lookup_task: Optional[asyncio.Task] = None

        # Keep original on close callback to call it in `self._on_close_connection`
        self._orig_on_close_callback = self._reader.connection_options.on_close

        # Configure connections specific for lookupd
        self._reader.connection_options = attr.evolve(
            self._reader.connection_options,
            # Lookupd adds and removes connections itself,
            # so disable auto-reconnect for each connections.
            auto_reconnect=False,
            # When a connection is closed it should be removed from the reader.
            # Lookupd would add it later if the producer is up.
            on_close=self._on_close_connection,
        )

        # Create lookupd connections
        self._lookupd_connections = [
            NsqLookupd.from_address(address) for address in http_addresses
        ]

    async def query_lookup(self) -> None:
        """Query lookupd for topic producers and connect to them."""
        # Get lookupd connection in a round robin fashion way
        lookupd_connection = self._get_lookupd_connection()

        # Try to query lookupd connection
        try:
            producer_addresses = await self._do_query_lookup(lookupd_connection)
        except Exception as exc:
            self._logger.error(
                "Failed to query lookupd %s due to: %s",
                lookupd_connection,
                exc,
                exc_info=exc if self._debug else False,
            )
            return

        # Connect to all producers addresses
        for address in producer_addresses:
            await self._reader.connect_to_nsqd(address.host, address.port)

    async def poll_lookup(self) -> NoReturn:
        """Poll ``query_lookup()`` infinitely."""
        # Add a delay to poll which helps to distribute evenly requests
        # even if multiple readers restart at the same time.
        delay = self._poll_interval * self._poll_jitter
        await asyncio.sleep(random.random() * delay)

        # Poll infinitely lookup
        while True:
            await asyncio.sleep(self._poll_interval)
            await self.query_lookup()

    async def start_polling(self) -> None:
        """Start polling lookupd."""
        # Polling is already started
        if self._poll_lookup_task is not None and not self._poll_lookup_task.done():
            return

        # Start polling task
        self._poll_lookup_task = self._loop.create_task(self.poll_lookup())

    async def stop_polling(self) -> None:
        """Stop polling lookupd."""
        if self._poll_lookup_task is None:
            return

        self._poll_lookup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._poll_lookup_task

    async def close(self) -> None:
        """Close all lookupd connections and stop poll lookup task."""
        for lookupd_connection in self._lookupd_connections:
            await lookupd_connection.close()

        await self.stop_polling()

    def _get_lookupd_connection(self) -> "NsqLookupd":
        """Return lookupd connection in a round robin fashion way."""
        index = self._query_lookupd_attempts % len(self._lookupd_connections)
        lookupd_connection = self._lookupd_connections[index]
        self._query_lookupd_attempts += 1
        return lookupd_connection

    async def _do_query_lookup(
        self, lookupd_connection: "NsqLookupd"
    ) -> List["Address"]:
        """Query lookup with a given connection and return producer addresses."""
        # Lookup for the reader's topic
        self._logger.debug("Query %s", lookupd_connection)
        lookup_response = await lookupd_connection.lookup(self._reader.topic)
        if not isinstance(lookup_response, dict):
            raise ValueError(f"lookup response must be a dict: {lookup_response}")

        # Get producers from the result response
        return self._get_producer_addresses(lookup_response)

    @staticmethod
    def _get_producer_addresses(response: Dict[str, Any]) -> List["Address"]:
        """Return a list of producers addresses from parsed lookup response."""
        producers = response.get("producers")
        if producers is None:
            raise ValueError("producers not found in response data")

        if not isinstance(producers, list):
            raise ValueError(f"producers must be a list: {producers}")

        addresses: List[Address] = []

        for producer in producers:
            if not isinstance(producer, dict):
                raise ValueError(f"producer must be a dict: {producer}")

            host = producer["broadcast_address"]
            port = int(producer["tcp_port"])
            addresses.append(Address(host, port))

        return addresses

    def _on_close_connection(self, connection: "NSQConnection") -> None:
        """A callback to be called after a connection being closed."""
        # Remove the connection from the reader so that lookupd could add it later
        self._reader.remove_connection(connection)

        # Call an original on_close callback if specified
        if self._orig_on_close_callback is not None:
            self._orig_on_close_callback(connection)


class Address(NamedTuple):
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


async def create_reader(
    topic: str,
    channel: str,
    nsqd_tcp_addresses: Optional[Sequence[str]] = None,
    lookupd_http_addresses: Optional[Sequence[str]] = None,
    lookupd_poll_interval: float = 60000,
    lookupd_poll_jitter: float = 0.3,
    connection_options: ConnectionOptions = ConnectionOptions(),
) -> Reader:
    """Return created and connected reader."""
    reader = Reader(
        topic=topic,
        channel=channel,
        nsqd_tcp_addresses=nsqd_tcp_addresses,
        lookupd_http_addresses=lookupd_http_addresses,
        lookupd_poll_interval=lookupd_poll_interval,
        lookupd_poll_jitter=lookupd_poll_jitter,
        connection_options=connection_options,
    )
    await reader.connect()
    return reader
