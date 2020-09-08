import abc
import asyncio
import logging
from asyncio.events import AbstractEventLoop
from asyncio.streams import StreamReader, StreamWriter
from collections import deque
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Deque, Optional, Tuple, Union

from ansq.typedefs import TCPResponse

if TYPE_CHECKING:
    from ansq.tcp.types import ConnectionStatus, NSQMessage, NSQMessageSchema


class TCPConnection(abc.ABC):
    instances_count = 0

    def __init__(
        self,
        host: str = "localhost",
        port: int = 4150,
        *,
        message_queue: asyncio.Queue = None,
        on_message: Callable = None,
        on_exception: Callable = None,
        loop: AbstractEventLoop = None,
        auto_reconnect: bool = True,
        heartbeat_interval: int = 30000,
        feature_negotiation: bool = True,
        tls_v1: bool = False,
        snappy: bool = False,
        deflate: bool = False,
        deflate_level: int = 6,
        sample_rate: int = 0,
        debug: bool = False,
        logger: logging.Logger = None,
    ):
        from ansq.tcp.protocol import Reader
        from ansq.tcp.types import ConnectionStatus
        from ansq.utils import get_logger

        self.instance_number = self.__class__.instances_count
        self.__class__.instances_count += 1

        self._host, self._port = host, port
        self._loop: AbstractEventLoop = loop or asyncio.get_event_loop()
        self._debug = debug
        self.logger = logger or get_logger(
            debug, f"{self._host}:{self._port}.{self.instance_number}",
        )

        self._message_queue: asyncio.Queue[
            Optional["NSQMessage"]
        ] = message_queue or asyncio.Queue()
        self._status: ConnectionStatus = ConnectionStatus.INIT
        self._reader: Optional[StreamReader] = None
        self._writer: Optional[StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._auto_reconnect = auto_reconnect

        self._parser = Reader()

        self._config: Union[dict, str] = {
            "deflate": deflate,
            "deflate_level": deflate_level,
            "sample_rate": sample_rate,
            "snappy": snappy,
            "tls_v1": tls_v1,
            "heartbeat_interval": heartbeat_interval,
            "feature_negotiation": feature_negotiation,
        }

        self._last_message_time: Optional[datetime] = None
        # Next queue is used for nsq commands
        self._cmd_waiters: Deque[
            Tuple[asyncio.Future, Optional[Callable[[TCPResponse], Any]]]
        ] = deque()
        # Mark connection in upgrading state to ssl socket
        self._is_upgrading = False
        # Number of received but not acked or req messages
        self._in_flight = 0
        self._secret: Optional[str] = None
        self._is_auth_required = False
        self._is_authorized = False

        # Handlers
        self._on_message = on_message
        self._on_exception = on_exception

        # Reader setup
        self._topic: Optional[str] = None
        self._channel: Optional[str] = None
        self.rdy_messages_count: int = 1
        self._is_subscribed = False

    def __repr__(self) -> str:
        return "<{class_name}: endpoint={endpoint}, status={status}>".format(
            class_name=self.__class__.__name__,
            endpoint=self.endpoint,
            status=self.status,
        )

    @property
    def status(self) -> "ConnectionStatus":
        return self._status

    @property
    def endpoint(self) -> str:
        return f"tcp://{self._host}:{self._port}"

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def message_queue(self) -> "asyncio.Queue[Optional[NSQMessage]]":
        return self._message_queue

    @property
    def last_message(self) -> Optional[datetime]:
        return self._last_message_time

    @property
    def is_subscribed(self) -> bool:
        return self._is_subscribed

    @property
    def subscribed_topic(self) -> Optional[str]:
        return self._topic

    @property
    def subscribed_channel(self) -> Optional[str]:
        return self._channel

    @property
    def is_auth_required(self) -> bool:
        return self._is_auth_required

    @property
    def is_authorized(self) -> bool:
        return self._is_authorized

    @property
    def is_closed(self) -> bool:
        """True if connection is closed or closing."""
        return self.status.is_closed or self._status.is_closing

    @abc.abstractmethod
    async def connect(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def reconnect(self) -> bool:
        raise NotImplementedError()

    async def close(self) -> None:
        """Cleanly close your connection (no more messages are sent)"""
        await self._do_close()

    async def cls(self) -> None:
        """Alias command for ``close()``."""
        await self.close()

    @abc.abstractmethod
    async def _do_close(
        self, exception: Optional[Exception] = None, change_status: bool = True
    ) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def execute(
        self,
        command: Union[str, bytes],
        *args: Any,
        data: Optional[Any] = None,
        callback: Callable[[TCPResponse], Any] = None,
    ) -> TCPResponse:
        raise NotImplementedError()

    @abc.abstractmethod
    async def identify(
        self, config: Optional[Union[dict, str]] = None, **kwargs: Any
    ) -> TCPResponse:
        raise NotImplementedError()

    async def _pulse(self) -> None:
        from ansq.tcp.types import NSQCommands

        await self.execute(NSQCommands.NOP)

    @abc.abstractmethod
    async def _upgrade_to_tls(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def _upgrade_to_snappy(self) -> asyncio.Future:
        raise NotImplementedError()

    @abc.abstractmethod
    def _upgrade_to_deflate(self) -> asyncio.Future:
        raise NotImplementedError()

    @abc.abstractmethod
    async def _read_data_task(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def _parse_data(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def _on_message_hook(self, response: "NSQMessageSchema") -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def _read_buffer(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def _start_upgrading(self, resp: Optional[TCPResponse] = None) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def _finish_upgrading(self, resp: Optional[TCPResponse] = None) -> None:
        raise NotImplementedError()
