import asyncio
import json
import logging
import sys
from asyncio.events import AbstractEventLoop
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Callable, Optional, Union

from ansq.tcp import consts
from ansq.tcp.exceptions import (
    ConnectionClosedError,
    NSQUnauthorized,
    ProtocolError,
    get_exception,
)
from ansq.tcp.types import (
    ConnectionStatus,
    NSQCommands,
    NSQErrorSchema,
    NSQMessage,
    NSQMessageSchema,
    NSQResponseSchema,
)
from ansq.tcp.types import TCPConnection as NSQConnectionBase
from ansq.typedefs import TCPResponse
from ansq.utils import truncate_text, validate_topic_channel_name


class NSQConnection(NSQConnectionBase):
    async def connect(self) -> bool:
        """Open connection"""
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port,
        )

        self._writer.write(NSQCommands.MAGIC_V2)
        self._status = ConnectionStatus.CONNECTED
        self.logger.debug(f"Connect to {self.endpoint} established")

        self._reader_task = self._loop.create_task(self._read_data_task())

        return True

    async def reconnect(self, raise_error: bool = True) -> bool:
        """Reconnect method will reopen the connection,
        send the ``identify`` command with your or default config,
        authorize you if you were authorized
        and resubscribe to previous topic/channel.

        So, after that command you can continue to work
        with NSQ like nothing is happened.

        :param raise_error: If ``False``, method will log exception
            and return the ``bool`` value anyway.
        :type raise_error: :class:`bool`

        :returns: Reconnect successful status.
        """
        self.logger.debug(f"Reconnecting to {self.endpoint}...")
        self._status = ConnectionStatus.RECONNECTING

        await self._do_close(change_status=False)
        try:
            await self.connect()
            await self.identify()
            self._secret and await self.auth(self._secret)
            if self._is_subscribed:
                assert self._topic is not None
                assert self._channel is not None
                await self.subscribe(
                    self._topic, self._channel, self.rdy_messages_count,
                )
        except Exception as e:
            if raise_error:
                raise e

            await self._do_close(e)
            return False

        self.logger.debug(f"Reconnected to {self.endpoint}")
        self._status = ConnectionStatus.CONNECTED
        return True

    async def _do_close(
        self, exception: Optional[Exception] = None, change_status: bool = True
    ) -> None:
        if self.is_closed or self.status.is_init or self.status.is_closing:
            return

        if change_status:
            self._status = ConnectionStatus.CLOSING

        if exception:
            self.logger.error(
                "Connection {} is closing due an error: {}".format(
                    self.endpoint, exception,
                ),
            )
        else:
            self.logger.debug(f"Connection {self.endpoint} is closing...")

        if self.is_subscribed and change_status:
            self._is_subscribed = False
            while self._message_queue.qsize() > 0:
                self._message_queue.get_nowait()
            self._message_queue.put_nowait(None)

        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except Exception as e:
                self.logger.exception(e)

        if change_status and self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except Exception as e:
                self.logger.exception(e)

        assert self._writer is not None
        try:
            self._writer.close()
            if sys.version_info >= (3, 7):
                await self._writer.wait_closed()
        except Exception as e:
            self.logger.exception(e)

        for future, callback in self._cmd_waiters:
            if not future.cancelled():
                future.set_exception(ConnectionClosedError("Connection is closed"))
                callback is not None and callback(None)

        if self._message_queue.qsize() > 0:
            self._message_queue.get_nowait()

        if change_status:
            self._status = ConnectionStatus.CLOSED
            self.logger.debug(f"Connection {self.endpoint} is closed")

    async def execute(
        self,
        command: Union[str, bytes],
        *args: Any,
        data: Optional[Any] = None,
        callback: Optional[Callable[[TCPResponse], Any]] = None,
    ) -> TCPResponse:
        """Execute command

        Be careful: commands ``NOP``, ``FIN``, ``RDY``, ``REQ``, ``TOUCH``
            by NSQ spec returns ``None`` as  The class:`asyncio.Future` result.

        :returns: The response from NSQ.
        """
        if command is None:
            raise ValueError("Command must not be None")
        if None in set(args):
            raise ValueError("Args must not contain None")

        if (
            self.is_auth_required
            and not self.is_authorized
            and command != NSQCommands.AUTH
        ):
            raise NSQUnauthorized("NSQ server requires client authorization")

        if (
            self.status.is_reconnecting
            and self._reconnect_task
            and not self._reconnect_task.done()
        ):
            await self._reconnect_task

        assert self._reader, "You should call `connect` method first"
        if not self._status and not (command == NSQCommands.CLS):
            raise ConnectionClosedError("Connection is closed")

        future = self._loop.create_future()
        if command in (
            NSQCommands.NOP,
            NSQCommands.FIN,
            NSQCommands.RDY,
            NSQCommands.REQ,
            NSQCommands.TOUCH,
        ):
            future.set_result(None)
            callback and callback(None)
        else:
            self._cmd_waiters.append((future, callback))

        command_raw = self._parser.encode_command(command, *args, data=data)
        if command != NSQCommands.NOP:
            self.logger.debug("NSQ: Executing command %s", command_raw)
        assert self._writer is not None
        self._writer.write(command_raw)

        # track all processed and requeued messages
        if command in (
            NSQCommands.FIN,
            NSQCommands.REQ,
            NSQCommands.FIN.decode(),
            NSQCommands.REQ.decode(),
        ):
            self._in_flight = max(0, self._in_flight - 1)

        return await future

    async def identify(
        self, config: Optional[Union[dict, str]] = None, **kwargs: Any
    ) -> TCPResponse:
        if config and isinstance(config, (dict, str)):
            raise TypeError("Config should be dict type or str")

        if config or kwargs:
            self._config = config or kwargs
        config = json.dumps(self._config)

        response = await self.execute(
            NSQCommands.IDENTIFY, data=config, callback=self._start_upgrading,
        )

        if response in (NSQCommands.OK, NSQCommands.OK.decode()):
            await self._finish_upgrading()
            return response

        assert isinstance(response, NSQResponseSchema)
        response_config = json.loads(response.body)
        fut = None

        if response_config.get("auth_required"):
            self._is_auth_required = True
        if response_config.get("tls_v1"):
            await self._upgrade_to_tls()
        if response_config.get("snappy"):
            fut = self._upgrade_to_snappy()
        elif response_config.get("deflate"):
            fut = self._upgrade_to_deflate()
        await self._finish_upgrading()

        if fut:
            upgrade_response = await fut
            assert upgrade_response.is_ok

        return response

    async def _upgrade_to_tls(self) -> None:
        raise NotImplementedError("Upgrade to TLSv1 not implemented yet")

    def _upgrade_to_snappy(self) -> asyncio.Future:
        raise NotImplementedError("Upgrade to snappy not implemented yet")

    def _upgrade_to_deflate(self) -> asyncio.Future:
        raise NotImplementedError("Upgrade to deflate not implemented yet")

    async def _read_data_task(self) -> None:
        """Response reader task."""
        assert self._reader is not None

        while not self._reader.at_eof():
            try:
                data = await self._reader.read(consts.MAX_CHUNK_SIZE)
            except asyncio.CancelledError:
                # useful during update to TLS, task canceled but connection
                # should not be closed
                return
            except Exception as exc:
                await self._do_close(exc)
                return

            self._parser.feed(data)
            not self._is_upgrading and await self._read_buffer()

        self.logger.info("Lost connection to NSQ")
        if self._auto_reconnect:
            await asyncio.sleep(1)
            self._reconnect_task = self._loop.create_task(
                self.reconnect(raise_error=False),
            )
        else:
            await self._do_close(OSError("Lost connection to NSQ"))

    async def _parse_data(self) -> bool:
        try:
            response = self._parser.get()
        except ProtocolError as exc:
            # ProtocolError is fatal
            await self._do_close(exc)
            return False

        if response is None:
            return False

        if response.is_heartbeat:
            await self._pulse()
            return True

        self.logger.debug("NSQ: Got data: %s", truncate_text(str(response)))

        if response.is_message:
            assert isinstance(response, NSQMessageSchema)
            # track number in flight messages
            self._in_flight += 1
            self._on_message_hook(response)
            return True

        future: asyncio.Future
        callback: Optional[Callable[[TCPResponse], Any]]
        future, callback = self._cmd_waiters.popleft()

        if response.is_response:
            if not future.cancelled():
                future.set_result(response)
                callback is not None and callback(response)

        if response.is_error:
            assert isinstance(response, NSQErrorSchema)
            exception = get_exception(response.code, response.body)

            if not future.cancelled():
                future.set_result(response)
            callback and callback(response)
            self._on_exception and self._on_exception(exception)

        return True

    def _on_message_hook(self, message_schema: NSQMessageSchema) -> None:
        self._last_message_time = datetime.now(tz=timezone.utc)
        message = NSQMessage(message_schema, self)

        if self._on_message:
            try:
                message = self._on_message(message)
            except Exception as e:
                self._do_close(e)
        self._message_queue.put_nowait(message)

    async def _read_buffer(self) -> None:
        is_continue = True
        while is_continue:
            is_continue = await self._parse_data()

    def _start_upgrading(self, resp: Optional[TCPResponse] = None) -> None:
        self._is_upgrading = True

    async def _finish_upgrading(self, resp: Optional[TCPResponse] = None) -> None:
        await self._read_buffer()
        self._is_upgrading = False

    async def auth(self, secret: str) -> TCPResponse:
        """If the ``IDENTIFY`` response indicates ``auth_required=true``
        the client must send ``AUTH`` before any ``SUB``, ``PUB`` or ``MPUB``
        commands. If auth_required is not present (or ``false``),
        a client must not authorize.

        :param secret:
        :return:
        """
        response = await self.execute(NSQCommands.AUTH, data=secret)
        if not isinstance(response, NSQErrorSchema):
            self._secret = secret
        return response

    async def sub(self, topic: str, channel: str) -> TCPResponse:
        """Subscribe to the topic and channel"""
        validate_topic_channel_name(topic)
        validate_topic_channel_name(channel)
        response = await self.execute(NSQCommands.SUB, topic, channel)
        if isinstance(response, NSQResponseSchema):
            self._topic = topic
            self._channel = channel
            self._is_subscribed = True
        return response

    async def pub(self, topic: str, message: Any) -> TCPResponse:
        """Publish a message to a topic"""
        validate_topic_channel_name(topic)
        return await self.execute(NSQCommands.PUB, topic, data=message)

    async def dpub(self, topic: str, message: Any, delay_time: int) -> TCPResponse:
        """Publish a deferred message to a topic"""
        validate_topic_channel_name(topic)
        return await self.execute(NSQCommands.DPUB, topic, delay_time, data=message)

    async def mpub(self, topic: str, *messages: Any) -> TCPResponse:
        """Publish multiple messages to a topic"""
        validate_topic_channel_name(topic)
        return await self.execute(
            NSQCommands.MPUB,
            topic,
            data=messages if len(messages) > 1 else messages[0],
        )

    async def rdy(self, messages_count: int = 1) -> None:
        """Update RDY state (indicate you are ready to receive N messages)"""
        assert isinstance(
            messages_count, int,
        ), "Argument messages_count should be positive integer"
        assert messages_count >= 0, "Argument messages_count should be positive integer"

        self.rdy_messages_count = messages_count
        await self.execute(NSQCommands.RDY, messages_count)

    async def fin(self, message_id: Union[str, NSQMessage]) -> None:
        """Finish a message (indicate successful processing)"""
        if isinstance(message_id, NSQMessage):
            await message_id.fin()
        await self.execute(NSQCommands.FIN, message_id)

    async def req(self, message_id: Union[str, NSQMessage], timeout: int = 0) -> None:
        """Re-queue a message (indicate failure to process)

        The re-queued message is placed at the tail of the queue,
        equivalent to having just published it.
        """
        if isinstance(message_id, NSQMessage):
            await message_id.req(timeout)
        await self.execute(NSQCommands.REQ, message_id, timeout)

    async def touch(self, message_id: Union[str, NSQMessage]) -> None:
        """Reset the timeout for an in-flight message"""
        if isinstance(message_id, NSQMessage):
            await message_id.touch()
        await self.execute(NSQCommands.TOUCH, message_id)

    async def _cls(self) -> TCPResponse:
        return await self.execute(NSQCommands.CLS)

    async def subscribe(
        self, topic: str, channel: str, messages_count: int = 1
    ) -> None:
        """Shortcut for ``sub()`` and ``rdy()`` methods"""
        sub_response = await self.sub(topic, channel)
        if not sub_response:
            return

        await self.rdy(messages_count)

    async def messages(self) -> AsyncGenerator[NSQMessage, None]:
        """Generator, yields messages"""
        assert self.is_subscribed, "You should subscribe to the topic first"

        while self.is_subscribed:
            message = await self._message_queue.get()
            if message is None:
                return
            yield message

    def get_message(self) -> Optional[NSQMessage]:
        """Shortcut for ``asyncio.Queue.get_nowait()``
        without raising exceptions
        """
        try:
            return self._message_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def wait_for_message(self) -> Optional[NSQMessage]:
        """Shortcut for `asyncio.Queue.get()``.

        :rtype: :class:`NSQMessage`
        :returns: :class:`NSQMessage`.
            Be aware on closing action may returns ``None``.
            This is need to exit from ``NSQConnection.messages`` generator
            when connection closed with exception.
        """
        return await self.message_queue.get()


async def open_connection(
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
) -> NSQConnection:
    nsq = NSQConnection(
        host,
        port,
        message_queue=message_queue,
        on_message=on_message,
        on_exception=on_exception,
        loop=loop,
        auto_reconnect=auto_reconnect,
        heartbeat_interval=heartbeat_interval,
        feature_negotiation=feature_negotiation,
        tls_v1=tls_v1,
        snappy=snappy,
        deflate=deflate,
        deflate_level=deflate_level,
        sample_rate=sample_rate,
        debug=debug,
        logger=logger,
    )
    await nsq.connect()
    await nsq.identify()
    return nsq
