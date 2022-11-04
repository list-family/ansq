import asyncio
import json
import warnings
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Callable, Mapping, Optional, Union

import attr

from ansq.tcp import consts
from ansq.tcp.exceptions import (
    ConnectionClosedError,
    NSQUnauthorized,
    ProtocolError,
    get_exception,
)
from ansq.tcp.types import (
    ConnectionFeatures,
    ConnectionOptions,
    ConnectionStatus,
    NSQCommands,
    NSQErrorSchema,
    NSQMessage,
    NSQMessageSchema,
    NSQResponseSchema,
)
from ansq.tcp.types import TCPConnection as NSQConnectionBase
from ansq.typedefs import TCPResponse
from ansq.utils import validate_topic_channel_name

# Auto reconnect settings
AUTO_RECONNECT_INITIAL_INTERVAL = 2
AUTO_RECONNECT_MAX_INTERVAL = 2048
AUTO_RECONNECT_PROGRESSION_RATIO = 2


class NSQConnection(NSQConnectionBase):
    async def connect(self) -> bool:
        """Open connection"""
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port
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

        await self._do_close(change_status=False, silent=True)
        try:
            await self.connect()
            await self.identify()
            self._secret and await self.auth(self._secret)
            if self._is_subscribed:
                assert self._topic is not None
                assert self._channel is not None
                await self.subscribe(
                    self._topic, self._channel, self.rdy_messages_count
                )
        except Exception as e:
            if raise_error:
                raise e

            await self._do_close(e)
            return False

        self.logger.debug(f"Reconnected to {self.endpoint}")
        self._status = ConnectionStatus.CONNECTED
        return True

    async def _do_auto_reconnect(
        self, interval: int = AUTO_RECONNECT_INITIAL_INTERVAL
    ) -> None:
        """Call ``reconnect()`` method. If failed, sleep and try again."""
        if not self._auto_reconnect:
            return

        # Reconnect silently
        try:
            reconnected = await self.reconnect()
        except Exception as exc:
            await self._do_close(exc, change_status=False, silent=True)
            reconnected = False

        # Return early if succeeded
        if reconnected:
            return

        # Don't sleep more than max interval
        if interval > AUTO_RECONNECT_MAX_INTERVAL:
            interval = AUTO_RECONNECT_MAX_INTERVAL

        self.logger.debug(
            "Failed to reconnect to %s. Wait for %s seconds ...",
            self.endpoint,
            interval,
        )

        # Reconnection is failed - sleep and schedule new reconnect
        await asyncio.sleep(interval)
        self._reconnect_task = self._loop.create_task(
            self._do_auto_reconnect(interval * AUTO_RECONNECT_PROGRESSION_RATIO),
        )

    async def _do_close(
        self,
        error: Optional[Union[Exception, str]] = None,
        change_status: bool = True,
        silent: bool = False,
    ) -> None:
        if self.is_closed or self.status.is_init or self.status.is_closing:
            return

        if change_status:
            self._status = ConnectionStatus.CLOSING

        if not silent:
            if error is not None:
                self.logger.error(
                    "Connection {} is closing due an error: {}".format(
                        self.endpoint, error
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
            except asyncio.CancelledError:
                # The task is cancelled - don't log useless exception trace
                # TODO: In the future look further for reasons a task could
                #       appear cancelled here
                pass
            except Exception as e:
                self.logger.exception(e)

        if change_status and self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                # The task is cancelled - don't log useless exception trace
                pass
            except Exception as e:
                self.logger.exception(e)

        assert self._writer is not None
        try:
            self._writer.close()
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
            if self._on_close is not None:
                self._on_close(self)

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
        self,
        config: Optional[Union[dict, str]] = None,
        *,
        features: Optional[ConnectionFeatures] = None,
        **kwargs: Any,
    ) -> TCPResponse:
        """Executes `IDENTIFY` command.

        Connection features are being determined in the following order: `features`,
        `config` (deprecated), `kwargs` (deprecated).

        If any of `features`, `config`, `kwargs` exist features defined in `__init__`
        method will be ignored.
        """
        # handle deprecated args
        if config is not None:
            if not isinstance(config, (dict, str)):
                raise TypeError("Config should be dict type or str")
            warnings.warn(
                message=(
                    "`config` argument for `NSQConnection.identify` is deprecated: "
                    "use `features` argument instead"
                ),
                category=DeprecationWarning,
            )
            if isinstance(config, dict):
                features = ConnectionFeatures(**config)
        elif kwargs:
            warnings.warn(
                message=(
                    "Passing keyword arguments to `NSQConnection.identify` is "
                    "deprecated: use `features` argument instead"
                ),
                category=DeprecationWarning,
            )
            features = ConnectionFeatures(**kwargs)

        # update options with features passed as arguments to this method
        if features is not None:
            self._options = attr.evolve(self._options, features=features)

        # maybe handle `config` argument passed as a string
        if features is None and isinstance(config, str):
            features_data = config
        else:
            features_data = json.dumps(attr.asdict(self._options.features))

        response = await self.execute(
            NSQCommands.IDENTIFY, data=features_data, callback=self._start_upgrading
        )

        if response in (NSQCommands.OK, NSQCommands.OK.decode()):
            await self._finish_upgrading()
            return response

        assert isinstance(response, NSQResponseSchema)

        # failed to update client metadata on the server and negotiate features
        if response.is_error:
            await self._do_close(error=response.text)
            return response

        try:
            response_config = json.loads(response.body)
        except ValueError as exc:
            self.logger.error("failed to parse IDENTIFY response - %r", response.body)
            await self._do_close(error=exc)
            return response

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

            if not self._is_upgrading:
                await self._read_buffer()

        self.logger.info("Lost connection to NSQ %s", self.endpoint)
        if self._auto_reconnect:
            await asyncio.sleep(1)
            self._reconnect_task = self._loop.create_task(self._do_auto_reconnect())
        else:
            await self._do_close()

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

        if self._debug:
            self.logger.debug("NSQ: Got data: %s", response)

        if response.is_message:
            assert isinstance(response, NSQMessageSchema)
            # track number in flight messages
            self._in_flight += 1
            await self._on_message_hook(response)
            return True

        # commands like RDY/FIN/REQ/TOUCH do not return a success response, however,
        # they might return an error
        if response.is_error and not self._cmd_waiters:
            self.logger.error(response.text)
            return False

        # non-error responses must have a command waiter, otherwise,
        # it's more likely a bug
        if not self._cmd_waiters:  # pragma: no cover
            self.logger.error("Unexpected response: %s", response)
            return False

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

    async def _on_message_hook(self, message_schema: NSQMessageSchema) -> None:
        self._last_message_time = datetime.now(tz=timezone.utc)
        message = NSQMessage(message_schema, self)

        if self._on_message:
            try:
                message = self._on_message(message)
            except Exception as e:
                await self._do_close(e)
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
            messages_count, int
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
            if message.is_timed_out:
                self.logger.error(f"Message id={message.id} is timed out")
                continue
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
    connection_options: ConnectionOptions = ConnectionOptions(),
    **kwargs: Mapping[str, Any],
) -> NSQConnection:
    """A helper to create and open an `NSQConnection`.

    If `connection_options` is defined other keyword args are being ignored.
    """
    nsq = NSQConnection(
        host,
        port,
        connection_options=connection_options,
        **kwargs,
    )
    await nsq.connect()
    await nsq.identify()
    return nsq
