from __future__ import annotations

import asyncio
import json
from time import time
from typing import Optional, Callable, Any, Union, Generator

from ansq.tcp.exceptions import ProtocolError, get_exception
from ansq.tcp.types import (
    TCPConnection as NSQConnectionBase, ConnectionStatus, NSQMessage,
    NSQResponseSchema, NSQMessageSchema, NSQErrorSchema, NSQCommands
)


class NSQConnection(NSQConnectionBase):
    async def connect(self) -> bool:
        """Open connection"""
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port)

        self._writer.write(NSQCommands.MAGIC_V2)
        self._status = ConnectionStatus.CONNECTED
        self.logger.info('Connect to {} established'.format(self.endpoint))

        self._reader_task = self._loop.create_task(self._read_data())

        return True

    async def reconnect(self) -> bool:
        """Reopen connection"""
        self.logger.debug('Reconnecting to {}...'.format(self.endpoint))
        self._status = ConnectionStatus.RECONNECTING

        await self._do_close(change_status=False)
        await asyncio.sleep(1)

        retry_count = 1
        last_exception: Optional[Exception] = None
        while retry_count < 10:
            try:
                if await self.connect() and await self.identify():
                    self.logger.info('Reconnected to {} in {} attempts'.format(
                        self.endpoint, retry_count))
                    return True
            except Exception as e:
                last_exception = e
                self.logger.exception(e)

            retry_count += 1
            await asyncio.sleep(3)

        raise last_exception

    async def _do_close(
            self, exception: Exception = None, change_status: bool = True):
        if self.is_closed or self.status.is_init or self.status.is_closing:
            return

        if change_status:
            self._status = ConnectionStatus.CLOSING

        if exception:
            self.logger.error(
                'Connection {} is closing due an error: {}'.format(
                    self.endpoint, exception))
        else:
            self.logger.debug('Connection {} is closing...'.format(
                self.endpoint))

        if self.is_subscribed:
            self._is_subscribed = False
            try:
                await self._cls()
            finally:
                pass

        for _ in range(self._message_queue.qsize()):
            self._message_queue.get_nowait()

        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except Exception as e:
                self.logger.exception(e)

        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except Exception as e:
                self.logger.exception(e)

        try:
            self._writer.close()
            await self._writer.wait_closed()
        finally:
            pass

        if change_status:
            self._status = ConnectionStatus.CLOSED

        self.logger.info('Connection {} is closed'.format(self.endpoint))

    async def execute(
            self, command: Union[str, bytes], *args, data: Any = None,
            callback: Callable = None
    ) -> Optional[Union[NSQResponseSchema, NSQErrorSchema, NSQMessageSchema]]:
        """Execute command

        Be careful: commands ``NOP``, ``FIN``, ``RDY``, ``REQ``, ``TOUCH``
            by NSQ spec returns ``None`` as  The class:`asyncio.Future` result.

        :returns: The response from NSQ.
        """
        assert self._reader, 'You should call `connect` method first'
        assert self._status or command == NSQCommands.CLS, (
            'Connection closed')

        if command is None:
            raise TypeError('Command must not be None')
        if None in set(args):
            raise TypeError('Args must not contain None')

        if self.status.is_reconnecting and not self._reconnect_task.done():
            await self._reconnect_task

        future = self._loop.create_future()
        if command in (
                NSQCommands.NOP, NSQCommands.FIN, NSQCommands.RDY,
                NSQCommands.REQ, NSQCommands.TOUCH
        ):
            future.set_result(None)
            callback and callback(None)
        else:
            self._cmd_waiters.append((future, callback))

        command_raw = self._parser.encode_command(command, *args, data=data)
        self.logger.debug('NSQ: Executing command %s' % command_raw)
        self._writer.write(command_raw)

        # track all processed and requeued messages
        if command in (
                NSQCommands.FIN, NSQCommands.REQ,
                NSQCommands.FIN.decode(), NSQCommands.REQ.decode()
        ):
            self._in_flight = max(0, self._in_flight - 1)

        return await future

    async def identify(self, config: Union[dict, str] = None, **kwargs):
        if config and isinstance(config, (dict, str)):
            raise TypeError('Config should be dict type or str')

        if config or kwargs:
            self._config = config or kwargs
        config = json.dumps(self._config)

        response = await self.execute(
            NSQCommands.IDENTIFY, data=config, callback=self._start_upgrading)

        if response in (NSQCommands.OK, NSQCommands.OK.decode()):
            await self._finish_upgrading()
            return response

        response_config = json.loads(response.body)
        fut = None

        if response_config.get('tls_v1'):
            await self._upgrade_to_tls()
        if response_config.get('snappy'):
            fut = self._upgrade_to_snappy()
        elif response_config.get('deflate'):
            fut = self._upgrade_to_deflate()
        await self._finish_upgrading()

        if fut:
            upgrade_response = await fut
            assert upgrade_response.is_ok

        return response

    async def _upgrade_to_tls(self):
        raise NotImplementedError('Upgrade to TLSv1 not implemented yet')

    def _upgrade_to_snappy(self):
        raise NotImplementedError('Upgrade to snappy not implemented yet')

    def _upgrade_to_deflate(self):
        raise NotImplementedError('Upgrade to deflate not implemented yet')

    async def _read_data(self):
        """Response reader task."""
        while not self._reader.at_eof():
            try:
                data = await self._reader.read(52)

                self._parser.feed(data)
                not self._is_upgrading and await self._read_buffer()
            except asyncio.CancelledError:
                # useful during update to TLS, task canceled but connection
                # should not be closed
                return
            except Exception as exc:
                self.logger.exception(exc)
                break

        if self.is_closed:
            return

        if self._auto_reconnect:
            self._reconnect_task = self._loop.create_task(self.reconnect())
        else:
            await self._do_close(RuntimeError('Lost connection to NSQ'))

    async def _parse_data(self) -> bool:
        try:
            response = self._parser.get()
        except ProtocolError as exc:
            # ProtocolError is fatal
            self.logger.exception(exc)
            await self.reconnect()
            return False

        if response is None:
            return False

        self.logger.debug('NSQ: Got data: %s', response)

        if response.is_heartbeat:
            await self._pulse()
            return True

        if response.is_message:
            # track number in flight messages
            self._in_flight += 1
            self._on_message_hook(response)
            return True

        future, callback = self._cmd_waiters.popleft()
        future: asyncio.Future
        callback: Callable

        if response.is_response:
            if not future.cancelled():
                future.set_result(response)
                callback is not None and callback(response)

        if response.is_error:
            exception = get_exception(response.code, response.body)
            self.logger.error(exception)

            if not future.cancelled():
                future.set_result(exception)
            callback and callback(exception)

            do_reconnect = self._auto_reconnect
            if self._on_exception_handler:
                do_reconnect = self._on_exception_handler(exception)

            if exception.fatal and do_reconnect:
                self._reconnect_task = self._loop.create_task(self.reconnect())

        return True

    def _on_message_hook(self, message_schema: NSQMessageSchema):
        self._last_message_timestamp = time()
        message = NSQMessage(message_schema, self)

        if self._on_message:
            try:
                message = self._on_message(message)
            except Exception as e:
                self._do_close(e)
        self._message_queue.put_nowait(message)

    async def _read_buffer(self):
        is_continue = True
        while is_continue:
            is_continue = await self._parse_data()

    def _start_upgrading(self, resp=None):
        self._is_upgrading = True

    async def _finish_upgrading(self, resp=None):
        await self._read_buffer()
        self._is_upgrading = False

    async def auth(self, secret) -> Union[NSQResponseSchema, NSQErrorSchema]:
        """If the ``IDENTIFY`` response indicates ``auth_required=true``
        the client must send ``AUTH`` before any ``SUB``, ``PUB`` or ``MPUB``
        commands. If auth_required is not present (or ``false``),
        a client must not authorize.

        :param secret:
        :return:
        """
        return await self.execute(NSQCommands.AUTH, data=secret)

    async def sub(self, topic, channel) -> Union[
        NSQResponseSchema, NSQErrorSchema
    ]:
        """Subscribe to a topic/channel"""
        response = await self.execute(NSQCommands.SUB, topic, channel)
        if isinstance(response, NSQResponseSchema):
            self._topic = topic
            self._channel = channel
            self._is_subscribed = True
        return response

    async def pub(self, topic, message) -> Union[
        NSQResponseSchema, NSQErrorSchema
    ]:
        """Publish a message to a topic"""
        return await self.execute(
            NSQCommands.PUB, topic, data=message)

    async def dpub(self, topic, message, delay_time) -> Union[
        NSQResponseSchema, NSQErrorSchema
    ]:
        """Publish a deferred message to a topic"""
        return await self.execute(
            NSQCommands.DPUB, topic, delay_time, data=message)

    async def mpub(self, topic, *messages) -> Union[
        NSQResponseSchema, NSQErrorSchema
    ]:
        """Publish multiple messages to a topic"""
        return await self.execute(
            NSQCommands.MPUB, topic,
            data=messages if len(messages) > 1 else messages[0]
        )

    async def rdy(self, messages_count: int = 1):
        """Update RDY state (indicate you are ready to receive N messages)"""
        assert isinstance(messages_count, int), (
            'Argument messages_count should be positive integer')
        assert messages_count >= 0, (
            'Argument messages_count should be positive integer')

        self.rdy_messages_count = messages_count
        await self.execute(NSQCommands.RDY, messages_count)

    async def fin(self, message_id: Union[str, NSQMessage]):
        """Finish a message (indicate successful processing)"""
        if isinstance(message_id, NSQMessage):
            await message_id.fin()
        await self.execute(NSQCommands.FIN, message_id)

    async def req(self, message_id: Union[str, NSQMessage], timeout: int = 0):
        """Re-queue a message (indicate failure to process)

        The re-queued message is placed at the tail of the queue,
        equivalent to having just published it.
        """
        if isinstance(message_id, NSQMessage):
            await message_id.req(timeout)
        await self.execute(NSQCommands.REQ, message_id, timeout)

    async def touch(self, message_id: Union[str, NSQMessage]):
        """Reset the timeout for an in-flight message"""
        if isinstance(message_id, NSQMessage):
            await message_id.touch()
        await self.execute(NSQCommands.TOUCH, message_id)

    async def _cls(self) -> Union[NSQResponseSchema, NSQErrorSchema]:
        return await self.execute(NSQCommands.CLS)

    async def subscribe(
            self, topic: str, channel: str, messages_count: int = 1):
        """Shortcut for ``sub()`` and ``rdy()`` methods"""
        sub_response = await self.sub(topic, channel)
        if not sub_response:
            return sub_response

        await self.rdy(messages_count)

    async def messages(self) -> Generator[NSQMessage, None]:
        """Generator, yields messages"""
        assert self.is_subscribed, (
            'You should subscribe to the topic first')

        while self.is_subscribed:
            message = await self._message_queue.get()
            yield message

    def get_message(self) -> Optional[NSQMessage]:
        """Shortcut for ``asyncio.Queue.get_nowait()``
        without raising exceptions
        """
        try:
            return self._message_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def wait_for_message(self) -> NSQMessage:
        """Shortcut for `asyncio.Queue.get()``."""
        return await self.message_queue.get()


async def open_connection(*args, **kwargs) -> NSQConnection:
    nsq = NSQConnection(*args, **kwargs)
    await nsq.connect()
    await nsq.identify()
    return nsq
