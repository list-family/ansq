from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from ansq.tcp.consts import DEFAULT_REQ_TIMEOUT

if TYPE_CHECKING:
    from ansq.tcp.connection import NSQConnection

    from . import NSQMessageSchema

__all__ = ["NSQMessage"]


def ensure_can_be_processed(func: Callable) -> Callable:
    """Decorator to verify that the message can be processed.

    :raises RuntimeWarning: in case message was processed earlier.
    """

    @wraps(func)
    async def wrapper(message: "NSQMessage", *args: Any, **kwargs: Any) -> Any:
        if message.is_processed:
            raise RuntimeWarning(f"Message id={message.id} has already been processed")
        if message.is_timed_out:
            raise RuntimeWarning(f"Message id={message.id} is timed out")
        return await func(message, *args, **kwargs)

    return wrapper


class NSQMessage:
    def __init__(
        self,
        message_schema: "NSQMessageSchema",
        connection: "NSQConnection",
    ) -> None:
        self.timestamp = message_schema.timestamp
        self.attempts = message_schema.attempts
        self.body = message_schema.body
        self.id = message_schema.id

        self._connection = connection
        self._timeout_in = timedelta(
            milliseconds=connection.options.features.msg_timeout
        )
        self._is_processed = False
        self._initialized_at = datetime.now(tz=timezone.utc)

    def __repr__(self) -> str:
        return (
            '<NSQMessage id="{id}", body={body!r}, attempts={attempts}, '
            "timestamp={timestamp}, timeout={timeout}, "
            "initialized_at={initialized_at}, is_timed_out={is_timed_out}, "
            "is_processed={is_processed}, can_be_processed={can_be_processed}>".format(
                id=self.id,
                body=self.body,
                attempts=self.attempts,
                timestamp=self.timestamp,
                timeout=self.timeout,
                initialized_at=self._initialized_at,
                is_timed_out=self.is_timed_out,
                is_processed=self.is_processed,
                can_be_processed=self.can_be_processed,
            )
        )

    def __str__(self) -> str:
        """Returns decoded message's body.

        :raises UnicodeDecodeError: Trying to decode bytes like ``b'\xa1'``.
            Be careful. Call this method only if you sure that the body is str.
        """
        return self.body.decode("utf-8")

    @property
    def is_processed(self) -> bool:
        """True if message has been processed:
        * finished
        * re-queued
        """
        return self._is_processed

    @property
    def timeout(self) -> timedelta:
        return self._timeout_in

    @property
    def is_timed_out(self) -> bool:
        return self._initialized_at + self.timeout < datetime.now(tz=timezone.utc)

    @property
    def can_be_processed(self) -> bool:
        """True if the message has not been processed and has not timed out yet"""
        return not self.is_timed_out and not self.is_processed

    @ensure_can_be_processed
    async def fin(self) -> None:
        """Finish a message (indicate successful processing)

        :raises RuntimeWarning: in case message was processed earlier or timed out.
        """
        await self._connection.fin(self.id)
        self._is_processed = True

    @ensure_can_be_processed
    async def req(self, timeout: int = DEFAULT_REQ_TIMEOUT) -> None:
        """Re-queue a message (indicate failure to process)

        :param timeout: An ``int`` in milliseconds where
            N <= configured max timeout; 0 is a special case
            that will not defer re-queueing.
        :raises RuntimeWarning: in case message was processed earlier or timed out.
        """
        await self._connection.req(self.id, timeout)
        self._is_processed = True

    @ensure_can_be_processed
    async def touch(self) -> None:
        """Reset the timeout for an in-flight message.

        :raises RuntimeWarning: in case message was processed earlier or timed out.
        """
        await self._connection.touch(self.id)
        self._initialized_at = datetime.now(tz=timezone.utc)
