from functools import wraps
from time import time
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from ansq.tcp.connection import NSQConnection
    from . import NSQMessageSchema

__all__ = 'NSQMessage'


def not_processed(func):
    """Decorator to verify that the message has not yet been processed.

    :raises RuntimeWarning: in case message was processed earlier.
    """
    @wraps(func)
    async def decorator(cls: 'NSQMessage', *args, **kwargs):
        if cls.is_processed:
            raise RuntimeWarning('Message has already been processed')
        response = await func(cls, *args, **kwargs)
        return response
    return decorator


class NSQMessage:
    def __init__(
            self, message_schema: 'NSQMessageSchema',
            connection: 'NSQConnection',
            timeout: int = 60000, is_processed: bool = False):
        self.timestamp = message_schema.timestamp
        self.attempts = message_schema.attempts
        self.body = message_schema.body
        self.id = message_schema.id
        self._connection = connection
        self._is_processed = is_processed

        self._timeout_in = timeout
        self._initialized_at = time()

    def __repr__(self):
        return (
            '<NSQMessage id="{id}", body={body}, attempts={attempts}, '
            'timestamp={timestamp}, timeout={timeout}, '
            'initialized_at={initialized_at}, is_timed_out={is_timed_out}, '
            'is_processed={is_processed}>'.format(
                id=self.id, body=self.body, attempts=self.attempts,
                timestamp=self.timestamp, timeout=self.timeout,
                initialized_at=self._initialized_at,
                is_timed_out=self.is_timed_out, is_processed=self.is_processed
            )
        )

    def __str__(self):
        """Returns decoded message's body.

        :raises UnicodeDecodeError: Trying to decode bytes like ``b'\xa1'``.
            Be careful. Call this method only if you sure that the body is str.
        """
        return self.body.decode('utf-8')

    @property
    def is_processed(self) -> bool:
        """True if message has been processed:
            * finished
            * re-queued
            * timed out
        """
        return self.is_timed_out or self._is_processed

    @property
    def timeout(self) -> float:
        return self._timeout_in

    @property
    def is_timed_out(self) -> bool:
        return self._initialized_at + self._timeout_in * 0.001 < time()

    @not_processed
    async def fin(self):
        """Finish a message (indicate successful processing)

        :raises RuntimeWarning: in case message was processed earlier.
        """
        await self._connection.fin(self.id)
        self._is_processed = True

    @not_processed
    async def req(self, timeout=10):
        """Re-queue a message (indicate failure to process)

        :param timeout: ``int`` configured max timeout  0 is a special case
            that will not defer re-queueing.
        :raises RuntimeWarning: in case message was processed earlier.
        """
        await self._connection.req(self.id, timeout)
        self._is_processed = True

    @not_processed
    async def touch(self):
        """Reset the timeout for an in-flight message.

        :raises RuntimeWarning: in case message was processed earlier.
        """
        await self._connection.touch(self.id)
