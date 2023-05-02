import asyncio
from asyncio import Event, Queue
from typing import Generic, Optional, TypeVar

T = TypeVar("T")


class QueueWasClosedError(Exception):
    def __init__(self, exc: Optional[BaseException]) -> None:
        self.exc = exc

    def __str__(self) -> str:
        return f"Queue was closed by {self.exc!r}"


class CloseableQueue(Generic[T], Queue):
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)

        self._close_event = Event()
        self._close_exception: Optional[BaseException] = None

    def close(self, *, exc: Optional[BaseException] = None) -> None:
        self._close_event.set()
        self._close_exception = exc

    def is_closed(self) -> bool:
        return self._close_event.is_set()

    async def wait_close(self) -> bool:
        return await self._close_event.wait()

    async def get(self) -> T:
        if not self.empty():
            return super().get_nowait()

        self._check_closed()

        wait_close = asyncio.create_task(self.wait_close())
        wait_message = asyncio.create_task(super().get())
        done, _ = await asyncio.wait(
            fs=(wait_close, wait_message),
            return_when=asyncio.FIRST_COMPLETED,
        )

        if wait_message.done():
            return wait_message.result()

        if wait_close.done():
            self._check_closed()

        raise RuntimeError(
            "Unreachable. Please report a bug "
            "to https://github.com/list-family/ansq if you got this error"
        )

    def get_nowait(self) -> T:
        try:
            return super().get_nowait()
        except asyncio.QueueEmpty:
            self._check_closed()
            raise

    def put_nowait(self, item: T) -> None:
        self._check_closed()
        return super().put_nowait(item)

    def _check_closed(self) -> None:
        if self.is_closed():
            raise QueueWasClosedError(self._close_exception)
