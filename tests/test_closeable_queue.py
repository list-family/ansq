import asyncio

import pytest

from ansq.closeable_queue import CloseableQueue, QueueWasClosedError


async def test_closeable_queue():
    queue = CloseableQueue()

    # test that we raise QueueEmpty if queue not closed
    with pytest.raises(asyncio.QueueEmpty):
        queue.get_nowait()

    # test successful put
    queue.put_nowait("test 1")
    await queue.put("test 2")

    # close queue
    queue.close()

    # test that we can't put after close
    with pytest.raises(QueueWasClosedError):
        queue.put_nowait("test 3")

    with pytest.raises(QueueWasClosedError):
        await queue.put("test 3")

    # test that we can read putted messages even queue was closed
    assert queue.get_nowait() == "test 1"
    assert await queue.get() == "test 2"

    # test exception raised when we read new messages from empty queue
    with pytest.raises(QueueWasClosedError):
        queue.get_nowait()

    with pytest.raises(QueueWasClosedError):
        await queue.get()
