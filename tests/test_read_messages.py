import asyncio
from time import time

import pytest

from ansq import open_connection
from ansq.tcp.types import NSQMessage


@pytest.mark.asyncio
async def test_read_message():
    nsq = await open_connection()
    assert nsq.status.is_connected

    timestamp = time()

    response = await nsq.pub("test_read_message", f"hello sent at {timestamp}")
    assert response.is_ok

    response = await nsq.sub("test_read_message", "channel1")
    assert response.is_ok

    await nsq.rdy(1)
    message = await nsq.message_queue.get()
    assert not message.is_processed

    await message.fin()
    assert message.is_processed

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_read_message_and_req():
    nsq = await open_connection()
    assert nsq.status.is_connected

    timestamp = time()

    response = await nsq.pub("test_read_message_and_req", f"hello sent at {timestamp}")
    assert response.is_ok

    response = await nsq.sub("test_read_message_and_req", "channel1")
    assert response.is_ok

    await nsq.rdy(1)
    message = await nsq.message_queue.get()
    assert not message.is_processed

    await message.req()
    assert message.is_processed

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_read_message_and_touch():
    nsq = await open_connection()
    assert nsq.status.is_connected

    timestamp = time()

    response = await nsq.pub(
        "test_read_message_and_touch", f"hello sent at {timestamp}",
    )
    assert response.is_ok

    response = await nsq.sub("test_read_message_and_touch", "channel1")
    assert response.is_ok

    await nsq.rdy(1)
    message = await nsq.message_queue.get()
    assert not message.is_processed

    await message.touch()
    assert not message.is_processed

    await message.fin()
    assert message.is_processed

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_read_message_and_fin_twice():
    nsq = await open_connection()
    assert nsq.status.is_connected

    timestamp = time()

    response = await nsq.pub(
        "test_read_message_and_fin_twice", f"hello sent at {timestamp}",
    )
    assert response.is_ok

    response = await nsq.sub("test_read_message_and_fin_twice", "channel1")
    assert response.is_ok

    await nsq.rdy(1)
    message = await nsq.message_queue.get()
    assert not message.is_processed
    await message.fin()

    with pytest.raises(RuntimeWarning) as warning:
        await message.fin()
    assert str(warning.value) == "Message has already been processed"

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_read_messages_via_generator():
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("test_read_messages_via_generator", "test_message")
    assert response.is_ok

    await nsq.subscribe("test_read_messages_via_generator", "channel1")
    assert nsq.is_subscribed

    processed_messages = 0

    async for message in nsq.messages():
        assert not message.is_processed
        await message.fin()
        processed_messages += 1

        if nsq.message_queue.qsize() == 0:
            break

    assert processed_messages > 0

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_read_single_message_via_get_message():
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("test_read_single_message_via_get_message", "test_message")
    assert response.is_ok

    await nsq.subscribe("test_read_single_message_via_get_message", "channel1")
    assert nsq.is_subscribed

    message = None
    iterations_count = 0
    while not message:
        if iterations_count == 10:
            break
        iterations_count += 1
        await asyncio.sleep(0.1)
        message = nsq.get_message()

    assert isinstance(message, NSQMessage)
    assert not message.is_processed
    await message.fin()

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_read_bytes_message():
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("test_read_bytes_message", b"\xa1")
    assert response.is_ok

    response = await nsq.sub("test_read_bytes_message", "channel1")
    assert response.is_ok

    await nsq.rdy(1)
    message = await nsq.message_queue.get()
    assert not message.is_processed

    assert message.body == b"\xa1"
    with pytest.raises(UnicodeDecodeError):
        str(message)

    await message.fin()
    assert message.is_processed

    await nsq.close()
    assert nsq.is_closed
