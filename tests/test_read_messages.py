import asyncio
import re
from time import time

import pytest

from ansq import ConnectionFeatures, ConnectionOptions, open_connection
from ansq.tcp.types import NSQMessage


async def test_read_message(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    timestamp = time()

    response = await nsq.pub("test_read_message", f"hello sent at {timestamp}")
    assert response.is_ok

    response = await nsq.sub("test_read_message", "channel1")
    assert response.is_ok

    await nsq.rdy(1)
    message = await nsq.message_queue.get()
    assert message.can_be_processed

    await message.fin()
    assert message.is_processed

    await nsq.close()
    assert nsq.is_closed


async def test_read_message_and_req(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    timestamp = time()

    response = await nsq.pub("test_read_message_and_req", f"hello sent at {timestamp}")
    assert response.is_ok

    response = await nsq.sub("test_read_message_and_req", "channel1")
    assert response.is_ok

    await nsq.rdy(1)
    message = await nsq.message_queue.get()
    assert message.can_be_processed

    await message.req()
    assert message.is_processed

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.parametrize(
    "process",
    (
        pytest.param(lambda message: message.fin(), id="fin"),
        pytest.param(lambda message: message.req(), id="req"),
    ),
)
async def test_read_and_process_twice_message(nsqd, process):
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("topic", "foo")
    assert response.is_ok

    await nsq.subscribe("topic", "channel")

    message = await nsq.wait_for_message()
    assert message.can_be_processed

    # First process
    await process(message)

    with pytest.raises(
        RuntimeWarning, match=r"Message id=[^ ]+ has already been processed"
    ):
        await process(message)

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.parametrize(
    "process",
    (
        pytest.param(lambda message: message.fin(), id="fin"),
        pytest.param(lambda message: message.req(), id="req"),
        pytest.param(lambda message: message.touch(), id="touch"),
    ),
)
async def test_read_and_process_timed_out_message(nsqd, process):
    nsq = await open_connection(
        connection_options=ConnectionOptions(
            features=ConnectionFeatures(msg_timeout=1000)
        )
    )
    assert nsq.status.is_connected

    response = await nsq.pub("topic", "foo")
    assert response.is_ok

    await nsq.subscribe("topic", "channel")

    message = await nsq.wait_for_message()
    assert message.can_be_processed

    # Wait until message is timed out
    await asyncio.sleep(1.1)

    with pytest.raises(RuntimeWarning, match=r"Message id=[^ ]+ is timed out"):
        await process(message)

    await nsq.close()
    assert nsq.is_closed


async def test_read_messages_via_generator(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("test_read_messages_via_generator", "test_message")
    assert response.is_ok

    await nsq.subscribe("test_read_messages_via_generator", "channel1")
    assert nsq.is_subscribed

    processed_messages = 0

    async for message in nsq.messages():
        assert message.can_be_processed
        await message.fin()
        processed_messages += 1

        if nsq.message_queue.qsize() == 0:
            break

    assert processed_messages > 0

    await nsq.close()
    assert nsq.is_closed


async def test_read_single_message_via_get_message(nsqd):
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
    assert message.can_be_processed
    await message.fin()

    await nsq.close()
    assert nsq.is_closed


async def test_read_bytes_message(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("test_read_bytes_message", b"\xa1")
    assert response.is_ok

    response = await nsq.sub("test_read_bytes_message", "channel1")
    assert response.is_ok

    await nsq.rdy(1)
    message = await nsq.message_queue.get()
    assert message.can_be_processed

    assert message.body == b"\xa1"
    with pytest.raises(UnicodeDecodeError):
        str(message)

    await message.fin()
    assert not message.can_be_processed

    await nsq.close()
    assert nsq.is_closed


async def test_timeout_messages(nsqd, caplog):
    nsq = await open_connection(
        connection_options=ConnectionOptions(
            features=ConnectionFeatures(msg_timeout=1000),
        )
    )
    assert nsq.status.is_connected

    first_message = "first test message at " + str(time())
    first_message_response = await nsq.pub(
        "test_read_timed_out_messages",
        first_message,
    )
    assert first_message_response.is_ok

    second_message = "second test message at " + str(time())
    second_message_response = await nsq.pub(
        "test_read_timed_out_messages",
        second_message,
    )
    assert second_message_response.is_ok

    await nsq.subscribe("test_read_timed_out_messages", "channel1", 1)
    assert nsq.is_subscribed

    # We need to wait while the message will be timed out.
    await asyncio.sleep(1.1)

    async for message in nsq.messages():
        assert message.can_be_processed
        assert str(message) == second_message
        assert message.attempts == 1
        await message.fin()
        break

    error_log_regex = re.compile(r"Message id=[^ ]+ is timed out")
    assert error_log_regex.search(caplog.text) is not None

    message = await asyncio.wait_for(nsq.message_queue.get(), timeout=1)
    assert message.can_be_processed
    assert str(message) == first_message
    assert message.attempts == 2
    await message.fin()

    await nsq.close()
    assert nsq.is_closed


async def test_read_message_and_touch(nsqd):
    nsq = await open_connection(
        connection_options=ConnectionOptions(
            features=ConnectionFeatures(msg_timeout=1000)
        )
    )
    assert nsq.status.is_connected

    timestamp = time()

    response = await nsq.pub(
        "test_read_message_and_touch",
        f"hello sent at {timestamp}",
    )
    assert response.is_ok

    response = await nsq.sub("test_read_message_and_touch", "channel1")
    assert response.is_ok

    await nsq.rdy(1)

    message = await nsq.message_queue.get()
    assert message.can_be_processed
    await asyncio.sleep(0.51)

    assert message.can_be_processed
    await message.touch()
    assert message.can_be_processed
    await asyncio.sleep(0.51)

    assert message.can_be_processed
    await message.fin()
    assert not message.can_be_processed

    await nsq.close()
    assert nsq.is_closed
