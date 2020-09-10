import asyncio
from time import sleep, time

import pytest

from ansq import open_connection
from ansq.tcp.connection import NSQConnection
from ansq.tcp.exceptions import ConnectionClosedError


@pytest.mark.asyncio
async def test_command_pub():
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("test_topic", f"test_message1, timestamp={time()}")
    assert response.is_ok

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_command_pub_after_reconnect():
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("test_topic", f"test_message1, timestamp={time()}")
    assert response.is_ok

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    response2 = await nsq.pub("test_topic", f"test_message1, timestamp={time()}")
    assert response2.is_ok

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_command_mpub():
    nsq = await open_connection()
    assert nsq.status.is_connected

    messages = ["message" for _ in range(10)]

    response = await nsq.mpub("test_topic", messages)
    assert response.is_ok

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_command_without_identity():
    nsq = NSQConnection()
    await nsq.connect()
    assert nsq.status.is_connected

    response = await nsq.pub("test_topic", "test_message")
    assert response.is_ok

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_command_without_connection():
    nsq = NSQConnection()
    assert nsq.status.is_init

    with pytest.raises(
        AssertionError, match="^You should call `connect` method first$",
    ):
        await nsq.pub("test_topic", "test_message")

    await nsq.close()
    assert nsq.status.is_init


@pytest.mark.asyncio
async def test_command_sub():
    nsq = NSQConnection()
    await nsq.connect()
    assert nsq.status.is_connected

    response = await nsq.sub("test_topic", "channel1")
    assert response.is_ok

    await nsq.close()
    assert nsq.is_closed


@pytest.mark.asyncio
async def test_command_with_closed_connection():
    nsq = await open_connection()
    await nsq.close()

    with pytest.raises(ConnectionClosedError, match="^Connection is closed$"):
        await nsq.pub("test_topic", "test_message")


@pytest.mark.asyncio
async def test_command_with_concurrently_closed_connection():
    nsq = await open_connection()

    async def wait_and_close():
        await asyncio.sleep(0)
        await nsq.close()

    async def blocking_wait_and_pub():
        sleep(0.1)
        await nsq.pub("test_topic", "test_message")

    with pytest.raises(ConnectionClosedError, match="^Connection is closed$"):
        await asyncio.wait_for(
            asyncio.gather(wait_and_close(), blocking_wait_and_pub()), timeout=1,
        )
