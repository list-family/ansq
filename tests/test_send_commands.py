import asyncio
from time import sleep, time

import pytest

from ansq import open_connection
from ansq.tcp.connection import NSQConnection
from ansq.tcp.exceptions import ConnectionClosedError


async def test_command_pub(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    response = await nsq.pub("test_topic", f"test_message1, timestamp={time()}")
    assert response.is_ok

    await nsq.close()
    assert nsq.is_closed


async def test_command_pub_after_reconnect(nsqd):
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


async def test_command_mpub(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    messages = ["message" for _ in range(10)]

    response = await nsq.mpub("test_topic", messages)
    assert response.is_ok

    await nsq.close()
    assert nsq.is_closed


async def test_command_without_identity(nsqd):
    nsq = NSQConnection()
    await nsq.connect()
    assert nsq.status.is_connected

    response = await nsq.pub("test_topic", "test_message")
    assert response.is_ok

    await nsq.close()
    assert nsq.is_closed


async def test_command_without_connection(nsqd):
    nsq = NSQConnection()
    assert nsq.status.is_init

    with pytest.raises(
        AssertionError, match="^You should call `connect` method first$"
    ):
        await nsq.pub("test_topic", "test_message")

    await nsq.close()
    assert nsq.status.is_init


async def test_command_sub(nsqd):
    nsq = NSQConnection()
    await nsq.connect()
    assert nsq.status.is_connected

    response = await nsq.sub("test_topic", "channel1")
    assert response.is_ok

    await nsq.close()
    assert nsq.is_closed


async def test_command_with_closed_connection(nsqd):
    nsq = await open_connection()
    await nsq.close()

    with pytest.raises(ConnectionClosedError, match="^Connection is closed$"):
        await nsq.pub("test_topic", "test_message")


async def test_command_with_concurrently_closed_connection(nsqd):
    nsq = await open_connection()

    async def close():
        await nsq.close()

    async def blocking_wait_and_pub():
        sleep(0.1)
        await nsq.pub("test_topic", "test_message")

    with pytest.raises(ConnectionClosedError, match="^Connection is closed$"):
        await asyncio.wait_for(
            asyncio.gather(close(), blocking_wait_and_pub()), timeout=1
        )
