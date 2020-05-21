import pytest
from ansq import open_connection


@pytest.mark.asyncio
async def test_connection():
    nsq = await open_connection()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


@pytest.mark.asyncio
async def test_reconnect_after_close():
    nsq = await open_connection()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


@pytest.mark.asyncio
async def test_reconnect_while_connected():
    nsq = await open_connection()
    assert nsq.status.is_connected

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed
