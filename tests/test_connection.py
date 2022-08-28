import pytest

from ansq import ConnectionOptions, open_connection


@pytest.mark.asyncio
async def test_connection(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


@pytest.mark.asyncio
async def test_reconnect_after_close(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


@pytest.mark.asyncio
async def test_reconnect_while_connected(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


@pytest.mark.asyncio
async def test_auto_reconnect(nsqd, wait_for):
    nsq = await open_connection(
        connection_options=ConnectionOptions(auto_reconnect=True)
    )
    assert nsq.status.is_connected

    await nsqd.stop()
    await wait_for(lambda: nsq.status.is_reconnecting)

    await nsqd.start()
    await wait_for(lambda: nsq.status.is_connected)

    await nsq.close()
    assert nsq.status.is_closed
