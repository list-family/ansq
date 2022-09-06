import pytest

from ansq import ConnectionOptions, open_connection


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


@pytest.mark.asyncio
async def test_connection_options_as_kwargs(nsqd):
    nsq = await open_connection(debug=True)
    assert nsq._options.debug is True
    await nsq.close()


@pytest.mark.asyncio
async def test_feature_options_as_kwargs(nsqd):
    nsq = await open_connection(heartbeat_interval=30001)
    assert nsq._options.features.heartbeat_interval == 30001
    await nsq.close()


@pytest.mark.asyncio
async def test_invalid_kwarg(nsqd):
    with pytest.raises(
        TypeError, match="got an unexpected keyword argument: 'invalid_kwarg'"
    ):
        await open_connection(invalid_kwarg=1)
