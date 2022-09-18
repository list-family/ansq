import pytest

from ansq import ConnectionFeatures, ConnectionOptions, open_connection


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


@pytest.mark.asyncio
async def test_invalid_feature(create_nsqd, wait_for, nsqd):
    nsq = await open_connection(
        connection_options=ConnectionOptions(
            # Default max heartbeat is 60s
            features=ConnectionFeatures(heartbeat_interval=60001)
        )
    )
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
