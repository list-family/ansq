import asyncio

import pytest

from ansq import ConnectionFeatures, ConnectionOptions, open_connection
from ansq.tcp.types import NSQCommands, TCPConnection


async def test_connection(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


async def test_reconnect_after_close(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


async def test_reconnect_while_connected(nsqd):
    nsq = await open_connection()
    assert nsq.status.is_connected

    assert await nsq.reconnect()
    assert nsq.status.is_connected

    await nsq.close()
    assert nsq.status.is_closed


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


async def test_invalid_feature(create_nsqd, wait_for, nsqd):
    nsq = await open_connection(
        connection_options=ConnectionOptions(
            # Default max heartbeat is 60s
            features=ConnectionFeatures(heartbeat_interval=60001)
        )
    )
    assert nsq.status.is_closed


async def test_connection_options_as_kwargs(nsqd):
    nsq = await open_connection(debug=True)
    assert nsq._options.debug is True
    await nsq.close()


async def test_feature_options_as_kwargs(nsqd):
    nsq = await open_connection(heartbeat_interval=30001)
    assert nsq._options.features.heartbeat_interval == 30001
    await nsq.close()


async def test_invalid_kwarg(nsqd):
    with pytest.raises(
        TypeError, match="got an unexpected keyword argument: 'invalid_kwarg'"
    ):
        await open_connection(invalid_kwarg=1)


@pytest.mark.parametrize(
    "cmd", (NSQCommands.RDY, NSQCommands.FIN, NSQCommands.TOUCH, NSQCommands.REQ)
)
async def test_errors_from_commands_without_responses(nsqd, wait_for, cmd, caplog):
    nsq = await open_connection()

    response = await nsq.execute(cmd)
    await wait_for(lambda: caplog.messages)
    await nsq.close()

    expected_log = f"[E_INVALID] cannot {cmd.decode('utf8')} in current state"
    assert expected_log in caplog.text
    assert response is None


async def test_on_heartbeat(nsqd):
    heartbeats_count = 0

    async def inc_heartbeats_count(conn):
        nonlocal heartbeats_count
        heartbeats_count += 1
        assert isinstance(conn, TCPConnection)
        assert conn.is_connected

    nsq = await open_connection(
        connection_options=ConnectionOptions(
            on_heartbeat=inc_heartbeats_count,
            features=ConnectionFeatures(heartbeat_interval=1000),
        ),
    )
    await asyncio.sleep(1.1)
    assert heartbeats_count == 1
    await nsq.close()
