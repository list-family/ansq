import pytest

from ansq import create_reader, create_writer
from ansq.tcp.writer import Writer


@pytest.fixture
async def nsqd2(tmp_path, create_nsqd):
    async with create_nsqd(port=4250, http_port=4251) as nsqd:
        yield nsqd


async def test_create_writer(nsqd):
    writer = await create_writer()

    open_connections = [conn for conn in writer.connections if conn.is_connected]
    assert len(open_connections) == 1

    await writer.close()


async def test_connect_writer(nsqd):
    writer = Writer()
    assert not writer.connections

    await writer.connect()
    open_connections = [conn for conn in writer.connections if conn.is_connected]
    assert len(open_connections) == 1

    await writer.close()


async def test_close_writer(nsqd):
    writer = await create_writer()
    await writer.close()

    closed_connections = [conn for conn in writer.connections if conn.is_closed]
    assert len(closed_connections) == 1


async def test_pub(nsqd):
    writer = await create_writer()

    response = await writer.pub(topic="foo", message="test_message")
    assert response.is_ok

    await writer.close()


async def test_mpub(nsqd):
    writer = await create_writer()

    messages = [f"test_message_{i}" for i in range(10)]
    response = await writer.mpub("foo", *messages)
    assert response.is_ok

    await writer.close()


async def test_dpub(nsqd):
    writer = await create_writer()

    response = await writer.dpub(topic="foo", message="test_message", delay_time=1)
    assert response.is_ok

    await writer.close()


async def test_pub_to_multiple_tcp_addresses(nsqd, nsqd2):
    writer = await create_writer(
        nsqd_tcp_addresses=[nsqd.tcp_address, nsqd2.tcp_address],
    )

    response = await writer.pub(topic="foo", message="test_message")
    assert response.is_ok

    await writer.close()

    reader = await create_reader(
        topic="foo",
        channel="bar",
        nsqd_tcp_addresses=[nsqd.tcp_address, nsqd2.tcp_address],
    )

    message = await reader.wait_for_message()
    assert message.body == b"test_message"

    await reader.close()
