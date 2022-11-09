import pytest

from ansq import create_reader, open_connection
from ansq.tcp.reader import Reader


@pytest.fixture
async def nsqd2(tmp_path, create_nsqd):
    async with create_nsqd(port=4250, http_port=4251) as nsqd:
        yield nsqd


async def test_create_reader(nsqd):
    reader = await create_reader(topic="foo", channel="bar")

    assert reader.topic == "foo"
    assert reader.channel == "bar"
    assert reader.max_in_flight == 1

    await reader.close()


async def test_connect_reader(nsqd):
    reader = Reader(topic="foo", channel="bar")
    assert not reader.connections

    await reader.connect()
    open_connections = [conn for conn in reader.connections if conn.is_connected]
    assert len(open_connections) == 1

    await reader.close()


async def test_close_reader(nsqd):
    reader = await create_reader(topic="foo", channel="bar")
    await reader.close()

    closed_connections = [conn for conn in reader.connections if conn.is_closed]
    assert len(closed_connections) == 1


async def test_wait_for_message(nsqd):
    nsq = await open_connection(nsqd.host, nsqd.port)
    await nsq.pub(topic="foo", message="test_message")
    await nsq.close()

    reader = await create_reader(topic="foo", channel="bar")

    message = await reader.wait_for_message()
    await message.fin()
    assert message.body == b"test_message"

    await reader.close()


async def test_messages_generator(nsqd):
    nsq = await open_connection(nsqd.host, nsqd.port)
    await nsq.pub(topic="foo", message="test_message1")
    await nsq.pub(topic="foo", message="test_message2")
    await nsq.close()

    reader = await create_reader(topic="foo", channel="bar")

    read_messages = []
    async for message in reader.messages():
        read_messages.append(message.body.decode())
        await message.fin()
        if len(read_messages) >= 2:
            break

    assert read_messages == ["test_message1", "test_message2"]

    await reader.close()


async def test_read_from_multiple_tcp_addresses(nsqd, nsqd2):
    reader = await create_reader(
        topic="foo",
        channel="bar",
        nsqd_tcp_addresses=[nsqd.tcp_address, nsqd2.tcp_address],
    )

    nsq1 = await open_connection(nsqd.host, nsqd.port)
    await nsq1.pub(topic="foo", message="test_message1")
    await nsq1.close()

    message = await reader.wait_for_message()
    await message.fin()
    assert message.body == b"test_message1"

    nsq2 = await open_connection(nsqd2.host, nsqd2.port)
    await nsq2.pub(topic="foo", message="test_message2")
    await nsq2.close()

    message = await reader.wait_for_message()
    await message.fin()
    assert message.body == b"test_message2"

    await reader.close()
