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
    await reader.set_max_in_flight(2)

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


async def test_set_max_in_flight(nsqd):
    reader = await create_reader(topic="foo", channel="bar")

    await reader.set_max_in_flight(7)

    assert reader.max_in_flight == 7

    await reader.close()


@pytest.mark.parametrize(
    "max_in_flight, expected_rdys",
    (
        (0, (0, 0)),
        (1, (1, 0)),
        (2, (1, 1)),
        (3, (1, 2)),
        (4, (2, 2)),
        (5, (2, 3)),
    ),
)
async def test_distribute_evenly_max_in_flight(
    nsqd, nsqd2, max_in_flight, expected_rdys
):
    reader = await create_reader(
        topic="foo",
        channel="bar",
        nsqd_tcp_addresses=[nsqd.tcp_address, nsqd2.tcp_address],
    )

    await reader.set_max_in_flight(max_in_flight)
    assert get_rdys(reader) == expected_rdys

    await reader.close()


async def test_redistribute_max_in_flight_on_close_connection(nsqd, nsqd2, wait_for):
    reader = await create_reader(
        topic="foo",
        channel="bar",
        nsqd_tcp_addresses=[nsqd.tcp_address, nsqd2.tcp_address],
    )

    await reader.set_max_in_flight(5)
    assert get_rdys(reader) == (2, 3)

    await reader.connections[0].close()
    await wait_for(lambda: get_rdys(reader) == (5,))

    await reader.close()


def get_rdys(reader) -> tuple[int, ...]:
    return tuple(
        conn.rdy_messages_count for conn in reader.connections if conn.is_connected
    )
