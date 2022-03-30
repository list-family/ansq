import pytest

from ansq import create_reader, create_writer

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
async def nsqd(tmp_path, create_nsqd, nsqlookupd):
    async with create_nsqd(lookupd_tcp_addresses=[nsqlookupd.tcp_address]) as nsqd:
        yield nsqd


@pytest.fixture
async def nsqd2(tmp_path, create_nsqd, nsqlookupd2):
    async with create_nsqd(
        port=4250, http_port=4251, lookupd_tcp_addresses=[nsqlookupd2.tcp_address]
    ) as nsqd:
        yield nsqd


@pytest.fixture
async def nsqlookupd(create_nsqlookupd):
    async with create_nsqlookupd() as nsqlookupd:
        yield nsqlookupd


@pytest.fixture
async def nsqlookupd2(create_nsqlookupd):
    async with create_nsqlookupd(port=4260, http_port=4261) as nsqlookupd:
        yield nsqlookupd


async def test_create_reader_with_lookupd(
    nsqd, nsqlookupd, nsqd2, nsqlookupd2, wait_for
):
    writer = await create_writer(nsqd_tcp_addresses=[nsqd.tcp_address])
    response = await writer.pub(topic="foo", message="test_message1")
    assert response.is_ok
    await writer.close()

    writer = await create_writer(nsqd_tcp_addresses=[nsqd2.tcp_address])
    response = await writer.pub(topic="foo", message="test_message2")
    assert response.is_ok
    await writer.close()

    reader = await create_reader(
        topic="foo",
        channel="bar",
        lookupd_http_addresses=[nsqlookupd.http_address, nsqlookupd2.http_address],
        lookupd_poll_interval=100,
    )

    assert reader.topic == "foo"
    assert reader.channel == "bar"

    await wait_for(lambda: len(reader.connections) == 2)

    assert len(reader.connections) == 2
    assert reader.max_in_flight == 2

    await reader.close()
