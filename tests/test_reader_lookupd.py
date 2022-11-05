import pytest

from ansq import create_reader, create_writer


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


@pytest.fixture
def register_producers():
    async def _register_producers(*servers):
        for i, server in enumerate(servers):
            writer = await create_writer(nsqd_tcp_addresses=[server.tcp_address])
            response = await writer.pub(topic="foo", message=f"test_message{i}")
            assert response.is_ok
            await writer.close()

    return _register_producers


async def test_create_reader(
    nsqlookupd, nsqlookupd2, nsqd, nsqd2, wait_for, register_producers
):
    reader = await create_reader(
        topic="foo",
        channel="bar",
        lookupd_http_addresses=[nsqlookupd.http_address, nsqlookupd2.http_address],
        lookupd_poll_interval=100,
    )

    assert reader.topic == "foo"
    assert reader.channel == "bar"
    assert len(reader.connections) == 0

    await register_producers(nsqd, nsqd2)
    await wait_for(lambda: len(reader.connections) == 2)

    await reader.close()


async def test_create_connection(nsqlookupd, wait_for, nsqd, register_producers):
    reader = await create_reader(
        topic="foo",
        channel="bar",
        lookupd_http_addresses=[nsqlookupd.http_address],
        lookupd_poll_interval=100,
    )
    assert len(reader.connections) == 0

    await register_producers(nsqd)
    await wait_for(lambda: len(reader.connections) == 1)

    await reader.close()


async def test_close_connection(nsqlookupd, nsqd, wait_for, register_producers):
    reader = await create_reader(
        topic="foo",
        channel="bar",
        lookupd_http_addresses=[nsqlookupd.http_address],
        lookupd_poll_interval=100,
    )

    await register_producers(nsqd)
    await wait_for(lambda: len(reader.connections) == 1)

    await nsqd.stop()
    await wait_for(lambda: len(reader.connections) == 0)

    await reader.close()


async def test_restore_connection(nsqlookupd, nsqd, wait_for, register_producers):
    reader = await create_reader(
        topic="foo",
        channel="bar",
        lookupd_http_addresses=[nsqlookupd.http_address],
        lookupd_poll_interval=100,
    )

    await register_producers(nsqd)
    await wait_for(lambda: len(reader.connections) == 1)

    await nsqd.stop()
    await wait_for(lambda: len(reader.connections) == 0)

    await nsqd.start()
    await wait_for(lambda: len(reader.connections) == 1)

    await reader.close()
