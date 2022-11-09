import pytest

from ansq.http.writer import NSQDHTTPWriter


@pytest.fixture
async def writer(event_loop, nsqd):
    http_writer = NSQDHTTPWriter(loop=event_loop)
    yield http_writer
    await http_writer.close()


async def test_ping(writer):
    res = await writer.ping()
    assert res == "OK"


async def test_info(writer):
    res = await writer.info()
    assert res["tcp_port"] == 4150
    assert res["http_port"] == 4151


async def test_pub(writer):
    res = await writer.pub("test-topic", "test-message")
    assert res == "OK"


async def test_mpub(writer):
    res = await writer.mpub("test-topic", "test-message1", "test-message2")
    assert res == "OK"


async def test_create_topic(writer):
    res = await writer.create_topic("test-create-topic")
    assert res == ""


async def test_delete_topic(writer):
    topic = "test-delete-topic"
    await writer.create_topic(topic)

    res = await writer.delete_topic(topic)
    assert res == ""


async def test_create_channel(writer):
    topic = "test-create-channel-topic"
    await writer.create_topic(topic)

    channel = "test-create-channel"
    res = await writer.create_channel(topic, channel)
    assert res == ""


async def test_delete_channel(writer):
    topic = "test-delete-channel-topic"
    await writer.create_topic(topic)

    channel = "test-delete-channel"
    res = await writer.create_channel(topic, channel)

    res = await writer.delete_channel(topic, channel)
    assert res == ""


async def test_empty_topic(writer):
    topic = "test-empty-topic"
    await writer.create_topic(topic)

    res = await writer.empty_topic(topic)
    assert res == ""


@pytest.mark.parametrize("method_name", ("topic_pause", "topic_unpause"))
async def test_pausable_topic(writer, method_name):
    topic = "test-pausable-topic"
    await writer.create_topic(topic)

    method = getattr(writer, method_name)
    res = await method(topic)
    assert res == ""


@pytest.mark.parametrize("method_name", ("pause_channel", "unpause_channel"))
async def test_pausable_channel(writer, method_name):
    topic = "test-pausable-channel-topic"
    await writer.create_topic(topic)

    channel = "test-pausable-channel"
    res = await writer.create_channel(topic, channel)

    method = getattr(writer, method_name)
    res = await method(channel=channel, topic=topic)
    assert res == ""
