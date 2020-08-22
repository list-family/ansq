from typing import Any

from ansq.typedefs import HTTPResponse

from ..utils import convert_to_str
from .base import NSQHTTPConnection


class NSQDHTTPWriter(NSQHTTPConnection):
    """
    :see: http://nsq.io/components/nsqd.html
    """

    async def ping(self) -> HTTPResponse:
        """Monitoring endpoint.
        :returns: should return `"OK"`, otherwise raises an exception.
        """
        return await self.perform_request("GET", "ping", None, None)

    async def info(self) -> HTTPResponse:
        """Returns version information."""
        resp = await self.perform_request("GET", "info", None, None)
        return resp

    async def stats(self) -> HTTPResponse:
        """Returns stats information."""
        resp = await self.perform_request("GET", "stats", {"format": "json"}, None)
        return resp

    async def pub(self, topic: str, message: Any) -> HTTPResponse:
        resp = await self.perform_request("POST", "pub", {"topic": topic}, message)
        return resp

    async def mpub(self, topic: str, *messages: Any) -> HTTPResponse:
        assert len(messages), "Specify one or more messages"
        msgs = "\n".join(convert_to_str(m) for m in messages)
        resp = await self.perform_request("POST", "mpub", {"topic": topic}, msgs)
        return resp

    async def create_topic(self, topic: str) -> HTTPResponse:
        resp = await self.perform_request(
            "POST", "topic/create", {"topic": topic}, None,
        )
        return resp

    async def delete_topic(self, topic: str) -> HTTPResponse:
        resp = await self.perform_request(
            "POST", "topic/delete", {"topic": topic}, None,
        )
        return resp

    async def create_channel(self, topic: str, channel: str) -> HTTPResponse:
        resp = await self.perform_request(
            "POST", "channel/create", {"topic": topic, "channel": channel}, None,
        )
        return resp

    async def delete_channel(self, topic: str, channel: str) -> HTTPResponse:
        resp = await self.perform_request(
            "POST", "channel/delete", {"topic": topic, "channel": channel}, None,
        )
        return resp

    async def empty_topic(self, topic: str) -> HTTPResponse:
        resp = await self.perform_request("POST", "topic/empty", {"topic": topic}, None)
        return resp

    async def topic_pause(self, topic: str) -> HTTPResponse:
        resp = await self.perform_request("POST", "topic/pause", {"topic": topic}, None)
        return resp

    async def topic_unpause(self, topic: str) -> HTTPResponse:
        resp = await self.perform_request(
            "POST", "topic/unpause", {"topic": topic}, None,
        )
        return resp

    async def pause_channel(self, channel: str, topic: str) -> HTTPResponse:
        resp = await self.perform_request(
            "POST", "channel/pause", {"topic": topic, "channel": channel}, None,
        )
        return resp

    async def unpause_channel(self, channel: str, topic: str) -> HTTPResponse:
        resp = await self.perform_request(
            "POST", "channel/unpause", {"topic": topic, "channel": channel}, None,
        )
        return resp

    async def debug_pprof(self) -> HTTPResponse:
        resp = await self.perform_request("GET", "debug/pprof", None, None)
        return resp

    async def debug_pprof_profile(self) -> HTTPResponse:
        resp = await self.perform_request("GET", "debug/pprof/profile", None, None)
        return resp

    async def debug_pprof_goroutine(self) -> HTTPResponse:
        resp = await self.perform_request("GET", "debug/pprof/goroutine", None, None)
        return resp

    async def debug_pprof_heap(self) -> HTTPResponse:
        resp = await self.perform_request("GET", "debug/pprof/heap", None, None)
        return resp

    async def debug_pprof_block(self) -> HTTPResponse:
        resp = await self.perform_request("GET", "debug/pprof/block", None, None)
        return resp

    async def debug_pprof_threadcreate(self) -> HTTPResponse:
        resp = await self.perform_request("GET", "debug/pprof/threadcreate", None, None)
        return resp

    async def nsqlookupd_tcp_addresses(self) -> HTTPResponse:
        """
        List of nsqlookupd TCP addresses.
        """
        resp = await self.perform_request(
            "GET", "config/nsqlookupd_tcp_addresses", None, None,
        )
        return resp
