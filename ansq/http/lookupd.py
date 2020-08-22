from typing import TYPE_CHECKING

from .base import NSQHTTPConnection

if TYPE_CHECKING:
    from .base import _HTTPResponse_T


class NsqLookupd(NSQHTTPConnection):
    """
    :see: http://nsq.io/components/nsqlookupd.html
    """

    async def ping(self) -> "_HTTPResponse_T":
        """Monitoring endpoint.
        :returns: should return `"OK"`, otherwise raises an exception.
        """
        return await self.perform_request("GET", "ping", None, None)

    async def info(self) -> "_HTTPResponse_T":
        """Returns version information."""
        response = await self.perform_request("GET", "info", None, None)
        return response

    async def lookup(self, topic: str) -> "_HTTPResponse_T":
        """XXX

        :param topic:
        :return:
        """
        response = await self.perform_request("GET", "lookup", {"topic": topic}, None)
        return response

    async def topics(self) -> "_HTTPResponse_T":
        """XXX

        :return:
        """
        resp = await self.perform_request("GET", "topics", None, None)
        return resp

    async def channels(self, topic: str) -> "_HTTPResponse_T":
        """XXX

        :param topic:
        :return:
        """
        resp = await self.perform_request("GET", "channels", {"topic": topic}, None)
        return resp

    async def nodes(self) -> "_HTTPResponse_T":
        """XXX

        :return:
        """
        resp = await self.perform_request("GET", "nodes", None, None)
        return resp

    async def create_topic(self, topic: str) -> "_HTTPResponse_T":
        """XXX

        :param topic:
        :return:
        """
        resp = await self.perform_request(
            "POST", "/topic/create", {"topic": topic}, None,
        )
        return resp

    async def delete_topic(self, topic: str) -> "_HTTPResponse_T":
        """XXX

        :param topic:
        :return:
        """
        resp = await self.perform_request(
            "POST", "/topic/delete", {"topic": topic}, None,
        )
        return resp

    async def create_channel(self, topic: str, channel: str) -> "_HTTPResponse_T":
        """XXX

        :param topic:
        :param channel:
        :return:
        """
        resp = await self.perform_request(
            "POST", "/channel/create", {"topic": topic, "channel": channel}, None,
        )
        return resp

    async def delete_channel(self, topic: str, channel: str) -> "_HTTPResponse_T":
        """XXX

        :param topic:
        :param channel:
        :return:
        """
        resp = await self.perform_request(
            "POST", "/channel/delete", {"topic": topic, "channel": channel}, None,
        )
        return resp

    async def tombstone_topic_producer(
        self, topic: str, node: str
    ) -> "_HTTPResponse_T":
        """XXX

        :param topic:
        :param node:
        :return:
        """
        resp = await self.perform_request(
            "POST", "delete_channel", {"topic": topic, "node": node}, None,
        )
        return resp
