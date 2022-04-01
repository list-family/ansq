import asyncio
import json
from typing import TYPE_CHECKING, Any, Optional, Type, TypeVar

import aiohttp

from ansq.typedefs import HTTPResponse

from ..utils import convert_to_str
from .http_exceptions import HTTP_EXCEPTIONS, NSQHTTPException

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop


_T = TypeVar("_T", bound="NSQHTTPConnection")


class NSQHTTPConnection:
    """XXX"""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 4151,
        *,
        loop: Optional["AbstractEventLoop"] = None,
    ) -> None:
        self._loop = loop or asyncio.get_event_loop()
        self._endpoint = (host, port)
        self._base_url = "http://{}:{}/".format(*self._endpoint)

        self._session = aiohttp.ClientSession()

    @classmethod
    def from_address(
        cls: Type[_T],
        address: str,
        loop: Optional["AbstractEventLoop"] = None,
    ) -> _T:
        try:
            host, port_str = address.split(":")
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Invalid address: {address}")

        return cls(host=host, port=port, loop=loop)

    @property
    def endpoint(self) -> str:
        return "http://{}:{}".format(*self._endpoint)

    async def close(self) -> None:
        await self._session.close()

    async def perform_request(
        self, method: str, url: str, params: Any, body: Any
    ) -> HTTPResponse:
        _body = convert_to_str(body) if body else body
        url = self._base_url + url
        resp = await self._session.request(method, url, params=params, data=_body)
        resp_body = await resp.text()
        try:
            response = json.loads(resp_body)
        except ValueError:
            return resp_body

        if not (200 <= resp.status <= 300):
            extra = None
            try:
                extra = json.loads(resp_body)
            except ValueError:
                pass
            exc_class = HTTP_EXCEPTIONS.get(resp.status, NSQHTTPException)
            raise exc_class(resp.status, resp_body, extra)
        return response

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"<{cls_name}: {self._endpoint}>"
