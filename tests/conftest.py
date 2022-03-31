import abc
import asyncio
import inspect
import os
import shutil
import signal
import time
from asyncio.subprocess import Process
from typing import Awaitable, Callable, List, Optional, Sequence, Type, Union

import async_generator
import pytest

from ansq.http import NSQDHTTPWriter, NsqLookupd

pytestmark = pytest.mark.asyncio


class BaseNSQServer(abc.ABC):
    """Base async nsq server."""

    http_writer_class: Type

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 4150,
        http_port: int = 4151,
    ) -> None:
        self.host = host
        self.port = port
        self.http_port = http_port
        self._process: Optional[Process] = None

        if shutil.which(self.command) is None:
            raise RuntimeError(
                f"{self.command} must be installed. "
                "Follow the instructions in the installing doc: "
                "https://nsq.io/deployment/installing.html",
            )

    def __repr__(self):
        return f"{type(self).__name__}({self.host!r}, {self.port})"

    @property
    def tcp_address(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def http_address(self) -> str:
        return f"{self.host}:{self.http_port}"

    @property
    @abc.abstractmethod
    def command(self) -> str:
        ...

    @property
    def command_args(self) -> List[str]:
        return [
            "-tcp-address",
            self.tcp_address,
            "-http-address",
            self.http_address,
        ]

    async def start(self):
        """Start nsqd in a separate process."""
        if self._process is not None:
            return

        self._process = await asyncio.create_subprocess_exec(
            self.command, *self.command_args
        )
        await self._wait_ping()

    async def stop(self):
        """Stop nsqd."""
        if self._process is None:
            return

        os.kill(self._process.pid, signal.SIGKILL)
        await self._process.wait()
        self._process = None

    async def _wait_ping(self, timeout: int = 3) -> None:
        """Wait for successful ping to HTTP API, otherwise raise last exception."""
        http_writer = self.http_writer_class(host=self.host, port=self.http_port)
        start = time.time()
        while True:
            try:
                res = await http_writer.ping()
            except Exception:
                res = None

            if res == "OK":
                break

            if time.time() - start > timeout:
                raise

            await asyncio.sleep(0.1)

        await http_writer.close()


class NSQD(BaseNSQServer):
    """Simple async nsqd server. Requires installed nsqd binary."""

    http_writer_class = NSQDHTTPWriter

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 4150,
        http_port: int = 4151,
        data_path="/tmp",
        broadcast_address: Optional[str] = None,
        lookupd_tcp_addresses: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            http_port=http_port,
        )
        self.data_path = data_path
        self.broadcast_address = broadcast_address
        self.lookupd_tcp_addresses = lookupd_tcp_addresses or []

    @property
    def command(self) -> str:
        return "nsqd"

    @property
    def command_args(self) -> List[str]:
        args = super().command_args + ["-data-path", self.data_path]

        if self.lookupd_tcp_addresses:
            for address in self.lookupd_tcp_addresses:
                args.extend(["-lookupd-tcp-address", address])

        if self.broadcast_address:
            args.extend(["-broadcast-address", self.broadcast_address])

        return args


class NSQLookupD(BaseNSQServer):
    http_writer_class = NsqLookupd

    @property
    def command(self) -> str:
        return "nsqlookupd"


@pytest.fixture
def create_nsqd(tmp_path):
    @async_generator.asynccontextmanager
    async def _create_nsqd(
        host="127.0.0.1",
        port=4150,
        http_port=4151,
        lookupd_tcp_addresses=None,
        broadcast_address="127.0.0.1",
    ):
        data_path = tmp_path / f"{host}:{port}"
        data_path.mkdir(parents=True)

        nsqd = NSQD(
            host=host,
            port=port,
            http_port=http_port,
            data_path=str(data_path),
            lookupd_tcp_addresses=lookupd_tcp_addresses,
            broadcast_address=broadcast_address,
        )
        try:
            await nsqd.start()
            yield nsqd
        finally:
            await nsqd.stop()

    return _create_nsqd


@pytest.fixture
def create_nsqlookupd():
    @async_generator.asynccontextmanager
    async def _create_nsqlookupd(host="127.0.0.1", port=4160, http_port=4161):
        nsqlookupd = NSQLookupD(host=host, port=port, http_port=http_port)
        try:
            await nsqlookupd.start()
            yield nsqlookupd
        finally:
            await nsqlookupd.stop()

    return _create_nsqlookupd


@pytest.fixture(autouse=True)
async def nsqd(create_nsqd) -> NSQD:
    async with create_nsqd() as nsqd:
        yield nsqd


@pytest.fixture
def wait_for():
    """Wait for a predicate with a timeout."""

    async def inner(
        predicate: Union[Callable[..., bool], Callable[..., Awaitable[bool]]],
        timeout: float = 5.0,
        sleep_time: float = 0.1,
    ):
        __tracebackhide__ = True

        start = time.time()

        while True:
            predicate_result = (
                await predicate()
                if inspect.iscoroutinefunction(predicate)
                else predicate()
            )
            if predicate_result:
                return

            if time.time() - start > timeout:  # pragma: no cover
                raise AssertionError("failed to wait for predicate")

            await asyncio.sleep(sleep_time)

    return inner
