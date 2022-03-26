import asyncio
import inspect
import os
import shutil
import signal
import time
from asyncio.subprocess import Process
from typing import Awaitable, Callable, Union

import async_generator
import pytest

from ansq.http import NSQDHTTPWriter

pytestmark = pytest.mark.asyncio


class AsyncNSQD:
    """Simple async nsqd server. Requires installed nsqd binary."""

    _process: Process

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 4150,
        http_port: int = 4151,
        data_path="/tmp",
    ) -> None:
        self.host = host
        self.port = port
        self.http_port = http_port
        self.data_path = data_path
        self._nsqd_command = "nsqd"

        if shutil.which(self._nsqd_command) is None:
            raise RuntimeError(
                "nsqd must be installed. "
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

    async def start(self):
        """Start nsqd in a separate process."""
        self._process = await asyncio.create_subprocess_exec(
            self._nsqd_command,
            "-tcp-address",
            self.tcp_address,
            "-http-address",
            self.http_address,
            "-data-path",
            self.data_path,
        )
        await self._wait_ping()

    async def stop(self):
        """Stop nsqd."""
        if not hasattr(self, "_process"):
            return

        os.kill(self._process.pid, signal.SIGKILL)
        await self._process.wait()

    async def _wait_ping(self, timeout: int = 3) -> None:
        """Wait for successful ping to HTTP API, otherwise raise last exception."""
        http_writer = NSQDHTTPWriter(host=self.host, port=self.http_port)
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


@pytest.fixture
def create_nsqd(tmp_path):
    @async_generator.asynccontextmanager
    async def _create_nsqd(host="127.0.0.1", port=4150, http_port=4151):
        data_path = tmp_path / f"{host}:{port}"
        data_path.mkdir(parents=True)

        nsqd = AsyncNSQD(
            host=host, port=port, http_port=http_port, data_path=str(data_path),
        )
        try:
            await nsqd.start()
            yield nsqd
        finally:
            await nsqd.stop()

    return _create_nsqd


@pytest.fixture(autouse=True)
async def nsqd(create_nsqd) -> AsyncNSQD:
    async with create_nsqd() as nsqd:
        yield nsqd


@pytest.fixture
def wait_for():
    """ Wait for a predicate with a timeout."""

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

            if time.time() - start > timeout:
                raise AssertionError("failed to wait for predicate")

            await asyncio.sleep(sleep_time)

    return inner
