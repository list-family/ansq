# ansq - Async NSQ
[![PyPI version](https://badge.fury.io/py/ansq.svg)](https://badge.fury.io/py/ansq)
![Tests](https://github.com/list-family/ansq/workflows/Test/badge.svg)
[![Coverage](https://codecov.io/gh/list-family/ansq/branch/master/graph/badge.svg)](https://codecov.io/gh/list-family/ansq)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ansq)

Written with native Asyncio NSQ package

## Installation

```commandline
python -m pip install ansq
```

## Overview
- `Reader` — high-level class for building consumers with `nsqlookupd` support
- `Writer` — high-level producer class supporting async publishing of messages to `nsqd`
  over the TCP protocol
- `NSQConnection` — low-level class representing a TCP connection to `nsqd`:
    - full TCP wrapper
    - one connection for `sub` and `pub`
    - self-healing: when the connection is lost, reconnects, sends identify
      and auth commands, subscribes to previous topic/channel

## Features

- [x] SUB
- [x] PUB
- [x] Discovery
- [ ] Backoff
- [ ] TLS
- [ ] Deflate
- [ ] Snappy
- [x] Sampling
- [ ] AUTH

## Usage

### Consumer

A simple consumer reads messages from "example_topic" and prints them to stdout.

```python
import asyncio

import ansq


async def main():
    reader = await ansq.create_reader(
        topic="example_topic",
        channel="example_channel",
    )

    async for message in reader.messages():
        print(f"Message: {message.body}")
        await message.fin()

    await reader.close()


if __name__ == "__main__":
    asyncio.run(main())
```

### Producer

A simple producer sends a "Hello, world!" message to "example_topic".

```python
import asyncio

import ansq


async def main():
    writer = await ansq.create_writer()
    await writer.pub(
        topic="example_topic",
        message="Hello, world!",
    )
    await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
```


## Contributing

Create and activate a development virtual environment.

```bash
python -m venv venv
source venv/bin/activate
```

Install `ansq` in editable mode and its testing dependencies.

```bash
python -m pip install -e .[testing]
```

Run tests.

```bash
pytest
```
