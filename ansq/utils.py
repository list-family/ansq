import json
import logging
import re
from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import singledispatch
from sys import version_info
from typing import Union, Tuple, Optional
from urllib.parse import urlsplit

PY37 = version_info >= (3, 7)


class JSONEncoder(json.JSONEncoder):
    def default(self, obj) -> str:
        try:
            return convert_to_str(obj)
        except TypeError:
            return json.JSONEncoder.default(self, obj)


def get_host_port(uri: str) -> Tuple[str, Optional[int]]:
    """Get host and port from provided URI."""
    split_uri = urlsplit(uri)
    return split_uri.hostname, split_uri.port


def validate_topic_channel_name(name: str):
    """Validate topic/channel names.
    The regex is ``^[.a-zA-Z0-9_-]{2,64}+(#ephemeral)?$``

    :raises AssertionError: Value not matches regex.
    """
    assert re.match(r'^[.a-zA-Z0-9_\-]{2,64}(#ephemeral)?$', name), (
        'Topic name must matches ^[.a-zA-Z0-9_-]{2,64}+(#ephemeral)?$ regex')


@singledispatch
def convert_to_bytes(value) -> bytes:
    """Dispatch for convertible types.

    Allowed types: ``bytes``, ``bytearray``, ``str``, ``int``, ``float``,
        ``dict``, ``Decimal``, ``dataclass``.

    :raises TypeError:
    """
    if PY37:
        from dataclasses import asdict, is_dataclass
        if is_dataclass(value) and not isinstance(value, type):
            return convert_to_bytes(asdict(value))
    raise TypeError(
        'Argument {} expected to be type of '
        'bytes, bytearray, str, int, float, dict, Decimal, datetime '
        'or dataclass'.format(value)
    )


@convert_to_bytes.register(bytes)
@convert_to_bytes.register(bytearray)
def _(value: Union[bytes, bytearray]) -> bytes:
    """Convert ``bytes`` or ``bytearray`` to bytes"""
    return value


@convert_to_bytes.register(str)
def _(value: str) -> bytes:
    """Convert ``str`` to bytes"""
    return value.encode('utf-8')


@convert_to_bytes.register(int)
@convert_to_bytes.register(float)
@convert_to_bytes.register(Decimal)
def _(value: Union[int, float, Decimal]) -> bytes:
    """Convert ``int``, ``float`` or ``Decimal`` to bytes"""
    return str(value).encode('utf-8')


@convert_to_bytes.register(dict)
def _(value: dict) -> bytes:
    """Convert ``dict`` to bytes"""
    return json.dumps(
        value,
        cls=JSONEncoder,
        separators=(',', ':'),
    ).encode('utf-8')


@convert_to_bytes.register(Enum)
def _(value: Enum) -> bytes:
    """Convert ``enum`` to bytes"""
    return convert_to_bytes(value.name)


@convert_to_bytes.register(datetime)
def _(value: datetime) -> bytes:
    """Convert ``datetime`` to bytes"""
    return value.isoformat().encode('utf-8')


@singledispatch
def convert_to_str(value):
    """Dispatch for convertible types.

    Allowed types: ``bytes``, ``bytearray``, ``str``, ``int``, ``float``,
        ``dict``, ``Decimal``, ``dataclass``.

    :raises TypeError:
    """
    if PY37:
        from dataclasses import asdict, is_dataclass
        if is_dataclass(value) and not isinstance(value, type):
            return convert_to_str(asdict(value))
    raise TypeError(
        'Argument {} expected to be type of '
        'bytes, bytearray, str, int, float, dict, Decimal, datetime '
        'or dataclass'.format(value)
    )


@convert_to_str.register(str)
def _(value: str) -> str:
    """Convert ``str`` to ``str``"""
    return value


@convert_to_str.register(bytes)
def _(value: bytes) -> str:
    """Convert ``bytes`` to ``str``"""
    return value.decode('utf-8')


@convert_to_str.register(bytearray)
def _(value: bytearray) -> str:
    """Convert ``bytearray`` to ``str``"""
    return bytes(value).decode('utf-8')


@convert_to_str.register(int)
@convert_to_str.register(float)
@convert_to_str.register(Decimal)
def _(value: Union[int, float, Decimal]) -> str:
    """Convert ``int``, ``float`` or ``Decimal`` to ``str``"""
    return str(value)


@convert_to_str.register(dict)
def _(value: dict) -> str:
    """Convert ``dict`` to JSON string"""
    return json.dumps(value)


@convert_to_str.register(Enum)
def _(value: Enum) -> str:
    """Convert ``enum`` to str"""
    return convert_to_str(value.name)


@convert_to_str.register(datetime)
def _(value: datetime) -> str:
    """Convert ``datetime`` to bytes"""
    return value.isoformat()


def get_logger(debug: bool = False, unique_name: str = None):
    """Get the ansq logger.

    :params debug: Set up debug level.
    :type debug: :class:`bool`
    :params unique_name: Used to make all loggers unique.
    :type unique_name: :class:`str`
    """
    logger = logging.getLogger(
        'ansq {}'.format(unique_name) if unique_name else 'ansq')
    log_format = "%(asctime)s - %(levelname)s - %(name)s: %(message)s"
    logging.basicConfig(format=log_format)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    return logger
