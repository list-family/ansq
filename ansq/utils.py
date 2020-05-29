import re
import logging
from decimal import Decimal
from functools import singledispatch
from typing import Union, Tuple, Optional
from urllib.parse import urlsplit


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
def convert_to_bytes(value):
    """Base dispatch for unregistered convertible types

    :raises TypeError:
    """
    raise TypeError(
        'Argument {} expected to be type of '
        'bytes, str, int or float'.format(value))


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


@singledispatch
def convert_to_str(value):
    """Base dispatch for unregistered convertible types

    :raises TypeError:
    """
    raise TypeError(
        'Argument {} expected to be type of '
        'bytes, str, int or float'.format(value))


@convert_to_str.register(str)
def _(value: str) -> str:
    """Convert ``str`` to str"""
    return value


@convert_to_str.register(bytes)
def _(value: bytes) -> str:
    """Convert ``bytes`` to str"""
    return value.decode('utf-8')


@convert_to_str.register(bytearray)
def _(value: bytearray) -> str:
    """Convert ``bytearray`` to str"""
    return bytes(value).decode('utf-8')


@convert_to_str.register(int)
@convert_to_str.register(float)
def _(value: Union[int, float]) -> str:
    """Convert ``int`` or ``float`` to str"""
    return str(value)


def get_logger(debug: bool = False):
    """Get the ansq logger.

    :params debug: Set up debug level.
    :type debug: :class:`bool`
    """
    logger = logging.getLogger('ansq')
    log_format = "%(asctime)s - %(levelname)s - %(name)s: %(message)s"
    logging.basicConfig(format=log_format)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    return logger
