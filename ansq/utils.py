import re
import logging
from urllib.parse import urlparse

TOPIC_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+$')
CHANNEL_NAME_RE = re.compile(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$')


def get_host_and_port(host):
    host_parsed = urlparse(host)
    if host_parsed.scheme == 'tcp':
        result = host_parsed.netloc
    elif host_parsed.scheme == '':
        result = host_parsed.path
    else:
        result = host
    result = result.split(':')
    if len(result) == 2:
        return result[0], result[-1]
    else:
        return result[0], None


def valid_topic_name(topic):
    if not 0 < len(topic) < 33:
        return False
    return bool(TOPIC_NAME_RE.match(topic))


def valid_channel_name(channel):
    if not 0 < len(channel) < 33:
        return False
    return bool(CHANNEL_NAME_RE.match(channel))


_converters_to_bytes_map = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
}


_converters_to_str_map = {
    str: lambda val: val,
    bytearray: lambda val: bytes(val).decode('utf-8'),
    bytes: lambda val: val.decode('utf-8'),
    int: lambda val: str(val),
    float: lambda val: str(val),
}


def _convert_to_bytes(value):
    if type(value) in _converters_to_bytes_map:
        converted_value = _converters_to_bytes_map[type(value)](value)
    else:
        raise TypeError("Argument {!r} expected to be of bytes,"
                        " str, int or float type".format(value))
    return converted_value


def _convert_to_str(value):
    if type(value) in _converters_to_str_map:
        converted_value = _converters_to_str_map[type(value)](value)
    else:
        raise TypeError("Argument {!r} expected to be of bytes,"
                        " str, int or float type".format(value))
    return converted_value


class MaxRetriesExcited(Exception):
    pass


def get_logger(debug: bool = False):
    logger = logging.getLogger('ansq')
    log_format = "%(asctime)s - %(levelname)s - %(name)s: %(message)s"
    logging.basicConfig(format=log_format)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    return logger
