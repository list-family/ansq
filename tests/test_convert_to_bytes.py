from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

import pytest

from ansq.utils import convert_to_bytes


class Color(Enum):
    RED = 1
    GREEN = "GREEN"
    BLUE = {"real_blue_color": True}


@pytest.mark.parametrize(
    "value, expected",
    (
        ("test_str", b"test_str"),
        (123, b"123"),
        (Decimal("3.14"), b"3.14"),
        (3.14159, b"3.14159"),
        (b"\xa1", b"\xa1"),
        ({"key": "value", 123: 1337}, b'{"key":"value","123":1337}'),
        (
            datetime(year=2020, month=1, day=1, hour=10, minute=34, second=2),
            b"2020-01-01T10:34:02",
        ),
        (
            {
                "int": 123,
                "float": 123.123,
                "str": "str",
                "list": [1, 2, 3],
                "dict": {"a": 1, "b": 2},
                "color": Color.GREEN,
                "datetime": datetime(year=2020, month=12, day=31),
            },
            b'{"int":123,"float":123.123,"str":"str","list":[1,2,3],'
            b'"dict":{"a":1,"b":2},"color":"GREEN",'
            b'"datetime":"2020-12-31T00:00:00"}',
        ),
        (
            "utf-16 str".encode("utf-16"),
            b"\xff\xfeu\x00t\x00f\x00-\x001\x006\x00 \x00s\x00t\x00r\x00",
        ),
        (bytearray(b"This is real bytearray"), b"This is real bytearray"),
        (
            bytearray("hello".encode("utf-32")),
            b"\xff\xfe\x00\x00h\x00\x00\x00e\x00\x00\x00l\x00"
            b"\x00\x00l\x00\x00\x00o\x00\x00\x00",
        ),
        (Color.RED, b"RED"),
        (Color.GREEN, b"GREEN"),
        (Color.BLUE, b"BLUE"),
    ),
)
def test_convert_to_bytes(value, expected):
    assert convert_to_bytes(value) == expected


@dataclass
class Point:
    x: int
    y: int
    color: Color = Color.BLUE
    name: Optional[str] = None


@dataclass
class DataclassWithDictPayload:
    name: str
    payload: dict


@pytest.mark.parametrize(
    "value, expected",
    (
        (Point(10, 20), b'{"x":10,"y":20,"color":"BLUE","name":null}'),
        (
            Point(10, 20, Color.RED, "A point"),
            b'{"x":10,"y":20,"color":"RED","name":"A point"}',
        ),
        (
            DataclassWithDictPayload(
                "Some str here",
                {
                    "int": 123,
                    "float": 123.123,
                    "str": "str",
                    "list": [1, 2, 3],
                    "dict": {"a": 1, "b": 2},
                    "color": Color.GREEN,
                    "dataclass": Point(10, 20),
                },
            ),
            b'{"name":"Some str here","payload":{"int":123,'
            b'"float":123.123,"str":"str","list":[1,2,3],'
            b'"dict":{"a":1,"b":2},"color":"GREEN","dataclass":'
            b'{"x":10,"y":20,"color":"BLUE","name":null}}}',
        ),
    ),
)
def test_convert_dataclass_to_bytes(value, expected):
    assert convert_to_bytes(value) == expected


@pytest.mark.parametrize("value", (None, [1, 2, 3], (1, 2), ["str_in_list"]))
def test_convert_to_bytes_with_exception(value):
    with pytest.raises(TypeError):
        convert_to_bytes(value)
