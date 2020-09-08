"""NSQ protocol parser.

:see: https://nsq.io/clients/tcp_protocol_spec.html
"""
import abc
import struct
from typing import Any, Optional, Tuple, Union

from ansq.tcp import consts
from ansq.tcp.exceptions import ProtocolError
from ansq.tcp.types import (
    FrameType,
    NSQErrorSchema,
    NSQMessageSchema,
    NSQResponseSchema,
)
from ansq.utils import convert_to_bytes

__all__ = "Reader"


class BaseReader(metaclass=abc.ABCMeta):
    @abc.abstractmethod  # pragma: no cover
    def feed(self, chunk: bytes) -> None:
        pass

    @abc.abstractmethod  # pragma: no cover
    def get(self) -> Any:
        pass

    @abc.abstractmethod  # pragma: no cover
    def encode_command(self, cmd: str, *args: Any, data: Optional[Any] = None) -> bytes:
        pass


class Reader(BaseReader):
    def __init__(self, buffer: bytes = None):
        self._buffer = bytearray()
        self._is_header = False
        self._payload_size = 0
        if buffer:
            self.feed(buffer)

    @property
    def buffer(self) -> bytearray:
        return self._buffer

    def feed(self, chunk: bytes) -> None:
        """Put raw chunk of data obtained from connection to buffer.

        :param chunk: Raw input data.
        :type chunk: :class:`bytes`
        """
        if not chunk:
            return
        self._buffer.extend(chunk)

    def get(
        self,
    ) -> Optional[Union[NSQResponseSchema, NSQErrorSchema, NSQMessageSchema]]:
        """Get from buffer NSQ response

        :raises ProtocolError: On unexpected NSQ message's FrameType
        :returns: Depends of ``frame_type``, returns
            :class:`NSQResponse`, :class:`NSQError`,  or :class:`NSQMessage`
        """
        buffer_size = len(self._buffer)

        if not self._is_header and buffer_size >= consts.DATA_SIZE:
            size = struct.unpack(">l", self._buffer[: consts.DATA_SIZE])[0]
            self._payload_size = size
            self._is_header = True

        if self._is_header and buffer_size >= consts.DATA_SIZE + self._payload_size:
            start, end = consts.DATA_SIZE, consts.HEADER_SIZE
            frame_type = FrameType(struct.unpack(">l", self._buffer[start:end])[0])
            resp = self._parse_payload(frame_type, self._payload_size)

            self._buffer = self._buffer[start + self._payload_size :]
            self._is_header = False
            self._payload_size = 0

            return resp

        return None

    def _parse_payload(
        self, frame_type: FrameType, payload_size: int,
    ) -> Union[NSQResponseSchema, NSQErrorSchema, NSQMessageSchema]:
        """Parse from buffer NSQ response

        :raises ProtocolError: On unexpected NSQ message's FrameType
        :returns: Depends of ``frame_type``, returns
            :class:`NSQResponse`, :class:`NSQError`,  or :class:`NSQMessage`
        """
        if frame_type == FrameType.RESPONSE:
            return NSQResponseSchema(
                self._unpack_response(payload_size), frame_type=frame_type,
            )
        if frame_type == FrameType.ERROR:
            return NSQErrorSchema(
                *self._unpack_error(payload_size), frame_type=frame_type,
            )
        if frame_type == FrameType.MESSAGE:
            return NSQMessageSchema(
                *self._unpack_message(payload_size), frame_type=frame_type,
            )

        raise ProtocolError(f"Got unexpected FrameType: {frame_type}")

    def _unpack_response(self, payload_size: int) -> bytes:
        """Unpack the response from the buffer"""
        start = consts.HEADER_SIZE
        end = consts.DATA_SIZE + payload_size
        return bytes(self._buffer[start:end])

    def _unpack_error(self, payload_size: int) -> Tuple[bytes, bytes]:
        """Unpack the error from the buffer"""
        error = self._unpack_response(payload_size)
        code, msg = error.split(maxsplit=1)
        return code, msg

    def _unpack_message(self, payload_size: int) -> Tuple[int, int, bytes, bytes]:
        """Unpack the message from the buffer.

        :see: https://docs.python.org/3/library/struct.html
        :rtype: :class:`NSQMessageSchema`
        :returns: NSQ Message
        """
        start = consts.HEADER_SIZE
        end = consts.DATA_SIZE + payload_size
        msg_len = end - start - consts.MSG_HEADER
        fmt = f">qh16s{msg_len}s"

        timestamp, attempts, id_, body = struct.unpack(fmt, self._buffer[start:end])
        return timestamp, attempts, id_, body

    def encode_command(
        self, cmd: Union[str, bytes], *args: Any, data: Any = None,
    ) -> bytes:
        """Encode command to bytes"""
        _cmd = convert_to_bytes(cmd.upper().strip())
        _args = [convert_to_bytes(a) for a in args]
        body_data, params_data = b"", b""

        if len(_args):
            params_data = b" " + b" ".join(_args)

        if data and isinstance(data, (list, tuple)):
            data_encoded = [self._encode_body(part) for part in data]
            num_parts = len(data_encoded)
            payload = struct.pack(">l", num_parts) + b"".join(data_encoded)
            body_data = struct.pack(">l", len(payload)) + payload
        elif data:
            body_data = self._encode_body(data)

        return b"".join((_cmd, params_data, consts.NL, body_data))

    @staticmethod
    def _encode_body(data: Any) -> bytes:
        _data = convert_to_bytes(data)
        result = struct.pack(">l", len(_data)) + _data
        return result
