from typing import Union

from . import NSQCommands, FrameType


class NSQResponseSchema:
    """NSQ Response schema"""
    body: bytes
    frame_type: FrameType

    def __init__(self, body: bytes, frame_type: Union[FrameType, int] = None):
        self.body = body
        self.frame_type = (
            frame_type
            if isinstance(frame_type, FrameType) or frame_type is None
            else FrameType(frame_type)
        )

    def __repr__(self):
        return '<NSQResponseSchema frame_type:{}, body:{}, is_ok:{}>'.format(
            self.frame_type, self.body, self.is_ok)

    def __bool__(self):
        return True

    @property
    def is_ok(self) -> bool:
        return self.body == NSQCommands.OK

    @property
    def is_heartbeat(self) -> bool:
        return self.body == b'_heartbeat_'

    @property
    def is_message(self) -> bool:
        return self.frame_type.is_message

    @property
    def is_response(self) -> bool:
        return self.frame_type.is_response

    @property
    def is_error(self) -> bool:
        return self.frame_type.is_error


class NSQMessageSchema(NSQResponseSchema):
    """NSQ Message schema"""
    timestamp: int = None
    attempts: int = None
    id: str = None

    def __init__(
            self, timestamp: int, attempts: int, id_: bytes, body: bytes,
            frame_type: Union[FrameType, int]):
        super().__init__(body, frame_type)
        self.timestamp = timestamp
        self.attempts = attempts
        self.id = id_.decode('utf-8')

    def __repr__(self):
        return (
            '<NSQMessageSchema frame_type:{}, body:{}, timestamp:{}, '
            'attempts:{}, id:{}>'
        ).format(
            self.frame_type, self.body, self.timestamp, self.attempts,
            self.id
        )


class NSQErrorSchema(NSQResponseSchema):
    """NSQ Error"""
    code: str

    def __init__(
            self, code: bytes, body: bytes,
            frame_type: Union[FrameType, int]):
        super().__init__(body, frame_type)
        self.code = code.decode('utf-8')

    def __repr__(self):
        return '<NSQErrorSchema frame_type:{}, body:{}, code:{}>'.format(
            self.frame_type, self.body, self.code)

    def __bool__(self):
        return False
