from enum import Enum


class FrameType(Enum):
    RESPONSE = 0
    ERROR = 1
    MESSAGE = 2

    @property
    def is_response(self) -> bool:
        return self == FrameType.RESPONSE

    @property
    def is_error(self) -> bool:
        return self == FrameType.ERROR

    @property
    def is_message(self) -> bool:
        return self == FrameType.MESSAGE
