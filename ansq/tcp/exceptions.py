from __future__ import annotations


class ConnectionClosedError(Exception):
    pass


class NSQException(Exception):
    """XXX"""


class ProtocolError(NSQException):
    """XXX"""


class NSQRequeueMessage(NSQException):
    pass


class NSQNoConnections(NSQException):
    pass


class NSQHttpError(NSQException):
    pass


class NSQFrameError(NSQException):
    pass


class NSQErrorCode(NSQException):
    fatal = True


class NSQInvalid(NSQErrorCode):
    """E_INVALID"""

    pass


class NSQBadBody(NSQErrorCode):
    """E_BAD_BODY"""

    pass


class NSQBadTopic(NSQErrorCode):
    """E_BAD_TOPIC"""

    pass


class NSQBadChannel(NSQErrorCode):
    """E_BAD_CHANNEL"""

    pass


class NSQBadMessage(NSQErrorCode):
    """E_BAD_MESSAGE"""

    pass


class NSQPutFailed(NSQErrorCode):
    """E_PUT_FAILED"""

    pass


class NSQPubFailed(NSQErrorCode):
    """E_PUB_FAILED"""


class NSQMPubFailed(NSQErrorCode):
    """E_MPUB_FAILED"""


class NSQAuthDisabled(NSQErrorCode):
    """E_AUTH_DISABLED"""


class NSQAuthFailed(NSQErrorCode):
    """E_AUTH_FAILED"""


class NSQUnauthorized(NSQErrorCode):
    """E_UNAUTHORIZED"""


class NSQFinishFailed(NSQErrorCode):
    """E_FIN_FAILED"""

    fatal = False


class NSQRequeueFailed(NSQErrorCode):
    """E_REQ_FAILED"""

    fatal = False


class NSQTouchFailed(NSQErrorCode):
    """E_TOUCH_FAILED"""

    fatal = False


ERROR_CODES = {
    "E_INVALID": NSQInvalid,
    "E_BAD_BODY": NSQBadBody,
    "E_BAD_TOPIC": NSQBadTopic,
    "E_BAD_CHANNEL": NSQBadChannel,
    "E_BAD_MESSAGE": NSQBadMessage,
    "E_PUT_FAILED": NSQPutFailed,
    "E_PUB_FAILED": NSQPubFailed,
    "E_MPUB_FAILED": NSQMPubFailed,
    "E_FINISH_FAILED": NSQFinishFailed,
    "E_AUTH_DISABLED": NSQAuthDisabled,
    "E_AUTH_FAILED": NSQAuthFailed,
    "E_UNAUTHORIZED": NSQUnauthorized,
    "E_FIN_FAILED": NSQFinishFailed,
    "E_REQUEUE_FAILED": NSQRequeueFailed,
    "E_REQ_FAILED": NSQRequeueFailed,
    "E_TOUCH_FAILED": NSQTouchFailed,
}

# https://groups.google.com/forum/#!msg/nsq-users/VSxdPtw2ZmY/kmQJJhZe4wEJ
# E_TOUCH_FAILED, fatal
# E_REQ_FAILED fatal
# E_FIN_FAILED


def get_exception(code: str, error_message: str | bytes) -> NSQException:
    return ERROR_CODES.get(code, NSQErrorCode)(f"{code}: {error_message!r}")
