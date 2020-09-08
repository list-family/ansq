from typing import TYPE_CHECKING, Dict, Optional, Union

if TYPE_CHECKING:
    from ansq.tcp.types import NSQErrorSchema, NSQMessageSchema, NSQResponseSchema


HTTPResponse = Union[Dict, str]
TCPResponse = Optional[Union["NSQResponseSchema", "NSQErrorSchema", "NSQMessageSchema"]]
