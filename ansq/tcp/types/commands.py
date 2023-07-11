from __future__ import annotations


class NSQCommands:
    MAGIC_V2 = b"  V2"
    OK = b"OK"
    BIN_OK = b"\x00\x00\x00\x06\x00\x00\x00\x00OK"
    IDENTIFY = b"IDENTIFY"
    NOP = b"NOP"
    FIN = b"FIN"
    REQ = b"REQ"
    TOUCH = b"TOUCH"
    RDY = b"RDY"
    MPUB = b"MPUB"
    CLS = b"CLS"
    AUTH = b"AUTH"
    SUB = b"SUB"
    PUB = b"PUB"
    DPUB = b"DPUB"
