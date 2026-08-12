from enum import IntEnum, auto

class RelayMessageTypes(IntEnum):
    NEW_CONNECTION = auto()
    CLOSE_CONNECTION = auto()
    MESSAGE = auto()