import logging, sys
from enum import IntEnum, auto
import json
from socket import socket, MSG_DONTWAIT, MSG_PEEK
from typing import Any
class RelayMessageTypes(IntEnum):
    NEW_CONNECTION = auto()
    CLOSE_CONNECTION = auto()
    MESSAGE = auto()


def configure_logger(verbose: bool = False):
    """configure logger"""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=(
            "[*][%(asctime)s][%(levelname)s][%(threadName)s]: %(message)s"
        ),
        stream=sys.stderr,
    )
    pass

def valid_msg(json_msg: dict) -> bool:
    # TODO: validate this in future
    return True



def grab_json(buffer: bytes) -> tuple[Any, bytes]:
    """
    given buffer containing n valid json objects: [a, b, c, ...]
    decodes first and returns rest of buffer (a, [b, c, ...])
    """
    # first try
    try:
        result = json.loads(buffer)
        # buffer is otherwise empty if this succeeds
        return result, b''
    except json.decoder.JSONDecodeError as e:
        if e.msg == 'Extra data':
            try:
                result = json.loads(buffer[:e.pos])
                return result, buffer[e.pos:]
            except:
                raise Exception("failed, no data")
        else:
            raise Exception("General JSON fail")


GENERIC_JSON_ERROR = json.dumps({
    "error": "bad json payload"
}).encode('utf8')

BAD_TYPE_JSON_ERROR = json.dumps({
    "error": "unknown json message type"
}).encode('utf8')

BAD_EVENT_JSON_ERROR = json.dumps({
    "error": "unknown json event type"
}).encode('utf8')

MISSING_PORT_JSON_ERROR = json.dumps({
    "error": "json message missing \"port\" field"
}).encode('utf8')

MISSING_RELAY_JSON_ERROR = json.dumps({
    "error": "no managed relay matching requested port number"
}).encode('utf8')

MISSING_ADDRESS_FIELDS = json.dumps({
    "error": "missing address and/or port field in message"
}).encode('utf8')

MISSING_DATA_FIELD = json.dumps({
    "error": "missing data field for data type message"
}).encode('utf8')

DECODING_ERROR = json.dumps({
    "error": "failed to base64 decode data payload"
}).encode('utf8')

UNKNOWN_EVENT_JSON_ERROR = json.dumps({
    "error": "unknown event for given message type"
}).encode('utf8')

def is_socket_open(s:socket) -> bool:
    """
    checks if a socket is in an open state
    """
    if s.fileno() == -1:
        return False

    try:
        # MSG_PEEK doesn't consume bytes on the recv buffer
        data = s.recv(64, MSG_DONTWAIT | MSG_PEEK)
        if len(data) == 0:
            return False
    except BlockingIOError:
        pass # socket open, but no data to read
    except ConnectionResetError:
        return False # connection closed or reset by peer
    except Exception:
        return False # other issue
    
    return True

class RelayManageMessage():
    pass