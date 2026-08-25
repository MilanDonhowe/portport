import logging, sys, struct
from enum import IntEnum, auto
import json
from socket import socket, MSG_DONTWAIT, MSG_PEEK, inet_pton, AddressFamily, inet_ntop
from typing import Any, Self
from ipaddress import ip_address, IPv4Address, IPv6Address

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

# message type between PortPort client and server
class PortPortMessageType(IntEnum):
    CREATE_RELAY = auto()
    DESTROY_RELAY = auto()
    NEW_CONNECTION = auto()
    CLOSE_CONNECTION = auto()
    DATA = auto()
    OPEN_RELAY = auto()
    ERROR = auto()
    
class PortPortErrorTypes(IntEnum):
    NO_ERROR = auto()
    RELAY_DOES_NOT_EXIST = auto()

# message between PortPort client and server
class PortPortMessage():
    def __init__(
            self, 
            msg_type: PortPortMessageType, 
            conn_addr: str = '0.0.0.0', 
            conn_port: int = -1, 
            relay_port: int = -1,  
            data: bytes = b'', 
            version: int = 1, 
            err: PortPortErrorTypes = PortPortErrorTypes.NO_ERROR
        ):
        self.msg_type=msg_type
        self.data=data
        self.data_len = len(self.data)
        self.version=version
        self.addr = ip_address(conn_addr)
        self.port = conn_port
        self.relay_port = relay_port
        self.error = err

    def serialize(self):
        # header:
        # 1 byte meaningless SOT byte | 1 byte relay message type | 1 byte version field
        msg_header = b'@' + struct.pack(">B", self.msg_type) + struct.pack(">B", self.version)
        if self.msg_type == PortPortMessageType.ERROR:
            return msg_header + struct.pack(">B", self.error)
        elif self.msg_type == PortPortMessageType.DATA:
            # ok, so serializing this will change if the address if IPv4 or IPv6
            if isinstance(self.addr, IPv6Address):
                msg_header = msg_header + struct.pack(">B", 6) + inet_pton(AddressFamily.AF_INET6, str(self.addr)) + struct.pack(">H", self.port) + struct.pack(">H", self.relay_port)
            else:
                msg_header = msg_header + struct.pack(">B", 4) + inet_pton(AddressFamily.AF_INET, str(self.addr)) +  struct.pack(">H", self.port) + struct.pack(">H", self.relay_port)
            return msg_header + struct.pack(">I", len(self.data)) + self.data
        elif self.msg_type == PortPortMessageType.CREATE_RELAY:
            return msg_header
        elif self.msg_type == PortPortMessageType.OPEN_RELAY:
            return msg_header + struct.pack(">H", self.relay_port)
        elif self.msg_type == PortPortMessageType.DESTROY_RELAY:
            # currently unused
            return msg_header + struct.pack(">H", self.relay_port)
        # close and new connection are the same fields
        elif self.msg_type == PortPortMessageType.NEW_CONNECTION or self.msg_type == PortPortMessageType.CLOSE_CONNECTION:
            # ok, so serializing this will change if the address if IPv4 or IPv6
            if isinstance(self.addr, IPv6Address):
                return msg_header + struct.pack(">B", 6) + inet_pton(AddressFamily.AF_INET6, str(self.addr)) + struct.pack(">H", self.port) + struct.pack(">H", self.relay_port)
            else:
                return msg_header + struct.pack(">B", 4) + inet_pton(AddressFamily.AF_INET, str(self.addr)) +  struct.pack(">H", self.port) + struct.pack(">H", self.relay_port)
        
        else:
            raise Exception(f"Unknown Message Type for Relay Management communication: {self.msg_type}")

    @classmethod
    def deserialize(cls, data: bytes) -> tuple[Self, bytes]:
        if len(data) == 0:
            raise Exception("empty stream")
        if data[0] != ord(b'@'):
            raise Exception("invalid stream")
        msg_type = PortPortMessageType(data[1])
        msg_version = data[2]

        if msg_type == PortPortMessageType.ERROR:
            msg_error = PortPortErrorTypes(data[3])
            return cls(msg_type=msg_type, version=msg_version, err=msg_error), data[4:]
        elif msg_type == PortPortMessageType.CREATE_RELAY:
            return cls(msg_type=msg_type, version=msg_version), data[3:]
        elif msg_type == PortPortMessageType.DESTROY_RELAY:
            relay_port = struct.unpack(">H", data[3:5])[0]
            return cls(msg_type=msg_type, version=msg_version, relay_port=relay_port ), data[5:]
        elif msg_type == PortPortMessageType.OPEN_RELAY:
            relay_port = struct.unpack(">H", data[3:5])[0]
            return cls(msg_type=msg_type, version=msg_version, relay_port=relay_port ), data[5:]
        else:
            address_type = data[3]
            address = ''
            offset = 4
            if address_type == 6:
                # IPv6 decode (16 bytes)
                address = inet_ntop(AddressFamily.AF_INET6, data[offset:offset+16])
                offset += 16
            elif address_type == 4:
                # IPv4 decode
                address = inet_ntop(AddressFamily.AF_INET, data[offset:offset+4])
                offset += 4
            else:
                raise Exception(f"Unknown address type: {address_type}")

            port = struct.unpack(">H", data[offset:offset+2])[0]
            offset += 2

            relay_port = struct.unpack(">H", data[offset:offset+2])[0]
            offset += 2
            if msg_type == PortPortMessageType.NEW_CONNECTION or msg_type == PortPortMessageType.CLOSE_CONNECTION:
                return cls(msg_type=msg_type, version=msg_version, relay_port=relay_port, conn_addr=address, conn_port=port), data[offset:]
            elif msg_type == PortPortMessageType.DATA:
                msg_len = struct.unpack(">I", data[offset:offset+4])[0]
                offset += 4
                msg_data = data[offset:offset+msg_len]
                if len(msg_data) != msg_len:
                    raise Exception("Incomplete buffer!")
                offset += msg_len
                return cls(msg_type=msg_type, version=msg_version, relay_port=relay_port, conn_addr=address, conn_port=port, data=msg_data), data[offset:]
            else:
                raise Exception(f"Invalid message type: {msg_type}")
        # this code path should not execute
        return Exception("dead code path executed?")


def grab_msg(buffer: bytes) -> tuple[PortPortMessage, bytes]:
    """
    given buffer containing n valid portport messages [a,b,c, ...]
    decodes first and returns rest of buffer (a, [b,c, ...])
    """
    msg, stream = PortPortMessage.deserialize(buffer)
    return msg, stream

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