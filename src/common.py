import logging, sys, struct
from enum import IntEnum, auto
import json
from socket import socket, MSG_DONTWAIT, MSG_PEEK, inet_pton, AddressFamily, inet_ntop, socketpair
from typing import Any, Self
from threading import Event
from ipaddress import ip_address, IPv6Address
from selectors import EVENT_READ
from collections.abc import Callable
from queue import Queue, Empty

class RelayMessageTypes(IntEnum):
    NEW_CONNECTION = auto()
    CLOSE_CONNECTION = auto()
    MESSAGE = auto()

type QueuedRelayMessage = tuple[RelayMessageTypes, str, int, bytes, int]


class WakingRelayQueue():
    def __init__(self, wakeup: Callable[[], None]):
        self.Q: Queue[QueuedRelayMessage] = Queue()
        self.wakeup = wakeup

    def put(self, msg: QueuedRelayMessage):
        self.Q.put(msg)
        self.wakeup()

    def get_nowait(self) -> None | QueuedRelayMessage:
        try:
            msg = self.Q.get_nowait()
            return msg
        except Empty:
            return

    

        

def wakeup_pair(closing_event: Event) -> tuple[socket, Callable[[socket, int], None], Callable[[], None]]:
    """
    returns non-blocking socket pair with selector callback and wakeup function callbacks
    """
    recv_sock, send_sock = socketpair()
    recv_sock.setblocking(False)
    send_sock.setblocking(False)

    def handle_wakeup(s: socket, mask: int):
        if mask & EVENT_READ:
            while 1:
                try:
                    d=s.recv(4096)
                    if len(d)==0:
                        # this internal socket only closes on system failures
                        closing_event.set()
                        break # EOF
                except: # should be like BlockingIOError when socket is empty
                    break

    def wakeup_socket():
        try:
            send_sock.send(b'\xFF')
        except BlockingIOError:
            pass

    return recv_sock, handle_wakeup, wakeup_socket

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

### Exception types for PortPort comms
class IncompleteFrame(Exception):
    """Raised when PortPortMessage frame is incomplete"""
    pass

class PortPortBadVersion(Exception):
    """Raised on protocol type mismatch"""
    pass

class PortPortUnsupportedMsgType(Exception):
    """Raised when decoding an entirely unknown message type"""
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
    AUTH = auto()
    AUTH_SUCCESS = auto()
    
class PortPortErrorTypes(IntEnum):
    NO_ERROR = auto()
    RELAY_DOES_NOT_EXIST = auto()
    AUTH_FAILURE = auto()
    RELAY_FAILURE_PORT_EXHAUSTION = auto()
    INVALID_MSG_VERSION = auto()
    UNKNOWN_MSG_TYPE = auto()

# message between PortPort client and server
class PortPortMessage():

    VERSION = 1
    # ethernet's max transmit unit is around 1400 bytes
    MAX_DATA_LENGTH = 2000

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
        self.auth = ""

    def serialize(self):
        # header:
        # 1 byte meaningless SOT byte | 1 byte relay message type | 1 byte version field
        msg_header = b'@' + struct.pack(">B", self.msg_type) + struct.pack(">B", self.version)
        if self.msg_type == PortPortMessageType.ERROR:
            return msg_header + struct.pack(">B", self.error)
        if self.msg_type == PortPortMessageType.AUTH:
            return msg_header + struct.pack(">H", len(self.auth)) + self.auth.encode('utf8')
        elif self.msg_type == PortPortMessageType.DATA:
            # ok, so serializing this will change if the address if IPv4 or IPv6
            if isinstance(self.addr, IPv6Address):
                msg_header = msg_header + struct.pack(">B", 6) + inet_pton(AddressFamily.AF_INET6, str(self.addr)) + struct.pack(">H", self.port) + struct.pack(">H", self.relay_port)
            else:
                msg_header = msg_header + struct.pack(">B", 4) + inet_pton(AddressFamily.AF_INET, str(self.addr)) +  struct.pack(">H", self.port) + struct.pack(">H", self.relay_port)
            return msg_header + struct.pack(">I", len(self.data)) + self.data
        elif self.msg_type == PortPortMessageType.CREATE_RELAY or self.msg_type == PortPortMessageType.AUTH_SUCCESS:
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

        if msg_version != PortPortMessage.VERSION:
            raise PortPortBadVersion(f"unsupported PORT PORT version = {msg_version}, server running {PortPortMessage.VERSION}")

        if msg_type == PortPortMessageType.ERROR:
            msg_error = PortPortErrorTypes(data[3])
            return cls(msg_type=msg_type, version=msg_version, err=msg_error), data[4:]
        elif msg_type == PortPortMessageType.AUTH:
            token_length = struct.unpack(">H", data[3:5])[0]
            token = data[5:5+token_length]
            auth_msg = cls(msg_type=msg_type, version=msg_version)
            auth_msg.auth = token.decode('utf8')
            return auth_msg, data[5+token_length:]
        elif msg_type == PortPortMessageType.CREATE_RELAY or msg_type == PortPortMessageType.AUTH_SUCCESS:
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
                    raise IncompleteFrame("pending additional DATA bytes!")
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