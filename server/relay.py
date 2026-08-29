import socket
from queue import Queue
from threading import Event
from .common import RelayMessageTypes, QueuedRelayMessage
from os import sched_yield
import logging
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from .metrics import ACTIVE_RELAYS, BYTES_TRANSFERRED, ACTIVE_CONNECTIONS
from collections.abc import Callable

RELAY_SERVER_LOGGER_NAME = "portport-server"


class RelayConnection():
    def __init__(self, sck: socket.socket):
        self.sck = sck
        self.write_enabled = False
        self.sendq = bytes()

class Relay():
    """open relay connection"""
    def __init__(self, close: Event, inbound: Queue[QueuedRelayMessage], outbound: Queue[QueuedRelayMessage], wakeup_mgmt: Callable[[], None] , port: int = 0, backlog: int = 5, addr: str = '0.0.0.0', sock_kind: socket.SocketKind = socket.SocketKind.SOCK_STREAM):
        #self.connection_table: dict[tuple[str,int], socket.socket] = {}
        #self.connection_queues: dict[tuple[str,int], bytes] = {}

        # should wake up relay mgmt select()
        self.wakeup_callback = wakeup_mgmt

        self.connection_table: dict[tuple[str,int], RelayConnection] = {}

        self.logger = logging.getLogger(RELAY_SERVER_LOGGER_NAME)
        self.id = 0
        self.backlog = backlog
        self.port = port
        self.addr = addr
        self.outbound = outbound
        self.inbound = inbound
        self.close_req = close;
        self._sck = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
        self._sck.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sck.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # 30 second timeout
        self._sck.settimeout(30.0)

        # for closing *just* this socket
        self.atomic_close = Event()
        self.atomic_close.clear()

        # selector for multiplexing sockets
        self.selector = DefaultSelector()

        # 
        # wake up socket pair
        # 
        # to short-circuit the .select() once we have more data to forward
        self._wakeup_read, self._wakeup_write = socket.socketpair()
        self._wakeup_read.setblocking(False)
        self._wakeup_write.setblocking(False)

        self.selector.register(
            self._wakeup_read,
            EVENT_READ,
            self._handle_wakeup)

        # addr should be 
        self._sck.bind((self.addr, self.port))
        self.identifying_port = self.get_port()
        self.logger.info(f"new relay initialized at port {self.identifying_port}")
        ACTIVE_RELAYS.inc()

    def _handle_wakeup(self, socket: socket.socket, mask: int):
        if mask & EVENT_READ:
            # drain completly
            while 1:
                try:
                    d = socket.recv(4096)
                    if len(d) == 0:
                        # EOF = closing this relay
                        self.atomic_close.set()
                        return
                except:
                    break

    def enqueue_outbound(self, msg: QueuedRelayMessage):
        self.outbound.put(msg)
        self.wakeup()

    def enqueue_inbound(self, msg: QueuedRelayMessage):
        self.inbound.put(msg)
        self.wakeup_callback()

    def wakeup(self):
        """wakes up selector()"""
        try:
            self._wakeup_write.send(b"\x00")
        except BlockingIOError:
            # should only happen if buffer is full
            pass
        except:
            # we must be shutting down--OR something seriously broke, let's propagate the error
            if not self.atomic_close.is_set():
                raise
        return
    def accept_inbound_connection(self, sock: socket.socket, mask: int):
        """accept new connection"""
        conn, addr = sock.accept()
        conn.setblocking(False)
        self.connection_table[addr] = RelayConnection(conn)
        self.selector.register(conn, EVENT_READ, self.handle_socket)
        self.enqueue_inbound((RelayMessageTypes.NEW_CONNECTION, addr[0], addr[1], b'', self.identifying_port))
        
        ACTIVE_CONNECTIONS.inc()

    def clean_up_connection(self, addr: tuple[str, int]):
        con_obj = self.connection_table[addr]
        self.enqueue_inbound((RelayMessageTypes.CLOSE_CONNECTION, addr[0], addr[1], b'', self.identifying_port))
        del self.connection_table[addr]
        self.selector.unregister(con_obj.sck)
        # ensure we closed socket
        con_obj.sck.close()
        ACTIVE_CONNECTIONS.dec()

    def handle_socket(self, sock: socket.socket, mask: int):
        """i/o on external socket connection to our relay"""
        # retrieve send queue
        addr = sock.getpeername()
        con_obj = self.connection_table[addr]
        
        if mask & EVENT_WRITE and len(con_obj.sendq) > 0:
            try:
                sent_length = sock.send(con_obj.sendq)
                con_obj.sendq = con_obj.sendq[sent_length:]
                BYTES_TRANSFERRED.labels(direction="relay_to_external_host").inc(sent_length)
                # unregister EVENT_WRITE if we have sent all pending bytes
                if len(con_obj.sendq) == 0:
                    con_obj.write_enabled = False
                    self.selector.modify(con_obj.sck, EVENT_READ, self.handle_socket)
            except (BrokenPipeError, ConnectionResetError):
                # blocking IO shouldn't happen afaik
                self.clean_up_connection(addr)
                return # don't continue to EVENT_READ handling
            except BlockingIOError:
                # this normally shouldn't happen but if it does let's keep chugging along
                pass
        if mask & EVENT_READ:
            # received data gets funneled to relay client (handled by relay mgmt)
            try:
                data = sock.recv(4096)
                if len(data) == 0:
                    self.clean_up_connection(addr)
                else:
                    BYTES_TRANSFERRED.labels(direction="external_host_to_relay").inc(len(data))
                    self.enqueue_inbound((RelayMessageTypes.MESSAGE, addr[0], addr[1], data, self.identifying_port))
            except (ConnectionResetError, BrokenPipeError):
                self.clean_up_connection(addr)


    def get_port(self) -> int:
        # if the socket is closed this exceptions out
        return self._sck.getsockname()[1]

    def open(self):
        # backlog default of five, should make this configurable
        self._sck.listen(self.backlog)
        self._sck.setblocking(False)
        assert self._sck.getblocking() == False
        self.selector.register(self._sck, EVENT_READ, self.accept_inbound_connection)
        self.logger.info(f"relay listening on port={self.identifying_port} ")

        while (not self.close_req.is_set() and (not self.atomic_close.is_set())):

            events = self.selector.select(timeout=5)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
            
            # update based on relay mgmt inbound data
            try:
                # need timeout or select here
                while not (self.outbound.qsize() == 0) and not self.close_req.is_set() and (not self.atomic_close.is_set()):
                    try:
                        msg, address, port, data, _id_port = self.outbound.get_nowait()
                        con = self.connection_table[(address, port)]
                        if msg == RelayMessageTypes.MESSAGE:
                             con.sendq += data
                             if not con.write_enabled:
                                 con.write_enabled = True
                                 self.selector.modify(con.sck, EVENT_READ|EVENT_WRITE, self.handle_socket)
                             
                        elif msg == RelayMessageTypes.CLOSE_CONNECTION:
                            self.clean_up_connection((address, port))
                        else:
                            # SHOULD RAISE ERROR
                            # we cannot have any other relay message types
                            raise Exception("Processing error: invalid relay message type!")
                            
                    except BlockingIOError:
                        break
            except KeyError:
                self.logger.warning("unknown connection referenced, ignoring...")
            except Exception as e:
                self.logger.error("UNKNOWN EXCEPTION HIT", extra={"exception": e})
                self.atomic_close.set()


            sched_yield()

        self.logger.info(f"closing relay port={self.identifying_port}, cleaning up existing connections")
        # clean up pending connections
        for addr in list(self.connection_table.keys()):
            self.clean_up_connection(addr)
        self._sck.close()
        self.selector.close()
        ACTIVE_RELAYS.dec()
        self.logger.info(f"cleaned up connections for relay port={self.identifying_port}.  Relay successfully closed!")
        

