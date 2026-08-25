import socket
from queue import Queue, Empty
from threading import Event, Timer
from time import sleep
from .common import RelayMessageTypes, is_socket_open
from os import sched_yield
import logging
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from .metrics import ACTIVE_RELAYS, BYTES_TRANSFERRED, ACTIVE_CONNECTIONS

RELAY_SERVER_LOGGER_NAME = "portport-server"

class Relay():
    """open relay connection"""
    def __init__(self, close: Event, inbound: Queue[tuple[RelayMessageTypes, str, int, bytes, int]], outbound: Queue[tuple[RelayMessageTypes, str, int, bytes, int]], port: int = 0, backlog: int = 5, addr: str = '0.0.0.0', sock_kind: socket.SocketKind = socket.SocketKind.SOCK_STREAM):
        self.connection_table: dict[tuple[str,int], socket.socket] = {}
        self.connection_queues: dict[tuple[str,int], bytes] = {}

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

        # addr should be 
        self._sck.bind((self.addr, self.port))
        self.identifying_port = self.get_port()
        self.logger.info(f"new relay initialized at port {self.identifying_port}")
        ACTIVE_RELAYS.inc()

    def accept_inbound_connection(self, sock: socket.socket, mask: int):
        """accept new connection"""
        conn, addr = sock.accept()
        conn.setblocking(False)
        self.connection_table[addr] = conn
        self.connection_queues[addr] = b''
        self.selector.register(conn, EVENT_READ | EVENT_WRITE, self.handle_socket)
        self.inbound.put((RelayMessageTypes.NEW_CONNECTION, addr[0], addr[1], b'', self.identifying_port))
        ACTIVE_CONNECTIONS.inc()

    def clean_up_connection(self, sock: socket.socket, addr: tuple[str, int]):
        self.inbound.put((RelayMessageTypes.CLOSE_CONNECTION, addr[0], addr[1], b'', self.identifying_port))
        del self.connection_queues[addr]
        del self.connection_table[addr]
        self.selector.unregister(sock)
        # ensure we closed socket
        sock.close()
        ACTIVE_CONNECTIONS.dec()

    def handle_socket(self, sock: socket.socket, mask: int):
        """i/o on external socket connection to our relay"""
        # retrieve send queue
        addr = sock.getpeername()
        sendq = self.connection_queues[addr]
        if mask & EVENT_WRITE and len(sendq) > 0:
            try:
                sent_length = sock.send(sendq)
                self.connection_queues[addr] = sendq[sent_length:]
                BYTES_TRANSFERRED.labels(direction="relay_to_external_host").inc(sent_length)
            except (BrokenPipeError, ConnectionResetError):
                # blocking IO shouldn't happen afaik
                self.clean_up_connection(sock, addr)
                return # don't continue to EVENT_READ handling
            except BlockingIOError:
                # this normally shouldn't happen but if it does let's keep chugging along
                pass
        if mask & EVENT_READ:
            # received data gets funneled to relay client (handled by relay mgmt)
            try:
                data = sock.recv(4096)
                if len(data) == 0:
                    self.clean_up_connection(sock, addr)
                else:
                    BYTES_TRANSFERRED.labels(direction="external_host_to_relay").inc(len(data))
                    self.inbound.put((RelayMessageTypes.MESSAGE, addr[0], addr[1], data, self.identifying_port))
            except (ConnectionResetError, BrokenPipeError):
                self.clean_up_connection(sock, addr)


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
                             self.connection_queues[(address, port)] += data
                        elif msg == RelayMessageTypes.CLOSE_CONNECTION:
                            self.clean_up_connection(con, (address, port))
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
        copy = list(self.connection_table.items())
        for addr, sock in copy:
            self.clean_up_connection(sock, addr)
        self._sck.close()
        self.selector.close()
        ACTIVE_RELAYS.dec()
        self.logger.info(f"cleaned up connections for relay port={self.identifying_port}.  Relay successfully closed!")
        

