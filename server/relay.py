import socket
from queue import Queue, Empty
from threading import Event, Timer
from time import sleep
from ..common.common import RelayMessageTypes


        

# outbound : foreign <- relay <- dedicated client
# inbound : foreign -> relay -> dedicated client

class Relay():
    """open relay connection"""
    def __init__(self, close: Event, port: int, inbound: Queue[tuple[RelayMessageTypes, str, int, bytes]], outbound: Queue[tuple[RelayMessageTypes, str, int, bytes]], backlog: int = 5, addr: str = '0.0.0.0', sock_kind: socket.SocketKind = socket.SocketKind.SOCK_STREAM):
        self.connection_table: dict[tuple[str,int], socket.socket] = {}
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
        self._sck.setblocking(False)
        # 30 second timeout
        self._sck.settimeout(30.0)

        #(sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1))

    def open(self):
        # addr should be 
        self._sck.bind((self.addr, self.port))
        # backlog default of five, should make this configurable
        self._sck.listen(self.backlog)


        while not self.close_req.is_set():
            # accept any new connection (foreign host)
            try:
                # connection
                con, addr = self._sck.accept()
                # ensure that con should be non-blocking
                con.setblocking(False)
                self.connection_table[addr] = con
            except BlockingIOError:
                pass

            # READ from foreign hosts to our dedicated host
            for addr, con in self.connection_table.items():
                # check if connection has closed
                socket_closed = False
                try:
                    # MSG_PEEK doesn't consume bytes on the recv buffer
                    data = con.recv(64, socket.MSG_DONTWAIT | socket.MSG_PEEK)
                    if len(data) == 0:
                        socket_closed = True
                except BlockingIOError:
                    pass  # Socket is open, it just has no data to read right now
                except ConnectionResetError:
                    socket_closed = True   # Connection was abruptly closed or reset by peer
                except Exception:
                    socket_closed = True   # Handle other socket errors as closed

                # was this closed?
                if con.fileno() == -1 or socket_closed:
                    # instruct remote client to close connection (is this the correct semantic?)
                    del self.connection_table[addr]
                    self.inbound.put((RelayMessageTypes.CLOSE_CONNECTION, addr[0], addr[1], b''), True)
                    continue

                # do we have any data to forward to the dedicated cilent?
                try:
                    # TODO: probably tune this
                    data = con.recv(4096)
                    self.inbound.put((RelayMessageTypes.MESSAGE, addr[0], addr[1], data), True)
                except BlockingIOError:
                    pass
            # WRITE to foreign hosts
            try:
                # TODO: maybe refactor? Unsure
                write_exceed = Event()
                
                write_exceed.clear()
                def set_write_exceed():
                    write_exceed.set()

                t = Timer(1.0, set_write_exceed)
                t.start()
                while (not write_exceed.is_set()) and (not self.outbound.empty()):
                    msg, address, port, data = self.outbound.get(True)
                    con = self.connection_table[(address, port)]
                    if msg == RelayMessageTypes.MESSAGE:
                        # what if (addr, port) don't exist in the table?
                        # what if exception?
                        con.sendall(data)
                    if msg == RelayMessageTypes.CLOSE_CONNECTION:
                        con.close()
                    else:
                        # SHOULD RAISE ERROR
                        # we cannot have
                        pass
                # cancel timer if not yet completed
                t.cancel()

            except:
                pass
                
            
        # clean up pending connections

                
        self._sck.close()
        

    def close(self):
        pass
