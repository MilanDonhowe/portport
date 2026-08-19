import socket
from queue import Queue, Empty
from threading import Event, Timer
from time import sleep
from .common import RelayMessageTypes, is_socket_open
from os import sched_yield

        

# outbound : foreign <- relay <- dedicated client
# inbound : foreign -> relay -> dedicated client

class Relay():
    """open relay connection"""
    def __init__(self, close: Event, inbound: Queue[tuple[RelayMessageTypes, str, int, bytes, int]], outbound: Queue[tuple[RelayMessageTypes, str, int, bytes, int]], port: int = 0, backlog: int = 5, addr: str = '0.0.0.0', sock_kind: socket.SocketKind = socket.SocketKind.SOCK_STREAM):
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
        # 30 second timeout
        self._sck.settimeout(30.0)

        # for closing *just* this socket
        self.atomic_close = Event()
        self.atomic_close.clear()

        # addr should be 
        self._sck.bind((self.addr, self.port))
        self.identifying_port = self.get_port()
        print(f"[*] new relay running at port {self.identifying_port}")


    def get_port(self) -> int:
        # if the socket is closed this exceptions out
        return self._sck.getsockname()[1]

    def open(self):
        # backlog default of five, should make this configurable
        self._sck.listen(self.backlog)
        self._sck.setblocking(False)
        assert self._sck.getblocking() == False


        while(not self.close_req.is_set()) and (not self.atomic_close.is_set()):
            # accept any new connection (foreign host)
            try:
                # connection
                con, addr = self._sck.accept()
                # ensure that con should be non-blocking
                con.setblocking(False)
                self.connection_table[addr] = con
                # new message created
                print(f"[*] new connection on {addr} for relay port {self.identifying_port}")
                self.inbound.put((RelayMessageTypes.NEW_CONNECTION, addr[0], addr[1], b'', self.identifying_port), True)
            except BlockingIOError:
                pass

            # READ from foreign hosts to our dedicated host

            # we cannot remove entries from the dictionary in-place while iterating through it (unfortunately).
            # this is probably because the dictionary iterator logic cannot confirm that the key we deleted has 
            # already been safely passed by the iterator at runtime.
            # we create a copy of the dictionary keys and iterate through that instead
            addresses = list(self.connection_table.keys())
            for addr in addresses:
                # get connection socket (con)
                con = self.connection_table[addr]
                # check if connection has closed
                if not is_socket_open(con):
                    # instruct remote client to close connection (is this the correct semantic?)
                    del self.connection_table[addr]
                    self.inbound.put((RelayMessageTypes.CLOSE_CONNECTION, addr[0], addr[1], b'', self.identifying_port), True)
                    continue

                # do we have any data to forward to the dedicated cilent?
                try:
                    data = con.recv(4096)
                    self.inbound.put((RelayMessageTypes.MESSAGE, addr[0], addr[1], data, self.identifying_port), True)
                except BlockingIOError:
                    pass
            # WRITE to foreign hosts
            try:
                # TODO: maybe refactor? Unsure of the efficiency here
                write_exceed = Event()
                
                write_exceed.clear()
                def set_write_exceed():
                    write_exceed.set()

                t = Timer(1.0, set_write_exceed)
                t.start()
                while (not write_exceed.is_set()) and (not self.outbound.empty()):
                    msg, address, port, data, _id_port = self.outbound.get(True)
                    con = self.connection_table[(address, port)]
                    if msg == RelayMessageTypes.MESSAGE:
                        # what if (addr, port) don't exist in the table?
                        # what if exception?
                        con.sendall(data)
                    if msg == RelayMessageTypes.CLOSE_CONNECTION:
                        con.close()
                    else:
                        # SHOULD RAISE ERROR
                        # we cannot have any other relay message types
                        pass
                # cancel timer if not yet completed
                t.cancel()

            except:
                print("timer fail")
                pass


            sched_yield()
            
        # clean up pending connections
        for sock in self.connection_table.values():
            # doesn't matter if we call ".close()" on a closed socket it should NOT raise an exception
            sock.close()

        self._sck.close()
        

    def close(self):
        pass
