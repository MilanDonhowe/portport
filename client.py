#===================================================================================================
#
# Relay Service!
# Ok, so this should work as the relay client that runs on the host system that needs this relay service.
# 
# The main idea here is we dynamically create a reverse-proxy with our relay service by initializing a TCP connection
# from this client program and spin up as many relay endpoints (proxies) as we want and redirect their TCP traffic to
# services running on the host.  We'll test with a basic ncat example first
#
#
# long term goal would be:
# relay_client --relay <ip> --port 4000 --cert <cert.pem>
# 
# 
#===================================================================================================
from socket import socket, SOL_SOCKET, SO_KEEPALIVE, AddressFamily, SocketKind, SO_REUSEADDR
from threading import Thread, Event
from server.relay import RelayMessageTypes
from queue import Queue, Empty
import signal, sys
from types import FrameType
import json
from server.common import grab_json, is_socket_open
from base64 import b64encode, b64decode
from os import sched_yield
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
from typing import List
import ssl

proxy_threads = {}

class ProxiedConnection():
    """local socket connection"""
    def __init__(self, connection: socket, addr: tuple[str, int], recv_queue: Queue[tuple[RelayMessageTypes, str, int, bytes, int]], relay_id: int):
        self.conn = connection
        self.sendq = bytes()
        self.relay_id = relay_id
        self.addr = addr
        self.recv_queue = recv_queue
        self.closed_msg_sent = False # have we already sent a "CLOSE_CONNECTION" message?

    def handle_io_event(self, mask: int):
        if mask & EVENT_READ:
            try:
                data = self.conn.recv(4096)
                if len(data) == 0:
                    # EOF
                    self.conn.close()
                    self.recv_queue.put((RelayMessageTypes.CLOSE_CONNECTION, self.addr[0], self.addr[1], data, self.relay_id))
                    self.closed_msg_sent = True
                    return
                self.recv_queue.put((RelayMessageTypes.MESSAGE, self.addr[0], self.addr[1], data, self.relay_id))
            except (ConnectionResetError, BrokenPipeError):
                self.conn.close()
                return
            except BlockingIOError:
                pass
        if mask & EVENT_WRITE and len(self.sendq) > 0:
            try:
                sent_len = self.conn.send(self.sendq)
                self.sendq = self.sendq[sent_len:]
            except (ConnectionResetError, BrokenPipeError):
                self.recv_queue.put((RelayMessageTypes.CLOSE_CONNECTION, self.addr[0], self.addr[1], b'', self.relay_id))
                self.conn.close()
                self.closed_msg_sent = True
            except BlockingIOError:
                pass
            
        

def create_local_proxy(local_port: int, remote_port: int, close_event: Event, to_local: Queue[tuple[RelayMessageTypes, str, int, bytes, int]], to_proxy: Queue[tuple[RelayMessageTypes, str, int, bytes, int]]):
    # inbound = data from local service
    # outbound = data to local service
    selector = DefaultSelector()
    connections: dict[tuple[str,int], ProxiedConnection] = {}



    while not close_event.is_set():
        # handle inbound data from relay
        try:
            BOUNDED_BATCH=5
            for _ in range(BOUNDED_BATCH):
                msg_type, addr, port, data, _r_port = to_local.get_nowait()
                if msg_type == RelayMessageTypes.NEW_CONNECTION:
                    connections[(addr,port)] = ProxiedConnection(socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM), (addr, port), to_proxy, _r_port)
                    selector.register(connections[(addr,port)].conn, EVENT_READ | EVENT_WRITE, connections[(addr,port)].handle_io_event)
                    connections[(addr,port)].conn.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
                    try:
                        # .connect will block--maybe fix that in the future not sure
                        connections[(addr,port)].conn.connect(('0.0.0.0', local_port))
                        connections[(addr,port)].conn.setblocking(False)
                    except ConnectionRefusedError:
                        to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, addr, port, b'', remote_port))
                        selector.unregister(connections[(addr,port)].conn)
                        connections[(addr,port)].conn.close() # ensure socket got cleaned up
                        del connections[(addr,port)] # remove from dict
                        continue
                elif msg_type == RelayMessageTypes.MESSAGE:
                    # check if socket online, then sendall
                    if (addr,port) not in connections:
                        # this should error, we're sending data to a connection we haven't setup yet
                        print("ERR: data from unknown connection " + addr + ":" + str(port))
                        to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, addr, port, b'', remote_port))
                        continue
                    # data should be already decoded from base64
                    connections[(addr,port)].sendq += data
                elif msg_type == RelayMessageTypes.CLOSE_CONNECTION:
                    if (addr, port) in connections:
                        selector.unregister(connections[(addr,port)].conn)
                        connections[(addr,port)].conn.close()
                        del connections[(addr,port)]
                    # otherwise we already cleaned it up, can proceed as normal
                else:
                    # should error
                    pass
        except Empty:
            pass

        # each connection check, collect and send outbound data for remote relay
        # handle socket I/O
        events = selector.select(1.0)
        for key, mask in events:
            key.data(mask)
        
        # clean up connections
        to_remove: List[tuple[str, int]] = []
        for connection in connections.values():
            if not is_socket_open(connection.conn):
                selector.unregister(connection.conn)
                to_remove.append(connection.addr)
                # make sure we notify proxy
                if not connection.closed_msg_sent:
                    to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, connection.addr[0], connection.addr[1], b'', connection.relay_id))
                    connection.closed_msg_sent = True
                    

        # clean up connections dict
        for addr in to_remove:
            del connections[addr]
        sched_yield()
# hard coding this for testing
LOCAL_PORT = 1234

# rn this is on local for testing
REMOTE_MGMT_SERVICE = "0.0.0.0"
REMOTE_MGMT_SERVICE_PORT = 1600

# setup TLS
context = ssl.create_default_context()
context.load_verify_locations("cert.pem")

# connection to relay server
client_connection = socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM)
client_connection.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
client_connection.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)

client_connection_ssl = context.wrap_socket(client_connection, server_hostname="localhost")
client_connection_ssl.connect(('0.0.0.0', REMOTE_MGMT_SERVICE_PORT))

close_service = Event()
close_service.clear()

def ctrl_c_handler(signum: int, frame: FrameType | None):
    """
    Function executed when Ctrl+C is pressed.
    signum: The signal number (usually 2 for SIGINT)
    frame: The current stack frame object
    """
    print("\n[*][Intercepted] Ctrl+C pressed! Performing safe shutdown...")
    close_service.set()

    # closes main thread
    sys.exit(0)

signal.signal(signal.SIGINT, ctrl_c_handler)
signal.signal(signal.SIGTERM, ctrl_c_handler)


# start up procedures

def create_remote_proxy(close: Event):
    # 1. create relay connection on remote server
    print("[*] requesting relay for service hosted on port " + str(LOCAL_PORT))
    client_connection_ssl.sendall(json.dumps({
        "type": "control",
        "event": "create_relay"
    }).encode('utf8'))

    data = client_connection_ssl.recv(4096)
    # switch to non-blocking
    client_connection_ssl.setblocking(False)

    sendq = bytes()
    recvq = bytes()

    result, data = grab_json(data)
    remote_port = result['port']
    print("[*] created relay on remote port: ", remote_port)
    # create local binding
    to_remote_proxy: Queue[tuple[RelayMessageTypes, str, int, bytes, int]]  = Queue()
    from_remote_proxy: Queue[tuple[RelayMessageTypes, str, int, bytes, int]]  = Queue()

    local_proxy_th = Thread(target=create_local_proxy, args=(LOCAL_PORT, remote_port, close, from_remote_proxy, to_remote_proxy,))
    local_proxy_th.start()

    # switch to using selector
    selector = DefaultSelector()


    def handle_relay(conn: ssl.SSLSocket, mask: int):
        nonlocal sendq, recvq
        if (mask & EVENT_WRITE) and len(sendq) > 0:
            try:
                sent_len = conn.send(sendq)
                sendq = sendq[sent_len:]
            except (BrokenPipeError, ConnectionResetError):
                conn.close()
                selector.unregister(conn)
                close.set()
                return
                # Do I need to add a msg to the queue? probably  
            except (BlockingIOError, ssl.SSLWantWriteError, ssl.SSLWantReadError):
                pass     
        if mask & EVENT_READ:
            try:
                data = conn.recv(4096)
                if len(data) == 0:
                    conn.close()
                    close.set()
                    selector.unregister(conn)
                    return
                recvq += data
            except (ConnectionResetError, BrokenPipeError):
                conn.close()
                close.set()
                selector.unregister(conn)
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                pass
        return

    def process_msg(msg: dict) -> None: # type: ignore
        # TODO: Validate msg
        if msg["type"] == "control":
            if msg["event"] == "new_connection":
                from_remote_proxy.put((RelayMessageTypes.NEW_CONNECTION, msg["address"], msg["port"], b'', msg["relay_port"]))
            elif msg["event"] == "close_connection":
                from_remote_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, msg["address"], msg["port"], b'', msg["relay_port"]))
            else:
                # error?
                raise Exception("Unknown event")        
        elif msg["type"] == "data":
            decoded_data = b64decode(msg["data"])
            from_remote_proxy.put((RelayMessageTypes.MESSAGE, msg["address"], msg["port"], decoded_data, msg["relay_port"]))
        return
    selector.register(client_connection_ssl, EVENT_READ | EVENT_WRITE, handle_relay)

    failed_decodes = 0
    while not close.is_set():
        events = selector.select(timeout=1)
        for key, mask in events:
            key.data(key.fileobj, mask)

        while len(recvq) > 0:
            try:
                json_obj, recvq = grab_json(recvq)
                process_msg(json_obj)
                failed_decodes = 0
            except:
                failed_decodes += 1
                if failed_decodes > 3:
                    print("too many failed json decodes, something went wrong.  exiting...")
                    close.set()
                break

        # do we have any data to send back?
        try:
            BOUNDED_BATCH=5
            for _ in range(BOUNDED_BATCH):
                # check message queue 
                msgType, addr, port, outbound_data, r_port = to_remote_proxy.get_nowait()
                if msgType == RelayMessageTypes.CLOSE_CONNECTION:
                    sendq += json.dumps({
                        "type": "control",
                        "event": "close_connection",
                        "address": addr,
                        "port": port,
                        "relay_port": r_port
                    }).encode("utf8")
                elif msgType == RelayMessageTypes.MESSAGE:
                    sendq += json.dumps({
                        "type":"data",
                        "address": addr,
                        "port": port,
                        "relay_port": r_port,
                        "data": b64encode(outbound_data).decode('utf8')
                    }).encode("utf8")
                else:
                    raise Exception("Unknown RelayMessageType!")
        except Empty:
            pass

        sched_yield()
    local_proxy_th.join()


r_proxy_thread = Thread(target=create_remote_proxy, args=(close_service,))
r_proxy_thread.start()

while not close_service.is_set():
    sched_yield()


print("[*] running client service")