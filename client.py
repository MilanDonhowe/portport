#===================================================================================================
#
# Relay Client Script
#
# Ok, so this should work as the relay client that runs on the host system that needs this relay service.
# 
# The main idea here is we dynamically create a reverse-proxy with our relay service by initializing a TCP connection
# from this client program and spin up as many relay endpoints (proxies) as we want and redirect their TCP traffic to
# services running on the host.
#
#
# python client.py --relay-host <ip> --relay-port 4000 --local-port 1234 --cert <cert.pem> --key <key.pem>
# 
# 
#===================================================================================================
from socket import socket, SOL_SOCKET, SO_KEEPALIVE, AddressFamily, SocketKind, SO_REUSEADDR
from threading import Thread, Event
from server.relay import RelayMessageTypes, QueuedRelayMessage
from queue import Queue, Empty
import signal, sys
from types import FrameType
from server.common import grab_msg, configure_logger, PortPortMessageType, PortPortMessage, wakeup_pair, PortPortErrorTypes
from os import sched_yield
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
import ssl
import argparse
import logging
from collections.abc import Callable

logger = logging.getLogger("portport-client")




# DEFAULTS
LOCAL_PORT = 1234
REMOTE_MGMT_SERVICE = "127.0.0.1"
REMOTE_MGMT_SERVICE_PORT = 1600


close_service = Event()
close_service.clear()

def ctrl_c_handler(signum: int, frame: FrameType | None):
    """
    Function executed when Ctrl+C is pressed.
    signum: The signal number (usually 2 for SIGINT)
    frame: The current stack frame object
    """
    logger.error("Ctrl+C pressed! Performing safe shutdown...")
    close_service.set()

    # closes main thread
    sys.exit(0)

signal.signal(signal.SIGINT, ctrl_c_handler)
signal.signal(signal.SIGTERM, ctrl_c_handler)

# TODO: fix the wake up socket logic here,
# it's very hard to follow and going to cause me a head-ache later

class ProxiedConnection():
    """local socket connection"""
    def __init__(self, connection: socket, addr: tuple[str, int], recv_queue: Queue[QueuedRelayMessage], relay_id: int, selector: DefaultSelector, wakeup_fn: Callable[[], None], wakeup_recv_handler: Callable[[], None]):
        self.conn = connection
        self.sendq = bytes()
        self.relay_id = relay_id
        self.addr = addr
        self.recv_queue = recv_queue
        self.closed_msg_sent = False # have we already sent a "CLOSE_CONNECTION" message?
        self.write_enabled = False
        self.selector_ref = selector
        self.wakeup_fn_ref = wakeup_fn
        self.wakeup_recv_handler_ref = wakeup_recv_handler

    def handle_io_event(self, _s: socket, mask: int):
        if mask & EVENT_READ:
            try:
                data = self.conn.recv(4096)
                if len(data) == 0:
                    # EOF
                    self.conn.close()
                    self.recv_queue.put((RelayMessageTypes.CLOSE_CONNECTION, self.addr[0], self.addr[1], data, self.relay_id))
                    self.wakeup_recv_handler_ref()
                    self.closed_msg_sent = True
                    return
                self.recv_queue.put((RelayMessageTypes.MESSAGE, self.addr[0], self.addr[1], data, self.relay_id))
                self.wakeup_recv_handler_ref()
            except (ConnectionResetError, BrokenPipeError):
                self.conn.close()
                return
            except BlockingIOError:
                pass
        if mask & EVENT_WRITE and len(self.sendq) > 0:
            try:
                sent_len = self.conn.send(self.sendq)
                self.sendq = self.sendq[sent_len:]
                if len(self.sendq) == 0:
                    self.write_enabled = False
                    self.selector_ref.modify(self.conn, EVENT_READ, self.handle_io_event)
                    self.wakeup_fn_ref()
            except (ConnectionResetError, BrokenPipeError):
                self.recv_queue.put((RelayMessageTypes.CLOSE_CONNECTION, self.addr[0], self.addr[1], b'', self.relay_id))
                self.wakeup_recv_handler_ref()
                self.conn.close()
                self.closed_msg_sent = True
            except BlockingIOError:
                pass
            
        

def create_local_proxy(
        local_port: int, 
        remote_port: int, 
        close_event: Event, 
        to_local: Queue[QueuedRelayMessage], 
        to_proxy: Queue[QueuedRelayMessage], 
        wakeup_remote_proxy: Callable[[],None],
        wakeup_local_sock: socket,
        handle_wakeup_local: Callable[[socket,int], None],
        wakeup_local_proxy: Callable[[], None]
        ):
    """local proxy thread that proxies connections to local service"""
    # inbound = data from local service
    # outbound = data to local service
    selector = DefaultSelector()
    connections: dict[tuple[str,int], ProxiedConnection] = {}

    
    selector.register(wakeup_local_sock, EVENT_READ, handle_wakeup_local)



    def cleanup_socket(address: tuple[str, int]):
        if address in connections:
            selector.unregister(connections[address].conn)
            connections[address].conn.close()
            del connections[address]
        else:
            raise KeyError("address in table")

    while not close_event.is_set():
        # handle inbound data from relay
        try:
            BOUNDED_BATCH=5
            for _ in range(BOUNDED_BATCH):
                msg_type, addr, port, data, _r_port = to_local.get_nowait()
                if msg_type == RelayMessageTypes.NEW_CONNECTION:
                    connections[(addr,port)] = ProxiedConnection(socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM), (addr, port), to_proxy, _r_port, selector, wakeup_local_proxy, wakeup_remote_proxy)
                    selector.register(connections[(addr,port)].conn, EVENT_READ, connections[(addr,port)].handle_io_event)
                    connections[(addr,port)].conn.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
                    try:
                        # .connect will block--maybe fix that in the future not sure
                        connections[(addr,port)].conn.connect(('0.0.0.0', local_port))
                        connections[(addr,port)].conn.setblocking(False)
                    except ConnectionRefusedError:
                        to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, addr, port, b'', remote_port))
                        wakeup_remote_proxy()
                        cleanup_socket((addr,port))
                        continue
                elif msg_type == RelayMessageTypes.MESSAGE:
                    # check if socket online, then sendall
                    if (addr,port) not in connections:
                        # this should error, we're sending data to a connection we haven't setup yet
                        logger.debug("ERR: data from unknown connection " + addr + ":" + str(port))
                        to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, addr, port, b'', remote_port))
                        wakeup_remote_proxy()
                        continue
                    # data should be already decoded from base64
                    connections[(addr,port)].sendq += data
                    if not connections[(addr,port)].write_enabled:
                        connections[(addr,port)].write_enabled = True
                        selector.modify(connections[(addr,port)].conn, EVENT_READ | EVENT_WRITE, connections[(addr,port)].handle_io_event)
                elif msg_type == RelayMessageTypes.CLOSE_CONNECTION:
                    cleanup_socket((addr,port))
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
            key.data(key.fileobj, mask)
        

        sched_yield()

    # clean up connections (these were not naturally closed)
    for addr in list(connections.keys()):
        relay_id = connections[addr].relay_id
        cleanup_socket(addr)
        to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, addr[0], addr[1], b'', relay_id))
        wakeup_remote_proxy()
        
                    





# start up procedures

def create_remote_proxy(close: Event, client_socket: ssl.SSLSocket, local_port: int, auth: str, skip_auth: bool):
    """thread interfacing with remote proxy"""

    # 1. create relay connection on remote server
    logger.info("requesting relay for service hosted on port " + str(local_port))

    # attempt handshake
    try:
        if skip_auth == False:
            auth_packet = PortPortMessage(PortPortMessageType.AUTH)
            client_socket.settimeout(30.0)
            auth_packet.auth = auth
            client_socket.sendall(auth_packet.serialize())

            auth_reply = client_socket.recv()
            decoded_msg, _data = grab_msg(auth_reply)
            if decoded_msg.msg_type != PortPortMessageType.AUTH_SUCCESS:
                raise Exception("invalid auth")
        else:
            logger.warning("skipping auth handshake")
        client_socket.sendall(PortPortMessage(PortPortMessageType.CREATE_RELAY).serialize())
        data = client_socket.recv()



        # switch to non-blocking
        client_socket.setblocking(False)

        result, data = grab_msg(data)

        if result.msg_type == PortPortMessageType.ERROR:
            logger.error(f"encountered error when trying to start up new relay: {result.error.name}. Aborting service...")
            close.set()
            return # short-circuit

        if result.msg_type != PortPortMessageType.OPEN_RELAY:
            logger.error(f"unexpected response from relay: {result.msg_type.name}, aborting service...")
            close.set()
            return

        remote_port = result.relay_port
    except Exception as e:
        logger.info(f"ran into exception {e} when attempting to establish connection to relay, exiting...")
        # in the future re-work this if client is able to support multiple connections
        close.set()
        return
    


    sendq = bytes()
    recvq = bytes()
    logger.info(f"created relay on remote port: {remote_port}")
    # create local binding
    to_remote_proxy: Queue[QueuedRelayMessage]  = Queue()
    from_remote_proxy: Queue[QueuedRelayMessage]  = Queue()

    # switch to using selector
    selector = DefaultSelector()

    # wake up for to remote_proxy
    wakeup_remote_sock, handle_remote_wakeup, wakeup_remote_proxy = wakeup_pair(close)
    selector.register(wakeup_remote_sock, EVENT_READ, handle_remote_wakeup)
    write_enabled = False

    # wake up for local proxy handing
    wakeup_local_sock, handle_local_wakeup, wakeup_local_proxy = wakeup_pair(close)
    

    local_proxy_th = Thread(target=create_local_proxy, args=(
        local_port, 
        remote_port, 
        close, 
        from_remote_proxy, 
        to_remote_proxy, 
        wakeup_remote_proxy, 
        wakeup_local_sock, 
        handle_local_wakeup,
         wakeup_local_proxy,))
    local_proxy_th.start()


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

    
    def enqueue_sendq(data: bytes):
        nonlocal sendq, write_enabled
        sendq += data
        if len(data) > 0 and write_enabled == False:
            write_enabled = True
            selector.modify(client_socket, EVENT_READ | EVENT_WRITE, handle_relay)


    def process_msg(msg: PortPortMessage) -> None: # type: ignore
        if msg.msg_type == PortPortMessageType.NEW_CONNECTION:
            from_remote_proxy.put((RelayMessageTypes.NEW_CONNECTION, str(msg.addr), msg.port, b'', msg.relay_port))
            wakeup_local_proxy()
        elif msg.msg_type == PortPortMessageType.CLOSE_CONNECTION:
            from_remote_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, str(msg.addr), msg.port, b'', msg.relay_port))
            wakeup_local_proxy()
        elif msg.msg_type == PortPortMessageType.ERROR:
            if msg.error == PortPortErrorTypes.RELAY_DOES_NOT_EXIST:
                logger.error("failed to create relay, exiting service...")
                close.set()
            else:
                logger.error("unhandled ERROR message from relay server, exiting service...")
                close.set()
            wakeup_local_proxy()
        elif msg.msg_type == PortPortMessageType.DATA:
            from_remote_proxy.put((RelayMessageTypes.MESSAGE, str(msg.addr), msg.port, msg.data, msg.relay_port))
            wakeup_local_proxy()
        else:
            logger.error(f"unsupported message type: \"{msg.msg_type}\"")
        return
    selector.register(client_socket, EVENT_READ, handle_relay)


    while not close.is_set():
        events = selector.select(timeout=1)
        for key, mask in events:
            key.data(key.fileobj, mask)

        while len(recvq) > 0:
            try:
                msg, recvq = grab_msg(recvq)
                process_msg(msg)
            except:
                break

        # do we have any data to send back?
        try:
            BOUNDED_BATCH=5
            for _ in range(BOUNDED_BATCH):
                # check message queue 
                msgType, addr, port, outbound_data, r_port = to_remote_proxy.get_nowait()
                if msgType == RelayMessageTypes.CLOSE_CONNECTION:
                    enqueue_sendq(PortPortMessage(
                        msg_type=PortPortMessageType.CLOSE_CONNECTION,
                        conn_addr=addr,
                        conn_port=port,
                        relay_port=r_port
                    ).serialize())
                elif msgType == RelayMessageTypes.MESSAGE:
                    enqueue_sendq(PortPortMessage(
                        msg_type=PortPortMessageType.DATA,
                        conn_addr=addr,
                        conn_port=port,
                        relay_port=r_port,
                        data=outbound_data
                    ).serialize())
                else:
                    raise Exception("Unknown RelayMessageType!")
        except Empty:
            pass

        sched_yield()
    local_proxy_th.join()


def start_client(relay_host: str, relay_port: int, local_port: int, auth: str, skip_auth: bool):
    # setup TLS
    context = ssl.create_default_context()
    context.load_verify_locations("cert.pem")

    # connection to relay server
    client_connection = socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM)
    client_connection.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    client_connection.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)

    client_connection_ssl = context.wrap_socket(client_connection, server_hostname="localhost")
    try:
        client_connection_ssl.connect((relay_host, relay_port))
    except ConnectionRefusedError:
        logger.error(f"could not connect to relay server at {relay_host}:{relay_port}")
        exit(3)
    
    r_proxy_thread = Thread(target=create_remote_proxy, args=(close_service,client_connection_ssl,local_port,auth,skip_auth,))
    r_proxy_thread.start()
    while not close_service.is_set():
        sched_yield()




def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run PortPort client"
    )

    parser.add_argument(
        "--relay-host", 
        default=REMOTE_MGMT_SERVICE, 
        help="external relay server hostname"
    )

    parser.add_argument(
        "--relay-port",
        type=int,
        default=REMOTE_MGMT_SERVICE_PORT,
        help="external relay management port"
    )

    parser.add_argument(
        "--local-port",
        type=int,
        required=True,
        help="local service port to expose"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="verbose logging"
    )

    parser.add_argument(
        "--auth",
        default="portport",
        help="auth token for accessing relay",
        required=False
    )

    parser.add_argument(
        "--no-auth",
        action="store_true",
        help="skips sending auth packet when connecting"
    )

    args = parser.parse_args()
    # configure logger
    configure_logger(args.verbose)

    # spin up client
    start_client(args.relay_host, args.relay_port, args.local_port, args.auth, args.no_auth)
    
    


if __name__ == "__main__":
    main()