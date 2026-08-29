#===================================================================================================
# portport.py reverse proxy server
# 
# 
#
#===================================================================================================
from server.common import *
from server.metrics import MGMT_CONNECTIONS, BYTES_TRANSFERRED, MESSAGE_PROCESSING_SECONDS
from logging import getLogger
from queue import Queue, Empty
import socket
import signal
import ssl
from ssl import SSLWantReadError, SSLWantWriteError
import threading
from os import sched_yield
from types import FrameType
from server.relay import Relay, RELAY_SERVER_LOGGER_NAME, QueuedRelayMessage
from typing import Dict
from selectors import DefaultSelector, EVENT_WRITE, EVENT_READ
from server.crypto import generate_ssc
from pathlib import Path
from prometheus_client import start_http_server
from uuid import uuid4
import argparse

RELAY_MGMT_PORT = 1600 
RELAY_MGMT_ADDR = "0.0.0.0"

logger = getLogger(RELAY_SERVER_LOGGER_NAME)


service_close_event = threading.Event()
service_close_event.clear()

def ctrl_c_handler(signum: int, frame: FrameType | None):
    """
    Function executed when Ctrl+C is pressed.
    signum: The signal number (usually 2 for SIGINT)
    frame: The current stack frame object
    """
    logger.error("Ctrl+C pressed! Performing safe shutdown...")
    service_close_event.set()

  

signal.signal(signal.SIGINT, ctrl_c_handler)
signal.signal(signal.SIGTERM, ctrl_c_handler)


def start_relay_mgmt_server(port: int, key_file: str, cert_file: str, passphrase: str, auth_token: str, auth_bypass: bool = False):

    # SSL context
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=cert_file, keyfile=key_file, password=passphrase.encode('utf8'))

    main_server = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
    main_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    main_server.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    # TODO: make host address configurable
    main_server.bind(('0.0.0.0', port))
    # non-blocking
    main_server.setblocking(False)
    # TODO: make back log configurable
    main_server.listen(5)

    if auth_bypass == True:
        logger.warning("warning: auth disabled! anyone can now control the relay server!")
        logger.info(f"spun up relay server on port {port}")
    else:
        logger.info(f"spun up relay server on port {port} with auth token \"{auth_token}\"")
    

    connection_table: dict[tuple[str, int], threading.Thread] = {}
    def add_to_conn_table(listening_socket: socket.socket, mask: int):
        if mask & EVENT_READ:
            inbound_socket, addr = listening_socket.accept()
            # apply SSL
            inbound_ssl_socket = context.wrap_socket(inbound_socket, server_side=True, do_handshake_on_connect=True)
            inbound_ssl_socket.settimeout(2.0) # it should be sent very quickly
            # While blocking ensure we get the Auth message
            try:
                if auth_bypass == False:
                    # the auth packet is small enough it shouldn't get fragmented over TCP/IP
                    auth_msg = inbound_ssl_socket.recv(4096)
                    # this should de-serialize correctly
                    auth_msg, _data = PortPortMessage.deserialize(auth_msg)
                    if auth_msg.msg_type != PortPortMessageType.AUTH:
                        logger.debug("initial message was not auth! killing client")
                        raise ConnectionError('Initial message with client not auth message')
                    if auth_msg.auth != auth_token:
                        # sendall() is typically bad since it blocks but again, this is a 3 byte write.  Should be over quickly
                        inbound_ssl_socket.sendall(PortPortMessage(PortPortMessageType.ERROR, err=PortPortErrorTypes.AUTH_FAILURE).serialize())
                        logger.debug("refused connection for invalid auth token")
                        raise Exception("Invalid auth token")
                    # else, success!
                    inbound_ssl_socket.sendall(PortPortMessage(PortPortMessageType.AUTH_SUCCESS).serialize())
                    logger.debug("successfully authenticated new relay managememt connection")

                else:
                    logger.warning("skipped auth handshake")
            except (TimeoutError, ConnectionError, BrokenPipeError, Exception, ConnectionResetError):
                inbound_ssl_socket.close()
                return

            # we have authenticated successfully! register + spin off this relay management thread
            inbound_ssl_socket.setblocking(False)
            connection_table[addr]= threading.Thread(target=relayMgmt, args=(inbound_ssl_socket, service_close_event,))
            connection_table[addr].start()
        return

    

    accept_selector = DefaultSelector()
    accept_selector.register(main_server, EVENT_READ)
    while not service_close_event.is_set():
        # new relay management connection
        events = accept_selector.select(timeout=5)
        for key, mask in events:
            add_to_conn_table(key.fileobj, mask) # type: ignore
        sched_yield()
        
    # close all active relays
    logger.info("closing relays")
    for _, thrd in connection_table.items():
        # this should have a timeout but I'm lazy
        thrd.join(timeout=1.0)
    return


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run PortPort server"
    )

    parser.add_argument(
        "--port",
        default=RELAY_MGMT_PORT,
        type=int,
        help="port for relay management server"
    )

    parser.add_argument(
        "--key",
        default="key.pem",
        help="private key pem file for TLS"
    )

    parser.add_argument(
        "--cert",
        default="cert.pem",
        help="certificate pem file to use for TLS"
    )

    parser.add_argument(
        "--auth",
        default=str(uuid4()),
        help="access token for local clients"
    )

    parser.add_argument(
        "--no-auth",
        action="store_true",
        help="disable authentication"
    )

    parser.add_argument(
        "--passphrase",
        default='portport',
        help="passphrase for private key"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="verbose logging"
    )

    parser.add_argument(
        "--metrics-port",
        default=9100,
        type=int,
        help="Prometheus metrics HTTP port; set to 0 to disable",
    )

    args = parser.parse_args()

    # configure logger
    configure_logger(args.verbose)


    if args.key == "key.pem" and args.cert == "cert.pem":
        if not Path("key.pem").is_file() or not Path("cert.pem").is_file():
            logger.info("default key/cert not present, generating self-signed certificate and private key...")
            generate_ssc()


    if args.metrics_port != 0:
        start_http_server(
            port=args.metrics_port,
            addr='0.0.0.0'
        )
        logger.info(f"Prometheus metrics server running on port {args.metrics_port}")


    # spin up server
    start_relay_mgmt_server(
        args.port, 
        args.key,
        args.cert, 
        args.passphrase,
        args.auth,
        args.no_auth
    )


def relayMgmt(s: ssl.SSLSocket, close_service: threading.Event):
    """thread to handle relay mgmt"""

    # metrics
    MGMT_CONNECTIONS.inc()

    close_thread = threading.Event()
    close_thread.clear()

    # messages written back to client host (s)
    inbound_data: Queue[QueuedRelayMessage] = Queue()
    relays: Dict[int, tuple[Relay, threading.Thread]] = dict()

    sendq: bytes = bytes()
    recvq = bytes()
    write_enabled = False
    selector = DefaultSelector()

    wakeup_read, handle_wakeup, wakeup_management = wakeup_pair(close_thread)
    selector.register(wakeup_read, EVENT_READ, handle_wakeup)
        
    def handle_relay_client(s: ssl.SSLSocket, mask: int):
        nonlocal sendq, recvq, write_enabled
        if mask & EVENT_WRITE and len(sendq) > 0:
            try:
                sent_len = s.send(sendq)
                sendq = sendq[sent_len:]
                # telemetry
                BYTES_TRANSFERRED.labels(direction="relay_to_client").inc(sent_len)
            except (ConnectionResetError, BrokenPipeError):
                close_thread.set()
            except (BlockingIOError, SSLWantReadError, SSLWantWriteError):
                pass
            if len(sendq) == 0:
                write_enabled = False
                selector.modify(s, EVENT_READ, handle_relay_client)
        if mask & EVENT_READ:
            try:
                data = s.recv(4096)
                if len(data) == 0:
                    close_thread.set()
                # telemetry
                BYTES_TRANSFERRED.labels(direction="client_to_relay").inc(len(data))
                # put onto processing queue
                recvq += data
            except (ConnectionResetError, BrokenPipeError):
                close_thread.set()
            except (BlockingIOError, SSLWantWriteError, SSLWantReadError):
                pass


    def add_to_sendq(data: bytes):
        nonlocal sendq, write_enabled
        sendq += data
        if not write_enabled and len(sendq)>0:
            write_enabled = True
            selector.modify(s, EVENT_WRITE|EVENT_READ, handle_relay_client)


    def process_msg(msg: PortPortMessage):
        with MESSAGE_PROCESSING_SECONDS.labels(type=msg.msg_type.name).time():
            nonlocal sendq
            if msg.msg_type == PortPortMessageType.CREATE_RELAY:
                # wake up socket pair
                # spawn new relay
                new_relay = Relay(close_service, inbound_data, Queue(), wakeup_management)
                # create thread
                new_relay_thread = threading.Thread(target=new_relay.open)
                relays[new_relay.get_port()] = (new_relay, new_relay_thread)
                # kick off relay thread (spawn thread)                                
                new_relay_thread.start()

                # notify client of new relay
                add_to_sendq(PortPortMessage(msg_type=PortPortMessageType.OPEN_RELAY, relay_port=new_relay.get_port()).serialize())
            # if local service closes connection
            elif msg.msg_type == PortPortMessageType.CLOSE_CONNECTION:
                relay, relay_t = relays[msg.relay_port]
                relay.enqueue_outbound((RelayMessageTypes.CLOSE_CONNECTION, str(msg.addr), msg.port, b'', msg.relay_port))
            elif msg.msg_type == PortPortMessageType.DESTROY_RELAY:
                if msg.relay_port in relays:
                    relay, relay_t = relays[msg.relay_port]
                    # signal close
                    relay.atomic_close.set()
                    # close thread (might have blocking issues here)
                    relay_t.join()
                    # remove from relay table
                    del relays[msg.relay_port]
                    # successful result
                    add_to_sendq(PortPortMessage(msg_type=PortPortMessageType.DESTROY_RELAY, relay_port=msg.relay_port).serialize())
            elif msg.msg_type == PortPortMessageType.DATA:
                # this data goes to foreign/external host
                if msg.relay_port not in relays:
                    add_to_sendq(PortPortMessage(msg_type=PortPortMessageType.ERROR, err=PortPortErrorTypes.RELAY_DOES_NOT_EXIST).serialize())
                else:
                    # data from client to foreign connection
                    relay = relays[msg.relay_port][0]
                    # identifying port is a bit redundant here but including it for uniformity between inbound/outbound queues
                    relay.enqueue_outbound((RelayMessageTypes.MESSAGE, str(msg.addr), msg.port, msg.data, msg.relay_port))

 


    selector.register(s, EVENT_READ, handle_relay_client)


    # on three failed decodes, we kill the connection
    failed_decode = 0
    while (not close_service.is_set()) and (not close_thread.is_set()):

        # socket I/O with proxied client
        events = selector.select(timeout=5)
        for key, mask in events:
            callback = key.data
            callback(key.fileobj, mask)

        # handle recv queue
        while len(recvq)>0:
            try:
                msg, recvq = grab_msg(recvq)
                process_msg(msg)
                failed_decode = 0
            # TODO: should probably have more intelligent error handling here
            except:
                failed_decode+=1
                break
        
        # handle sendq (messages from our relays to client relay mgmt connection)
        try:
            # should be bounded 
            BOUNDED_BATCH=5
            # should I handle more than one message at a time?
            for _ in range(BOUNDED_BATCH):
                msg_type, con_addr, con_port, data, relay_port = inbound_data.get_nowait()
                if msg_type == RelayMessageTypes.MESSAGE:
                    add_to_sendq(PortPortMessage(
                        msg_type=PortPortMessageType.DATA, 
                        conn_addr=con_addr, 
                        conn_port=con_port,
                        relay_port=relay_port,
                        data=data
                    ).serialize())
                # notify new connection
                elif msg_type == RelayMessageTypes.NEW_CONNECTION:
                    add_to_sendq(PortPortMessage(
                        msg_type=PortPortMessageType.NEW_CONNECTION,
                        conn_addr=con_addr,
                        conn_port=con_port,
                        relay_port=relay_port
                    ).serialize())
                elif msg_type == RelayMessageTypes.CLOSE_CONNECTION:
                    add_to_sendq(PortPortMessage(
                        msg_type=PortPortMessageType.CLOSE_CONNECTION,
                        conn_addr=con_addr,
                        conn_port=con_port,
                        relay_port=relay_port
                    ).serialize())
                else:
                    # really, we should probably raise some sort of exception here since this code path implies some unaccounted for inbound message
                    # from a proxied host payload intended for our client but for now I'm going to ignore it :)
                    pass
        except Empty:
            # normal: queue has no more items
            pass
        except Exception:
            logger.error("unexpected error handling outbound relay queue")
            close_thread.set()
        # yield control over CPU so other threads can execute
        sched_yield()
    logger.info("closing management socket")

    # close all pending threads
    for (r_obj, th) in relays.values():
        r_obj.atomic_close.set()
        r_obj.wakeup()
        th.join(timeout=2.0)
    selector.close()
    s.close()
    logger.info("relay sockets closed, done")
    MGMT_CONNECTIONS.dec()
    


if __name__ == "__main__":
    main()

