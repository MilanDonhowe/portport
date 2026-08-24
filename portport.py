from server.common import *
from logging import getLogger
from queue import Queue, Empty
import socket
import signal
import ssl
from ssl import SSLWantReadError, SSLWantWriteError
import json
import threading
from os import sched_yield
from types import FrameType
from server.relay import Relay, RELAY_SERVER_LOGGER_NAME
from typing import Dict
from base64 import b64decode, b64encode
from selectors import DefaultSelector, EVENT_WRITE, EVENT_READ
from server.crypto import generate_ssc
from pathlib import Path
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



if not Path("key.pem").is_file() or not Path("cert.pem").is_file():
    generate_ssc()


def start_relay_mgmt_server(port: int, key_file: str, cert_file: str, passphrase: str):

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

    logger.info("spun up relay server on port " + str(port))


    connection_table: dict[tuple[str, int], threading.Thread] = {}
    def add_to_conn_table(listening_socket: socket.socket, mask: int):
        if mask & EVENT_READ:
            inbound_socket, addr = listening_socket.accept()
            # apply SSL
            inbound_ssl_socket = context.wrap_socket(inbound_socket, server_side=True, do_handshake_on_connect=True)
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
        "--passphrase",
        default='portport',
        help="passphrase for private key"
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="verbose logging"
    )

    args = parser.parse_args()

    # configure logger
    configure_logger(args.verbose)

    # spin up server
    start_relay_mgmt_server(args.port, args.key, args.cert, args.passphrase)





# {"type": "control", "event": "create_relay"}
# {"type": "control", "event": "close_relay", "port": 44}
# {"type": "control", "event": "open_relay", "port": <int>}
# {"type": "data", "address": "152.123.543.12", "port": 1244, "data": "base64", "relay_port": 50011}

def relayMgmt(s: ssl.SSLSocket, close_service: threading.Event):
    """thread to handle relay mgmt"""
    close_thread = threading.Event()
    close_thread.clear()

    # messages written back to client host (s)
    inbound_data: Queue[tuple[RelayMessageTypes, str, int, bytes, int]] = Queue()
    relays: Dict[int, tuple[Relay, threading.Thread]] = dict()

    sendq: bytes = bytes()
    recvq = bytes()

    def handle_relay_client(s: ssl.SSLSocket, mask: int):
        nonlocal sendq, recvq
        if mask & EVENT_WRITE and len(sendq) > 0:
            sent_len = s.send(sendq)
            sendq = sendq[sent_len:]
        if mask & EVENT_READ:
            try:
                data = s.recv(4096)
                if len(data) == 0:
                    close_thread.set()
                # put onto processing queue
                recvq += data
            except (ConnectionResetError, BrokenPipeError):
                close_thread.set()
            except (BlockingIOError, SSLWantWriteError, SSLWantReadError):
                pass
        #print(f"sendq size: {len(sendq)}, recvq size: {len(recvq)}")



    def process_msg(msg: dict):
        nonlocal sendq
        # TODO VALIDATE JSON
        if not valid_msg(msg):
            raise EncodingWarning("invalid json msg received")
        
        if msg["type"] == 'control':
            # check control type
            if msg["event"] == 'create_relay':
                # spawn new relay
                new_relay = Relay(close_service, inbound_data, Queue())
                # create thread
                new_relay_thread = threading.Thread(target=new_relay.open)
                relays[new_relay.get_port()] = (new_relay, new_relay_thread)
                # kick off relay thread (spawn thread)                                
                new_relay_thread.start()
                # notify client
                sendq += json.dumps({
                    "type": "control",
                    "event": "open_relay",
                    "port": new_relay.get_port()
                }).encode('utf8')
                # if local service closes connection
            elif msg["event"] == "close_connection":
                relay, relay_t = relays[msg['relay_port']]
                # TODO: validation better
                relay.outbound.put((RelayMessageTypes.CLOSE_CONNECTION, msg["address"], msg["port"], b'', msg["relay_port"])) # type: ignore
            elif msg["event"] == 'close_relay':
                if "port" in msg:
                    port = msg["port"]
                    if port in relays:
                        relay, relay_t = relays[port]
                        # signal close
                        relay.atomic_close.set()
                        # close thread (might have blocking issues here)
                        relay_t.join()
                        # remove from relay table
                        del relays[port]
                        # successful result
                        sendq += json.dumps({"type":"control", "event":"relay_closed","port": port}).encode('utf8')
                    else:
                        sendq += MISSING_RELAY_JSON_ERROR
                else:
                    sendq += MISSING_PORT_JSON_ERROR
            else:
                sendq += UNKNOWN_EVENT_JSON_ERROR
        elif msg["type"] == 'data':
            if 'address' not in msg or 'port' not in msg or 'relay_port' not in msg:
                sendq += MISSING_ADDRESS_FIELDS
            elif "data" in msg:
                try:
                    data = b64decode(msg['data']) # type: ignore
                    # this data goes to foreign host
                    if msg['relay_port'] not in relays:
                        sendq += MISSING_RELAY_JSON_ERROR
                    else:
                        # data from client to foreign connection
                        relay = relays[msg['relay_port']][0]
                        # identifying port is a bit redundant here but including it for uniformity between inbound/outbound queues
                        relay.outbound.put((RelayMessageTypes.MESSAGE, msg['address'], msg['port'], data, msg['relay_port'])) # type: ignore
                except:
                    sendq += DECODING_ERROR
            else:
                sendq += MISSING_DATA_FIELD
        else:
            sendq += BAD_TYPE_JSON_ERROR
 


    selector = DefaultSelector()
    selector.register(s, EVENT_READ | EVENT_WRITE, handle_relay_client)


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
                json_obj, recvq = grab_json(recvq)
                process_msg(json_obj)
                failed_decode = 0
            except:
                failed_decode+=1
                #if failed_decode > 100:
                #    logger.error("too many consecutive json decodes, killing relay mgmt thread")
                #    close_thread.set()
                break
        
        # handle sendq (messages from our relays to client relay mgmt connection)
        try:
            # should be bounded 
            BOUNDED_BATCH=5
            # should I handle more than one message at a time?
            for _ in range(BOUNDED_BATCH):
                msg_type, con_addr, con_port, data, relay_port = inbound_data.get_nowait()
                if msg_type == RelayMessageTypes.MESSAGE:
                    sendq += json.dumps({
                        "type":"data",
                        "address": con_addr,
                        "port": con_port,
                        "relay_port": relay_port,
                        "data": b64encode(data).decode('utf8')
                    }).encode("utf8")
                # notify new connection
                elif msg_type == RelayMessageTypes.NEW_CONNECTION:
                    sendq += json.dumps({
                        "type": "control",
                        "event": "new_connection",
                        "address": con_addr,
                        "port": con_port,
                        "relay_port": relay_port
                    }).encode('utf8')
                elif msg_type == RelayMessageTypes.CLOSE_CONNECTION:
                    sendq += json.dumps({
                        "type": "control",
                        "event": "close_connection",
                        "address": con_addr,
                        "port": con_port,
                        "relay_port": relay_port
                    }).encode('utf8')
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
        th.join(timeout=2.0)
    selector.close()
    s.close()
    logger.info("relay sockets closed, done")
    


if __name__ == "__main__":
    main()

