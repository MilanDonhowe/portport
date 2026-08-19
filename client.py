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
# relay_client --relay <ip> --port 4000
# 
# 
#===================================================================================================
from socket import *
from threading import Thread, Event
from server.relay import Relay, RelayMessageTypes
from queue import Queue, Empty
import signal, sys
from types import FrameType
import json
from server.common import grab_json, is_socket_open
from base64 import b64encode, b64decode
from os import sched_yield


proxy_threads = {}

def create_local_proxy(local_port: int, remote_port: int, close_event: Event, to_local: Queue[tuple[RelayMessageTypes, str, int, bytes, int]], to_proxy: Queue[tuple[RelayMessageTypes, str, int, bytes, int]]):
    # inbound = data from local service
    # outbound = data to local service

    connections: dict[tuple[str,int], socket] = {}
    while not close_event.is_set():
        # handle inbound data from relay
        try:
            msg_type, addr, port, data, _r_port = to_local.get_nowait()
            if msg_type == RelayMessageTypes.NEW_CONNECTION:
                connections[(addr,port)] = socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM)
                connections[(addr,port)].setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
                try:
                    connections[(addr,port)].connect(('0.0.0.0', local_port))
                    connections[(addr,port)].setblocking(False)
                except ConnectionRefusedError:
                    to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, addr, port, b'', remote_port))
                    continue
            elif msg_type == RelayMessageTypes.MESSAGE:
                # check if socket online, then sendall
                if (addr,port) not in connections:
                    # this should error, we're sending data to a connection we haven't setup yet
                    print("ERR: data from unknown connection " + addr + ":" + str(port))
                    to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, addr, port, b'', remote_port))
                    continue
                sock = connections[(addr,port)]
                if not is_socket_open(sock):
                    to_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, addr, port, b'', remote_port))
                    continue
                # data should be already decoded
                sock.sendall(data)
            elif msg_type == RelayMessageTypes.CLOSE_CONNECTION:
                connections[(addr,port)].close()
                del connections[(addr,port)]
            else:
                # should error
                pass
        except Empty:
            pass

        # each connection check, collect and send outbound data for remote relay
        for (conn_addr, conn_port), conn in connections.items():
            assert conn.getblocking() == False
            # ensure all queued data is shoved into queue
            exhausted=False
            while (not exhausted) and (not close_event.is_set()):
                try:
                    data = conn.recv(4096)
                    to_proxy.put((RelayMessageTypes.MESSAGE, conn_addr, conn_port, data, remote_port))
                except BlockingIOError:
                    exhausted=True
                except:
                    exhausted=True
        
        sched_yield()
# hard coding this for testing
LOCAL_PORT = 1234

# rn this is on local for testing
REMOTE_MGMT_SERVICE = "0.0.0.0"
REMOTE_MGMT_SERVICE_PORT = 1600

# connection to relay server
client_connection = socket(AddressFamily.AF_INET, SocketKind.SOCK_STREAM)
client_connection.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
client_connection.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
client_connection.connect(('0.0.0.0', REMOTE_MGMT_SERVICE_PORT))

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
    print("[*] requesting relay")
    client_connection.sendall(json.dumps({
        "type": "control",
        "event": "create_relay"
    }).encode('utf8'))

    data = client_connection.recv(4096)
    # switch to non-blocking
    client_connection.setblocking(False)

    result, data = grab_json(data)
    remote_port = result['port']
    print("[*] created relay on remote port: ", remote_port)
    print(client_connection.getblocking())
    # create local binding
    to_remote_proxy: Queue[tuple[RelayMessageTypes, str, int, bytes, int]]  = Queue()
    from_remote_proxy: Queue[tuple[RelayMessageTypes, str, int, bytes, int]]  = Queue()

    local_proxy_th = Thread(target=create_local_proxy, args=(LOCAL_PORT, remote_port, close, from_remote_proxy, to_remote_proxy,))
    local_proxy_th.start()

    failed_decodes = 0
    while not close.is_set():
        # did remote proxy send data?
        # ach, we need to rework relay object since it needs to connect to existing service as a client, not as a server
        old_len = len(data)
        exhausted=False
        while not exhausted and not close.is_set():
            try:
                payload = client_connection.recv(4096)
                data = data + payload
            except ConnectionResetError:
                print("[*] relay service went down, exiting.")
                close.set()
                continue
            except BlockingIOError:
                exhausted = True

        # did we get more data?
        # TODO: more robust message validation
        if len(data) > old_len:
            try:
                msg, data = grab_json(data)
                if msg["type"] == "control":
                    if msg["event"] == "new_connection":
                        from_remote_proxy.put((RelayMessageTypes.NEW_CONNECTION, msg["address"], msg["port"], b'', msg["relay_port"]))
                    elif msg["event"] == "close_connection":
                        from_remote_proxy.put((RelayMessageTypes.CLOSE_CONNECTION, msg["address"], msg["port"], b'', msg["relay_port"]))
                    else:
                        # error?
                        pass
                elif msg["type"] == "data":
                    decoded_data = b64decode(msg["data"])
                    from_remote_proxy.put((RelayMessageTypes.MESSAGE, msg["address"], msg["port"], decoded_data, msg["relay_port"]))
            except:
                failed_decodes += 1
            if failed_decodes > 3:
                print("too many failed json decodes, something went wrong.  exiting...")
                close.set()
                break

        # do we have any data to send back?
        try:
            msgType, addr, port, outbound_data, r_port = to_remote_proxy.get_nowait()
            if msgType == RelayMessageTypes.CLOSE_CONNECTION:
                try:
                    client_connection.sendall(json.dumps({
                        "type": "control",
                        "event": "close_connection",
                        "address": addr,
                        "port": port,
                        "relay_port": r_port
                    }).encode("utf8"))
                except BlockingIOError:
                    # if this blocks, we have an error
                    close.set()
            elif msgType == RelayMessageTypes.MESSAGE:
                client_connection.sendall((json.dumps({
                    "type":"data",
                    "address": addr,
                    "port": port,
                    "relay_port": r_port,
                    "data": b64encode(outbound_data).decode('utf8')
                }).encode("utf8")))
            else:
                pass
        except Empty:
            pass
        except BrokenPipeError:
            print("[*] remote relay server went down, exiting...")
            close.set()
            continue

        sched_yield()
    local_proxy_th.join()


r_proxy_thread = Thread(target=create_remote_proxy, args=(close_service,))
r_proxy_thread.start()

while not close_service.is_set():
    sched_yield()


print("[*] running client service")