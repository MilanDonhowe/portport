from server.common import *
from logging import getLogger
from queue import Queue, Empty
import socket
import signal
import sys
import time
import json
import threading
from os import sched_yield
from types import FrameType
from server.relay import Relay
from typing import List, Dict
from base64 import b64decode, b64encode

RELAY_MGMT_PORT = 1600 

logger = getLogger("portport")


main_server = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
main_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
main_server.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

# TODO: make host address configurable
main_server.bind(('0.0.0.0', RELAY_MGMT_PORT))
# non-blocking
main_server.setblocking(False)
main_server.listen(5)

print("[*] spun up relay server on port " + str(RELAY_MGMT_PORT))

service_close_event = threading.Event()
service_close_event.clear()

def ctrl_c_handler(signum: int, frame: FrameType | None):
    """
    Function executed when Ctrl+C is pressed.
    signum: The signal number (usually 2 for SIGINT)
    frame: The current stack frame object
    """
    print("\n[*][Intercepted] Ctrl+C pressed! Performing safe shutdown...")
    service_close_event.set()

    # closes main thread
    #sys.exit(0)

signal.signal(signal.SIGINT, ctrl_c_handler)
signal.signal(signal.SIGTERM, ctrl_c_handler)


# {"type": "control", "event": "create_relay"}
# {"type": "control", "event": "close_relay", "port": 44}
# {"type": "control", "event": "open_relay", "port": <int>}
# {"type": "data", "address": "152.123.543.12", "port": 1244, "data": "base64", "relay_port": 50011}

def relayMgmt(s: socket.socket, close: threading.Event):
    """thread to handle relay mgmt"""
    # TODO: do like an auth handshake or something with openssl

    #relays = []
    # from external (foreign host)
	#inbound = Queue()
    # to the client socket (s.sendall)
    
    # messages written back to client host (s)
    inbound_data: Queue[tuple[RelayMessageTypes, str, int, bytes, int]] = Queue()
    relays: Dict[int, tuple[Relay, threading.Thread]] = dict()

    buffer = b''
    # on three failed decodes, we kill the connection
    failed_decode = 0
    while not close.is_set():

        # check: is mgmt socket closed?
        if not is_socket_open(s):
          close.set()
          continue
        

        # check: message from client
        try:
            # data should be a valid json payload
            prior_payload_len = len(buffer)
            data = s.recv(8096)
            buffer = buffer + data
            
            # no new data?
            if prior_payload_len < len(buffer):
 
                try:
                    json_result, rem = grab_json(buffer)
                    buffer = rem
                    # process msg
                    if "type" in json_result:
                        if json_result["type"] == 'control':
                            # check control type
                            if "event" in json_result:
                                if json_result["event"] == 'create_relay':
                                    # spawn new relay
                                    new_relay = Relay(close, inbound_data, Queue())
                                    # create thread
                                    new_relay_thread = threading.Thread(target=new_relay.open)
                                    relays[new_relay.get_port()] = (new_relay, new_relay_thread)

                                    # kick off relay thread (spawn thread)                                
                                    new_relay_thread.start()

                                    # notify client
                                    s.sendall(json.dumps({
                                        "type": "control",
                                        "event": "open_relay",
                                        "port": new_relay.get_port()
                                    }).encode('utf8'))
                                # if local service closes connection
                                elif json_result["event"] == "close_connection":
                                    relay, relay_t = relays[json_result['relay_port']]
                                    relay.outbound.put((RelayMessageTypes.CLOSE_CONNECTION, json_result["address"], json_result["port"], b'', json_result["relay_port"]))
                                elif json_result["event"] == 'close_relay':
                                    if "port" in json_result:
                                        port = json_result["port"]
                                        if port in relays:
                                            relay, relay_t = relays[port]
                                            # signal close
                                            relay.atomic_close.set()
                                            # close thread (might have blocking issues here)
                                            relay_t.join()
                                            # remove from relay table
                                            del relays[port]
                                            # successful result
                                            s.sendall(json.dumps({"type":"control", "event":"relay_closed","port": port}).encode('utf8'))
                                        else:
                                            s.sendall(MISSING_RELAY_JSON_ERROR)
                                    else:
                                        s.sendall(MISSING_PORT_JSON_ERROR)
                                else:
                                    s.sendall(UNKNOWN_EVENT_JSON_ERROR)

                            else:
                                s.sendall(BAD_EVENT_JSON_ERROR)
                            pass
                        elif json_result["type"] == 'data':
                            if 'address' not in json_result or 'port' not in json_result or 'relay_port' not in json_result:
                                s.sendall(MISSING_ADDRESS_FIELDS)
                            elif "data" in json_result:
                                try:
                                    data = b64decode(json_result['data'])
                                    # this data goes to foreign host
                                    if json_result['relay_port'] not in relays:
                                        s.sendall(MISSING_RELAY_JSON_ERROR)
                                    else:
                                        # data from client to foreign connection
                                        relay = relays[json_result['relay_port']][0]
                                        # identifying port is a bit redundant here but including it for uniformity between inbound/outbound queues
                                        relay.outbound.put((RelayMessageTypes.MESSAGE, json_result['address'], json_result['port'], data, json_result['relay_port']))
                                except:
                                    s.sendall(DECODING_ERROR)
                            else:
                                s.sendall(MISSING_DATA_FIELD)
                        else:
                            s.sendall(BAD_TYPE_JSON_ERROR)
                    else:
                        s.sendall(GENERIC_JSON_ERROR)        
                    # we got all the way here with no error?
                    failed_decode=0
                except:
                    failed_decode += 1
                    if failed_decode >= 10:
                        print("[*] too many failed json decodes, exiting.")
                        s.close()
                        close.set()
                        continue
        except BlockingIOError:
            pass

        # check: inbound message to send?
        try:
            # should I handle more than one message at a time?
            msg_type, con_addr, con_port, data, relay_port = inbound_data.get_nowait()
            if msg_type == RelayMessageTypes.MESSAGE:
                s.sendall(json.dumps({
                    "type":"data",
                    "address": con_addr,
                    "port": con_port,
                    "relay_port": relay_port,
                    "data": b64encode(data).decode('utf8')
                }).encode("utf8"))
            # notify new connection
            elif msg_type == RelayMessageTypes.NEW_CONNECTION:
                s.sendall(json.dumps({
                    "type": "control",
                    "event": "new_connection",
                    "address": con_addr,
                    "port": con_port,
                    "relay_port": relay_port
                }).encode('utf8'))
            elif msg_type == RelayMessageTypes.CLOSE_CONNECTION:
                s.sendall(json.dumps({
                    "type": "control",
                    "event": "close_connection",
                    "address": con_addr,
                    "port": con_port,
                    "relay_port": relay_port
                }).encode('utf8'))
            else:
                # really, we should probably raise some sort of exception here since this code path implies some unaccounted for inbound message
                # from a proxied host payload intended for our client but for now I'm going to ignore it :)
                pass
        except Empty:
            pass

        # yield control over CPU so other threads can execute
        sched_yield()
    print("[*] closing management socket")

    # close all pending threads
    for (r_obj, th) in relays.values():
        r_obj.atomic_close.set()
        th.join()


connection_table: dict[tuple[str, int], threading.Thread] = {}
while not service_close_event.is_set():

    # new relay management connection
    try:
        sck, addr = main_server.accept()
        print("[*] creating new relay management thread")


        connection_table[addr]= threading.Thread(target=relayMgmt, args=(sck,service_close_event,))
        connection_table[addr].start()
    except BlockingIOError:
        pass

	
    sched_yield()


# close all active relays
for _, thrd in connection_table.items():
    # this should have a timeout but I'm lazy
    thrd.join()