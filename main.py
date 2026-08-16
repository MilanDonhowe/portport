from server.common import *
from logging import getLogger
from queue import Queue
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
# {"type": "data", "address": "152.123.543.12", "port": 1244, "data": "base64"}

def relayMgmt(s: socket.socket, close: threading.Event):
    """thread to handle relay mgmt"""
    # TODO: do like an auth handshake or something with openssl

    #relays = []
    # from external (foreign host)
	#inbound = Queue()
    # to the client socket (s.sendall)
    
    # messages written back to client host (s)
    inbound_data: Queue[tuple[RelayMessageTypes, str, int, bytes]] = Queue()
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
            data = s.recv(4096)
            buffer = buffer + data
            try:
                json_result, rem = grab_json(buffer)
                print("[*] got result: ", json_result)
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
                        pass
                    else:
                        s.sendall(BAD_TYPE_JSON_ERROR)
                else:
                    s.sendall(GENERIC_JSON_ERROR)
                    
            except:
                failed_decode += 1
                if failed_decode >= 3:
                    s.close()
                    close.set()
                    continue
        except BlockingIOError:
            pass

        # check: inbound message to send?
        # check: did we get a control msg?
		# send msg!
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