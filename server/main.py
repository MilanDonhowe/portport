from relay import Relay
from logging import getLogger
from queue import Queue
import socket
RELAY_MGMT_PORT = 1600 

logger = getLogger("portport")


s = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)



def createRelay(shutdown: Event):
    # create relay
    # create queues
    inbound_queue = Queue
    outbound_queue = Queue
    relay = Relay()
    

if __name__ == '__main__':
    pass