#===================================================================================================
# portport.py reverse proxy server
# 
# 
#
#===================================================================================================
import signal
import threading
import argparse
from src.common import *
from types import FrameType
from src.relay import RELAY_SERVER_LOGGER_NAME
from src.crypto import generate_ssc
from pathlib import Path
from prometheus_client import start_http_server
from uuid import uuid4
from src.relay_manager import start_relay_mgmt_server, RELAY_MGMT_PORT, parse_port_range
from logging import getLogger

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

    parser.add_argument(
        "--port-range",
        default="0",
        help='''Port range for opening external hosts with format "<min openable port>-<max openable port>"
        For instance, 5000-5005 would result in the server attempting to open relays on port 5000, 5001, 5002, 5003, 5004 or 5005.
        '''
    )

    args = parser.parse_args()

    # TODO: parse port-range
    port_range = None
    if args.port_range != "0":
        try:
            port_range = parse_port_range(args.port_range)
        except Exception as e:
            logger.error(f"Ran into exception when parsing provided port range={e}")

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
        service_close_event,
        args.port, 
        args.key,
        args.cert, 
        args.passphrase,
        args.auth,
        args.no_auth,
        port_range
    )




if __name__ == "__main__":
    main()

