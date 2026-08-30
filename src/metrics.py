from prometheus_client import Gauge, Counter, Histogram

BYTES_TRANSFERRED = Counter(
    "portport_bytes_total",
    "TCP bytes transported via relay server",
    ["direction"],
)

MGMT_CONNECTIONS = Gauge(
    "portport_management_connections",
    "total active relay-management connections (i.e., hosting clients)"
)

ACTIVE_RELAYS = Gauge(
    "portport_active_relays",
    "Total current active relays"
)

MESSAGE_PROCESSING_SECONDS = Histogram(
    "portport_message_processing_seconds",
    "Time spent processing management messages",
    ["type"],
)

MESSAGES = Counter(
    "portport_messages",
    "Relay managemant messages handled"
)

ACTIVE_CONNECTIONS = Gauge(
    "portport_connections",
    "Total proxied TCP connections"
)