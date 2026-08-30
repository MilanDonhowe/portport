# portport: small dynamic TCP reverse proxy.

Small reverse proxy intended for temporarily exposing locally hosted services to external hosts via remote gateway.

### Use case:

You want to host some service from your local machine (i.e., maybe a minecraft server), but your network firewall doesn't allow for inbound traffic (i.e., the internet) to access your local machine (i.e., there's no port-forwarding on your network) and for whatever reason, you lack control over your network router settings or don't desire modifying them.

Assuming you have some virtual private server you can create out bound connections to, you can use portport to create a relay, and from there allow external hosts to reach your locally hosted minecraft server using your VPS as a sort of "relay".

### Usage or quick start:

On your VPS (*also making sure you have the correct firewall settings):
`python portport.py --port 5555 --port-range 9000-9100`

On your local:
`python client.py --relay-host <RELAY_HOST> --relay-port 5555 --local-port <local port> --auth <uuid>`

And now external hosts can access your local TCP server via your VPS IP (the port on the VPS will be randomly selected, you will need to whitelist it on your VM).
