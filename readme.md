# portport: small dynamic TCP reverse proxy.

ngrok but slow (yet free!).

DISCLAIMER: this is not well-tested, not performant, and shouldn't be used in 99.9999% of the time.

### Use case:

You want to host some service from your local machine (i.e., maybe a minecraft server), but your network firewall doesn't allow for inbound traffic (i.e., the internet) to access your local machine (i.e., there's no port-forwarding on your network) and for whatever reason, you lack control over your network router settings or don't desire modifying them.

Assuming you have some virtual private server you can create out bound connections to, you can use portport to create a relay, and from there allow external hosts to reach your locally hosted minecraft server using your VPS as a sort of "relay".

### Usage or quick start:

On your VPS (*also making sure you have the correct firewall settings):
`python portport.py --port 5555`

On your local:
`python client.py --relay-host <RELAY_HOST> --relay-port 5555 --local-port <local port>`

And now external hosts can access your local TCP server via your VPS IP (the port on the VPS will be randomly selected, you will need to whitelist it on your VM).

### TODO:
- [ ] let users specify preferred port list on the relay
- [ ] add auth between relay client <--> relay server lol
- [ ] fix bad select() that probably is busy polling
- [ ] make ssl support not crude
- [ ] reduce head-of-line blocking by including multiple TCP connections between server and client
- [ ] better readme guide

