# portport: dynamic reverse proxy for TCP traffic.

In short: simple ngrok clone using python.

```
*external client connection* --> [ portport.py ] <-- TCP connection --> [ portportclient.py  <--> your local service    ] 
                                  external system                           your local machine
```
This allows external clients to connect to your local service via some external system.

Use case:

You want to host some service from your local machine, but unfortunately your network firewall doesn't allow for inbound traffic (i.e., the internet) to access your local machine (i.e., there's no port-forwarding on your network).

However, because you (presumably) can create outbound connections, you just need to forward data between some "relay" server.

Known limitations:
- This only forwards TCP traffic.
- Not well tested.  Works on my macbook but not necessarily
- Relay server does not use known port list.
- I'm using JSON as a framing protocol because I'm lazy and I don't really care about making this performant.

