# client side
import json

if __name__ == "__main__":
    pass

# create connection to server

class Command():
    def __init__(self, id: str):
        self.COMMAND = id
        self.message_id = 0

remote = ''
addr = 1600

# json parse test
d=b''
try:
    d = b'{"apple":1}{}'
    json.loads(d)
except json.decoder.JSONDecodeError as e:
    print(e.msg)
    print(e.pos)
    print(d[:e.pos])

# JSON RPC
# 1. create relay endpoint
# 2. close relay endpoint
# 3. msg forward (connection_details, id, data)
