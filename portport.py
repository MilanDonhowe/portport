# client side

if __name__ == "__main__":
    pass

# create connection to server

class Command():
    def __init__(self, id: str):
        self.COMMAND = id
        self.message_id = 0

remote = ''
addr = 1600

# JSON RPC
# 1. create relay endpoint
# 2. close relay endpoint
# 3. msg forward (connection_details, id, data)
