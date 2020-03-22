class MessageTypes:
    ELECTION_START = 1
    ELECTION_REPLY = 2
    LEADER = 3


receive_base_port = 5000
server_base_port = 6000

leader_request_timeout = 3
election_reply_timeout = 1

nodes_ips = [
    '127.0.0.1',
    '127.0.0.1',
    '127.0.0.1',
    '127.0.0.1'
]

