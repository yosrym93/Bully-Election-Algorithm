class MessageTypes:
    ELECTION_START = 1
    ELECTION_REPLY = 2
    LEADER = 3


send_base_port = 5000
server_port = 6000

leader_request_timeout = 1000
election_reply_timeout = 1000

nodes_ips = [
    '127.0.0.1',
    '127.0.0.1',
    '127.0.0.1'
]

