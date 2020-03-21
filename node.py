import zmq
import random
import time
from config import *
from multiprocessing import Process


class SwitchToLeaderException(Exception):
    pass


class SwitchToNodeException(Exception):
    pass


class Node(Process):
    def __init__(self, priority, leader):
        super().__init__()

        self.priority = priority
        self.leader = leader

        self.server_socket = None
        self.client_socket = None

        self.send_socket = None
        self.receive_socket = None

        self.init_sockets()

    def init_sockets(self):
        context = zmq.Context.instance()

        # Leader socket to serve requests
        self.server_socket = context.socket(zmq.REP)

        # Sockets to send and receive messages from all nodes
        self.send_socket = context.socket(zmq.PUB)
        self.receive_socket = context.socket(zmq.SUB)

        # Send sockets bind to a specific port based on the priority of the node
        send_port = send_base_port + self.priority
        self.send_socket.bind('tcp://*:{}'.format(send_port))

        # Receive sockets bind to all nodes
        for priority, ip in enumerate(nodes_ips):
            if priority != self.priority:
                ip = nodes_ips[priority]
                port = send_base_port + priority
                self.receive_socket.connect('tcp://{0}:{1}'.format(ip, port))

        self.receive_socket.subscribe(bytes([MessageTypes.LEADER]))

    def init_node(self):
        context = zmq.Context.instance()
        self.client_socket = context.socket(zmq.REQ)
        self.client_socket.connect('tcp://{0}:{1}'.format(nodes_ips[self.leader], server_port))
        self.client_socket.RCVTIMEO = leader_request_timeout  # Receive timeout

        self.receive_socket.subscribe(bytes([MessageTypes.ELECTION_REPLY, self.priority]))

        for priority in range(0, self.priority):
            self.receive_socket.subscribe(bytes([MessageTypes.ELECTION_START, priority]))

    def init_leader(self):
        context = zmq.Context.instance()
        self.server_socket = context.socket(zmq.REP)
        self.server_socket.bind('tcp://*:{}'.format(server_port))

    def change_leader(self, new_leader):
        # Disconnect from the old leader
        self.client_socket.disconnect('tcp://{0}:{1}'.format(nodes_ips[self.leader], server_port))

        # Connect to the new leader
        self.client_socket.connect('tcp://{0}:{1}'.format(nodes_ips[new_leader], server_port))

        # Update the current leader
        self.leader = new_leader

    def send_request(self):
        print('{}: Sending request'.format(self.priority))
        self.client_socket.send_string(str(self.priority))
        try:
            self.client_socket.recv_string()
            print('{}: Response received'.format(self.priority))
        except zmq.error.Again:
            new_leader_elected = False
            while not new_leader_elected:
                new_leader_elected = self.start_election()

    def receive_request(self):
        print('{}: Receiving request'.format(self.priority))
        try:
            self.server_socket.recv_string(flags=zmq.NOBLOCK)
            self.server_socket.send_string(str(self.priority))
        except zmq.error.Again:
            return

    def start_election(self):
        print('{}: Started election'.format(self.priority))
        # Unsubscribe from lower priority start election messages
        for priority in range(0, self.priority):
            self.receive_socket.unsubscribe(bytes([MessageTypes.ELECTION_START, priority]))

        # Receive replies from higher priority nodes and checks from lower priority nodes
        new_leader_elected = False
        reply_received = False
        start = time.time()
        while not reply_received and time.time() < start + election_reply_timeout:
            print('{}: Waiting for reply'.format(self.priority))
            try:
                message_type, priority = self.receive_socket.recv(flags=zmq.NOBLOCK)
                if message_type == MessageTypes.ELECTION_REPLY:
                    reply_received = True
                elif message_type == MessageTypes.LEADER:
                    self.change_leader(new_leader=priority)
                    new_leader_elected = True
                else:
                    raise Exception('Wrong message type received while waiting for reply, '
                                    'received: {}'.format(message_type))
            except zmq.error.Again:
                continue

        if not new_leader_elected and reply_received:
            print('{}: Waiting for leader election'.format(self.priority))
            start = time.time()
            while not new_leader_elected and time.time() < start + election_reply_timeout:
                try:
                    message_type, leader = self.receive_socket.recv(flags=zmq.NOBLOCK)
                    if message_type == MessageTypes.LEADER:
                        self.change_leader(new_leader=leader)
                        new_leader_elected = True
                    else:
                        raise Exception('Wrong message type received while waiting for leader election, '
                                        'received: {}'.format(message_type))
                except zmq.error.Again:
                    continue

        else:
            print('{}: Switching to leader'.format(self.priority))
            raise SwitchToLeaderException

        # Resubscribe from lower priority start election messages
        for priority in range(0, self.priority):
            self.receive_socket.subscribe(bytes([MessageTypes.ELECTION_START, priority]))

        return new_leader_elected

    def run_leader(self):
        print('{}: Running leader'.format(self.priority))
        self.init_leader()
        while True:
            self.receive_request()
            self.send_socket.send(bytes([MessageTypes.LEADER, self.priority]))
            try:
                message_type, priority = self.receive_socket.recv(flags=zmq.NOBLOCK)
                if message_type == MessageTypes.LEADER:
                    if priority > self.priority:
                        self.switch_to_node()
            except zmq.error.Again:
                continue

    def run_node(self):
        print('{}: Running node'.format(self.priority))
        self.init_node()
        print('{}: Initialized sockets'.format(self.priority))
        while True:
            should_send_request = random.random() < 1 / len(nodes_ips)
            if should_send_request:
                self.send_request()

            try:
                print('1')
                message_type, priority = self.receive_socket.recv(flags=zmq.NOBLOCK)
                print('2')
                if message_type == MessageTypes.ELECTION_START:
                    self.send_socket.send(bytes([MessageTypes.ELECTION_REPLY, priority]))
                    self.start_election()
                elif message_type == MessageTypes.LEADER:
                    if priority != self.leader:
                        self.change_leader(new_leader=priority)
                else:
                    raise Exception('Received a reply without sending election start')
            except zmq.error.Again:
                continue

    def run(self):
        is_leader = self.priority == self.leader
        while True:
            try:
                if is_leader:
                    self.run_leader()
                else:
                    self.run_node()
            except SwitchToNodeException:
                is_leader = False
            except SwitchToLeaderException:
                is_leader = True
