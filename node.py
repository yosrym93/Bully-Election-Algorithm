import zmq
import random
import time
from config import *


class SwitchToLeaderException(Exception):
    pass


class SwitchToNodeException(Exception):
    pass


class Node:
    def __init__(self, priority, leader):

        self.priority = priority
        self.leader_priority = leader

        self.server_socket = None
        self.client_socket = None

        self.send_socket = None
        self.receive_socket = None

        self.init_common_sockets()

    def init_common_sockets(self):
        context = zmq.Context.instance()

        # Client socket to send requests from nodes to the leader
        self.client_socket = context.socket(zmq.REQ)
        self.client_socket.RCVTIMEO = leader_request_timeout*1000  # Receive timeout
        self.client_socket.setsockopt(zmq.REQ_RELAXED, 1)
        self.client_socket.setsockopt(zmq.REQ_CORRELATE, 1)

        # Server socket for the leader to serve requests
        self.server_socket = context.socket(zmq.REP)

        # Sockets to send and receive messages from all nodes
        self.send_socket = context.socket(zmq.PUB)
        self.receive_socket = context.socket(zmq.SUB)

        # Send sockets bind to a specific port based on the priority of the node
        receive_port = receive_base_port + self.priority
        self.receive_socket.bind('tcp://*:{}'.format(receive_port))

        # Receive sockets bind to all nodes
        for priority, ip in enumerate(nodes_ips):
            if priority != self.priority:
                ip = nodes_ips[priority]
                port = receive_base_port + priority
                self.send_socket.connect('tcp://{0}:{1}'.format(ip, port))

        self.receive_socket.subscribe(bytes([MessageTypes.LEADER]))

    def init_node_sockets(self):
        server_port = server_base_port + self.leader_priority
        self.client_socket.connect('tcp://{0}:{1}'.format(nodes_ips[self.leader_priority], server_port))

        self.receive_socket.subscribe(bytes([MessageTypes.ELECTION_REPLY, self.priority]))

        for priority in range(0, self.priority):
            self.receive_socket.subscribe(bytes([MessageTypes.ELECTION_START, priority]))

    def destroy_node_sockets(self):
        self.client_socket.disconnect(self.client_socket.getsockopt(zmq.LAST_ENDPOINT))
        self.receive_socket.unsubscribe(bytes([MessageTypes.ELECTION_REPLY, self.priority]))

        for priority in range(0, self.priority):
            self.receive_socket.unsubscribe(bytes([MessageTypes.ELECTION_START, priority]))

    def init_leader_sockets(self):
        server_port = server_base_port + self.priority
        self.server_socket.bind('tcp://*:{}'.format(server_port))

    def destroy_leader_sockets(self):
        self.server_socket.unbind(self.server_socket.getsockopt(zmq.LAST_ENDPOINT))

    def change_leader(self, new_leader):
        print('Changing leader from {0} to {1}'.format(self.leader_priority, new_leader))
        # Disconnect from the old leader server
        server_port = server_base_port + self.leader_priority
        self.client_socket.disconnect('tcp://{0}:{1}'.format(nodes_ips[self.leader_priority], server_port))

        # Connect to the new leader server
        server_port = server_base_port + new_leader
        self.client_socket.connect('tcp://{0}:{1}'.format(nodes_ips[new_leader], server_port))

        # Update the current leader
        self.leader_priority = new_leader

    def send_request_to_leader(self):
        self.client_socket.send_string(str(self.priority))
        try:
            self.client_socket.recv_string()

        except zmq.error.Again:
            # If the leader does not respond within a timeout, start a new election
            new_leader_elected = False
            while not new_leader_elected:
                new_leader_elected = self.start_election()

    def serve_node_request(self):
        # If any node sent a request, respond to it
        try:
            self.server_socket.recv_string(flags=zmq.NOBLOCK)
            self.server_socket.send_string(str(self.priority))

        except zmq.error.Again:
            return

    def handle_leader_message(self, leader_priority):
        # Handle leader messages
        if leader_priority > self.priority:
            if leader_priority != self.leader_priority:
                self.change_leader(new_leader=leader_priority)
        else:
            raise SwitchToLeaderException

    def start_election(self):
        print('{}: Started election'.format(self.priority))
        # Notify higher priority nodes of the election
        self.send_socket.send(bytes([MessageTypes.ELECTION_START, self.priority]))

        # Receive replies from higher priority nodes and checks from lower priority nodes
        reply_received = False
        start_time = time.time()
        print('{}: Waiting for higher priority nodes replies'.format(self.priority))
        while time.time() < start_time + election_reply_timeout:
            try:
                message_type, priority = self.receive_socket.recv(flags=zmq.NOBLOCK)
                if message_type == MessageTypes.ELECTION_REPLY:
                    reply_received = True
                elif message_type == MessageTypes.LEADER:
                    self.handle_leader_message(leader_priority=priority)
                    return True
                elif message_type == MessageTypes.ELECTION_START:
                    self.send_socket.send(bytes([MessageTypes.ELECTION_REPLY, priority]))

            except zmq.error.Again:
                continue

        # Wait for one of the higher nodes to announce themselves as the leader
        if reply_received:
            print('{}: Waiting for leader election'.format(self.priority))
            start_time = time.time()
            while time.time() < start_time + election_reply_timeout:
                try:
                    message_type, priority = self.receive_socket.recv(flags=zmq.NOBLOCK)
                    if message_type == MessageTypes.LEADER:
                        self.handle_leader_message(leader_priority=priority)
                        return True
                    elif message_type == MessageTypes.ELECTION_START:

                        self.send_socket.send(bytes([MessageTypes.ELECTION_REPLY, priority]))
                except zmq.error.Again:
                    continue

        # If none of the higher priority node responds, become the leader
        else:
            print('{}: Switching to leader'.format(self.priority))
            self.send_leader_message()
            raise SwitchToLeaderException

        return False

    def send_leader_message(self):
        self.send_socket.send(bytes([MessageTypes.LEADER, self.priority]))

    def run_leader(self):
        print('{}: Running leader'.format(self.priority))
        start_time = time.time()
        while True:
            self.serve_node_request()

            # Send leader message periodically
            # Faster than the election reply timeout to prevent any unnecessary elections
            if time.time() > start_time + election_reply_timeout/2:
                self.send_leader_message()
                start_time += 1

            try:
                message_type, priority = self.receive_socket.recv(flags=zmq.NOBLOCK)
                if message_type == MessageTypes.LEADER:
                    if priority > self.priority:
                        self.leader_priority = priority
                        raise SwitchToNodeException

            except zmq.error.Again:
                continue

    def run_node(self):
        print('{}: Running node'.format(self.priority))
        while True:
            try:
                # Probability of sending a request = 0.5 / number of nodes
                should_send_request = random.random() < 0.5 / len(nodes_ips)
                if should_send_request:
                    self.send_request_to_leader()

                message_type, priority = self.receive_socket.recv(flags=zmq.NOBLOCK)

                if message_type == MessageTypes.ELECTION_START:
                    self.send_socket.send(bytes([MessageTypes.ELECTION_REPLY, priority]))
                    self.start_election()
                elif message_type == MessageTypes.LEADER:
                    self.handle_leader_message(leader_priority=priority)

            except zmq.error.Again:
                continue

    def run(self):
        is_leader = self.priority == self.leader_priority
        while True:
            try:
                if is_leader:
                    self.init_leader_sockets()
                    self.run_leader()
                else:
                    self.init_node_sockets()
                    self.run_node()
            except SwitchToNodeException:
                self.destroy_leader_sockets()
                is_leader = False
            except SwitchToLeaderException:
                self.destroy_node_sockets()
                is_leader = True
                self.leader_priority = self.priority


def start(priority, leader):
    node = Node(priority, leader)
    node.run()
