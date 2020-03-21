import sys
import os
import signal
from node import Node


if __name__ == '__main__':
    _, priority, leader = sys.argv
    priority = int(priority)
    leader = int(leader)
    node = Node(priority, leader)
    node.start()
    input('Press any key to exit\n')
    os.killpg(0, signal.SIGKILL)
