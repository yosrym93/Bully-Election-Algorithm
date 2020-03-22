import sys
import os
import signal
import node
from multiprocessing import Process


if __name__ == '__main__':
    _, priority, leader = sys.argv
    priority = int(priority)
    leader = int(leader)
    node_process = Process(target=node.start,
                           args=(priority, leader))
    node_process.start()

    input('Press any key to exit\n')
    os.kill(0, signal.SIGKILL)
