import queue
import threading
from dataclasses import dataclass


q = queue.Queue()
# NOTE: the python queue might already be multi-thread safe, but I'm too lazy to figure out how to use it correctly
q_lock = threading.Lock()


@dataclass
class QueueMessage:
    id: str = None


def create_message(id):
    return QueueMessage(id=id)
