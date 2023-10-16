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


def enqueue_id(id):
    if id is None:
        raise ValueError("id must be specified")
    message = create_message(id)
    with q_lock:
        q.put(message)
