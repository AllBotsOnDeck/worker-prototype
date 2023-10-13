from worker_prototype.v2.db import create_top_level_task, task_db
from worker_prototype.v2.tasks import (
    fetch_value_task,
    add_two_random_values_serial_task,
    add_two_random_values_parallel_task,
)
from worker_prototype.v2.q import create_message, q, q_lock
import threading
import queue
import logging

logging.basicConfig(level=logging.DEBUG)


task_map = {
    "fetch_value_task": fetch_value_task.fetch_value_task,
    "add_two_random_values_serial_task": add_two_random_values_serial_task.add_two_random_values_serial_task,
    "add_two_random_values_parallel_task": add_two_random_values_parallel_task.add_two_random_values_parallel_task,
}


def queue_worker():
    while True:
        message = None
        with q_lock:
            try:
                message = q.get(block=False)
            except queue.Empty:
                logging.debug("Queue is empty, done processing")
                return
        type = task_db[message.id].type
        threading.Thread(target=task_map[type], kwargs=message.__dict__).start()


def main():
    for i in range(100):
        task = create_top_level_task(
            type="add_two_random_values_serial_task",
            data={},
        )
        message = create_message(
            id=task.id,
        )
        with q_lock:
            q.put(message)

    for i in range(100):
        task = create_top_level_task(
            type="add_two_random_values_parallel_task",
            data={},
        )
        message = create_message(
            id=task.id,
        )
        with q_lock:
            q.put(message)

    thread = threading.Thread(target=queue_worker)

    thread.start()


if __name__ == "__main__":
    main()
