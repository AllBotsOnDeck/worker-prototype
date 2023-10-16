from worker_prototype.v3.db import create_top_level_task, get_task
from worker_prototype.v3.tasks import (
    add_two_random_values_serial_task,
    add_two_random_values_parallel_task,
)
from worker_prototype.v3.q import enqueue_id, q, q_lock
import threading
import queue
import logging
from worker_prototype.v3.task_registry import function_runner

logging.basicConfig(level=logging.DEBUG)


def queue_worker():
    # NOTE: currently runs forever
    while True:
        message = None
        with q_lock:
            try:
                message = q.get(block=False)
            except queue.Empty:
                # NOTE: we should be able to get rid of this by using the queue correctly but it's not the way it will really be run
                import time

                time.sleep(0.001)
                continue
                # logging.debug("Queue is empty, done processing")
                # return

        threading.Thread(target=function_runner, kwargs={"id": message.id}).start()


def main():
    for i in range(100):
        # NOTE: clean up file structure...
        add_two_random_values_serial_task.add_two_random_values_serial_task()

    for i in range(100):
        add_two_random_values_parallel_task.add_two_random_values_parallel_task()

    thread = threading.Thread(target=queue_worker)

    thread.start()


if __name__ == "__main__":
    main()
