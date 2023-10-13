from worker_prototype.v2.db import TaskStatus, get_task
from worker_prototype.v2.errors import (
    InvalidTaskStatusError,
    InvalidTaskTypeError,
    InvalidTaskCallbackIdError,
    InvalidTaskDataError,
)
import logging

from worker_prototype.v2.q import create_message, q, q_lock

logging.basicConfig(level=logging.DEBUG)


def validate_task_status(id, expected_status_list, no_error=False):
    task = get_task(id)
    if task.status not in expected_status_list:
        if no_error:
            return False
        raise InvalidTaskStatusError(
            f"Task {id} has unexpected status {task.status}, expected one of {expected_status_list}"
        )
    return True


def enqueue_success_callback(id):
    task = get_task(id)

    logging.debug(f"Task {id} succeeded! Info {task}")
    task.status = TaskStatus.SUCCESS

    callback_id = task.callback_id

    if callback_id is None:
        return

    parent_task = get_task(callback_id)

    # re-enque the parent task
    message = create_message(
        id=parent_task.id,
    )
    with q_lock:
        q.put(message)


def enqueue_failure_callback(id):
    task = get_task(id)

    logging.debug(f"Task {id} failed! Info: {task}")
    task.status = TaskStatus.FAILED

    callback_id = task.callback_id

    if callback_id is None:
        return

    parent_task = get_task(callback_id)

    # re-enque the parent task
    message = create_message(
        id=parent_task.id,
    )
    with q_lock:
        q.put(message)
