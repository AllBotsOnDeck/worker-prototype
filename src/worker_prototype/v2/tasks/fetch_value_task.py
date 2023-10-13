# Fetch value from hashmap task
from worker_prototype.v2.db import (
    TaskStatus,
    create_task,
    get_task,
    set_task_error,
    set_task_result,
    set_task_status,
    task_db,
    mock_info_store,
)
from worker_prototype.v2.errors import InvalidTaskStateError, LockedTaskError
from worker_prototype.v2.task_utils import (
    enqueue_failure_callback,
    enqueue_success_callback,
    validate_task_status,
)


def fetch_value_task(id=None, data=None, callback_id=None, type="fetch_value_task"):
    if type != "fetch_value_task":
        raise ValueError(f"Task type must be fetch_value_task, not {type}")
    # if this is our first time running this task, create a new id
    if id is None:
        id = create_task(type=type, callback_id=callback_id, data=data)["id"]
    elif task_db[id].status != TaskStatus.CREATED:
        raise InvalidTaskStateError(f"It looks like we've already run this task {id}")

    # get lock
    task = get_task(id)
    data = task.data
    if not task.lock.acquire(blocking=False):
        # we don't expect this task to ever be re-run so something is wrong
        raise LockedTaskError(f"Task {id} is already running")

    try:
        validate_task_status(
            id, [TaskStatus.PENDING, TaskStatus.RETRYING, TaskStatus.CREATED]
        )

        set_task_status(id, TaskStatus.RUNNING)

        try:
            key = data["key"]
        except TypeError:
            set_task_error(id, "data must be a dict")
            enqueue_failure_callback(id)
            return
        except KeyError:
            set_task_error(id, "key not found in task data")
            enqueue_failure_callback(id)
            return

        try:
            value = mock_info_store[key]
        except KeyError:
            set_task_error(id, f"Key {key} not found")
            enqueue_failure_callback(id)
            return

        set_task_result(id, value)
        enqueue_success_callback(id)
        return
    finally:
        task.lock.release()
