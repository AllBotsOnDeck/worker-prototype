import queue
import threading
import random
import uuid
from enum import Enum
from dataclasses import dataclass, field
import logging

logging.basicConfig(level=logging.DEBUG)

q = queue.Queue()
q_lock = threading.Lock()


# task status enum
class TaskStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    PENDING = "pending"
    RETRYING = "retrying"
    SUCCESS = "success"
    FAILED = "failed"


# NOTE: currently there's no way to get the progress of a task
# this could be stored in the DB but that might make it more difficult to use external services
@dataclass
class Task:
    id: str
    type: str
    status: TaskStatus
    data: dict = None
    result: str = None
    error: str = None
    callback_id: str = None
    parent_id: str = None
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class QueueMessage:
    type: str
    id: str = None
    data: dict = None
    callback_id: str = None


task_db: dict[str, Task] = {}


mock_info_store = {
    "v1": 1,
    "v2": 2,
    "v3": 3,
}


class InvalidTaskStatusError(Exception):
    pass


class InvalidTaskIdError(Exception):
    pass


class InvalidTaskTypeError(Exception):
    pass


class InvalidTaskCallbackIdError(Exception):
    pass


class InvalidTaskDataError(Exception):
    pass


class InvalidTaskStateError(Exception):
    pass


class LockedTaskError(Exception):
    pass


def create_task(id, type, callback_id, data, parent_id=None):
    if id is None:
        id = str(uuid.uuid4())
    if type is None:
        raise ValueError("type must be specified")
    if callback_id is None:
        raise ValueError("callback_id must be specified")
    if id in task_db:
        raise ValueError(f"Task with id {id} already exists")
    if id not in task_db:
        task_db[id] = Task(
            id=id,
            type=type,
            status=TaskStatus.CREATED,
            callback_id=callback_id,
            data=data,
            parent_id=parent_id,
        )
    return task_db[id]


def create_top_level_task(id, type, data):
    if id is None:
        id = str(uuid.uuid4())
    if type is None:
        raise ValueError("type must be specified")
    if id in task_db:
        raise ValueError(f"Task with id {id} already exists")
    if id not in task_db:
        task_db[id] = Task(
            id=id,
            type=type,
            status=TaskStatus.CREATED,
            data=data,
            parent_id=None,
        )
    return task_db[id]


def get_task(id):
    try:
        return task_db[id]
    except KeyError:
        raise InvalidTaskIdError(f"Task {id} not found")


def set_task_status(id, status):
    task = get_task(id)
    task.status = status


def set_task_result(id, result):
    task = get_task(id)
    task.result = result
    task.status = TaskStatus.SUCCESS


def set_task_error(id, error):
    task = get_task(id)
    task.error = error
    task.status = TaskStatus.FAILED


def set_task_callback_id(id, callback_id):
    task = get_task(id)
    task.callback_id = callback_id


def validate_task_status(id, expected_status_list, no_error=False):
    task = get_task(id)
    if task.status not in expected_status_list:
        if no_error:
            return False
        raise InvalidTaskStatusError(
            f"Task {id} has unexpected status {task.status}, expected one of {expected_status_list}"
        )
    return True


# TODO: we can eliminate this by storing all task data in the task object and pulling it when we run the task
def validate_task(id, type, callback_id, data):
    task = get_task(id)
    if task.type != type:
        raise InvalidTaskTypeError(
            f"Task {id} was previously type {task.type} but is now type {type}"
        )
    if task.callback_id != callback_id:
        raise InvalidTaskCallbackIdError(
            f"Task {id} previously had callback_id {task.callback_id} but now has callback_id {callback_id}"
        )
    # deep compare data
    if task.data != data:
        raise InvalidTaskDataError(
            f"Task {id} previously had data {task.data} but now has data {data}"
        )


def create_message(type, id=None, data=None, callback_id=None):
    return QueueMessage(type=type, id=id, data=data, callback_id=callback_id)


# NOTE: for the success and failure callbacks we explicitly call the parent but this could also
# - call a url to allow interfacing with external services
# - fire an event to a topic

# to facilitate this we could add a success/error callback_url field to the task instead of a callback_id
# calling some external services might be more complex so we could even specify a callback task for more flexibility
# the task could, for example, authenticate with the external service and then call the callback_url


def enqueue_success_callback(id, callback_id):
    task = get_task(id)

    logging.debug(f"Task {id} succeeded! Info {task}")
    task.status = TaskStatus.SUCCESS

    if callback_id is None:
        return

    parent_task = get_task(callback_id)

    # re-enque the parent task
    message = create_message(
        type=parent_task.type,
        callback_id=parent_task.callback_id,
        data=parent_task.data,
        id=parent_task.id,
    )
    with q_lock:
        q.put(message)


def enqueue_failure_callback(id, callback_id):
    task = get_task(id)

    logging.debug(f"Task {id} failed! Info: {task}")
    task.status = TaskStatus.FAILED

    if callback_id is None:
        return

    parent_task = get_task(callback_id)

    # re-enque the parent task
    message = create_message(
        type=parent_task.type,
        callback_id=parent_task.callback_id,
        data=parent_task.data,
        id=parent_task.id,
    )
    with q_lock:
        q.put(message)


# Fetch value from hashmap task
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
    if not task.lock.acquire(blocking=False):
        # we don't expect this task to ever be re-run so something is wrong
        raise LockedTaskError(f"Task {id} is already running")

    try:
        validate_task_status(
            id, [TaskStatus.PENDING, TaskStatus.RETRYING, TaskStatus.CREATED]
        )
        validate_task(id, type, callback_id, data)

        set_task_status(id, TaskStatus.RUNNING)

        try:
            key = data["key"]
        except TypeError:
            set_task_error(id, "data must be a dict")
            enqueue_failure_callback(id, callback_id)
            return
        except KeyError:
            set_task_error(id, "key not found in task data")
            enqueue_failure_callback(id, callback_id)
            return

        try:
            value = mock_info_store[key]
        except KeyError:
            set_task_error(id, f"Key {key} not found")
            enqueue_failure_callback(id, callback_id)
            return

        set_task_result(id, value)
        enqueue_success_callback(id, callback_id)
        return
    finally:
        task.lock.release()


class AddTwoRandomValuesSerialState(Enum):
    BEFORE_FIRST_FETCH = "before_first_fetch"
    AFTER_FIRST_FETCH = "after_first_fetch"
    AFTER_SECOND_FETCH = "after_second_fetch"
    ERROR_FETCHING_VALUE = "error_fetching_value"


def add_two_random_values_serial_task(
    id=None, data=None, callback_id=None, type="add_two_random_values_serial_task"
):
    if type != "add_two_random_values_serial_task":
        raise ValueError(
            f"Task type must be add_two_random_values_serial_task, not {type}"
        )

    if id is None:
        id = create_task(type=type, callback_id=callback_id, data=data)["id"]

    task = get_task(id)
    with task.lock:
        if not validate_task_status(
            id,
            [TaskStatus.PENDING, TaskStatus.RETRYING, TaskStatus.CREATED],
            no_error=True,
        ):
            validate_task_status(id, [TaskStatus.SUCCESS, TaskStatus.FAILED])
            return
        validate_task(id, type, callback_id, data)

        set_task_status(id, TaskStatus.RUNNING)

        # find child tasks
        child_tasks = [t for t in task_db.values() if t.parent_id == id]

        # determine state
        if len(child_tasks) == 0:
            state = AddTwoRandomValuesSerialState.BEFORE_FIRST_FETCH
        elif len(child_tasks) == 1:
            raise InvalidTaskStateError(
                f"Child task {child_tasks[0].id} has only 1 child task, but both child tasks are initialized at the same time"
            )
        elif len(child_tasks) == 2 and all(
            [t.status == TaskStatus.SUCCESS for t in child_tasks]
        ):
            state = AddTwoRandomValuesSerialState.AFTER_SECOND_FETCH
        elif len(child_tasks) == 2 and any(
            [t.status == TaskStatus.FAILED for t in child_tasks]
        ):
            state = AddTwoRandomValuesSerialState.ERROR_FETCHING_VALUE
        elif len(child_tasks) == 2 and any(
            [t.status in [TaskStatus.CREATED, TaskStatus.RUNNING] for t in child_tasks]
        ):
            state = AddTwoRandomValuesSerialState.AFTER_FIRST_FETCH
        else:
            raise InvalidTaskStateError(
                f"Child tasks have invalid statuses: {child_tasks[0].status}, {child_tasks[1].status} for task {id}"
            )

        match state:
            case AddTwoRandomValuesSerialState.BEFORE_FIRST_FETCH:
                # NOTE: the 2nd task could be created after the first task has already completed but this adds some interesting complexity (maybe)
                key1 = random.randint(1, 4)
                key2 = random.randint(1, 4)

                subtask1_id = str(uuid.uuid4())
                create_task(
                    id=subtask1_id,
                    type="fetch_value_task",
                    callback_id=id,
                    data={"key": f"v{key1}"},
                    parent_id=id,
                )
                subtask2_id = str(uuid.uuid4())
                create_task(
                    id=subtask2_id,
                    type="fetch_value_task",
                    callback_id=id,
                    data={"key": f"v{key2}"},
                    parent_id=id,
                )

                m1 = create_message(
                    id=subtask1_id,
                    type="fetch_value_task",
                    data={"key": f"v{key1}"},
                    callback_id=id,
                )
                with q_lock:
                    q.put(m1)
                set_task_status(id, TaskStatus.PENDING)
                return
            case AddTwoRandomValuesSerialState.AFTER_FIRST_FETCH:
                # grab the 2nd key from the uncompleted task
                subtask2 = [c for c in child_tasks if c.status == TaskStatus.CREATED][0]
                key2 = subtask2.data["key"]

                m2 = create_message(
                    id=subtask2.id,
                    type="fetch_value_task",
                    data={"key": key2},
                    callback_id=id,
                )
                with q_lock:
                    q.put(m2)
                set_task_status(id, TaskStatus.PENDING)
                return
            case AddTwoRandomValuesSerialState.AFTER_SECOND_FETCH:
                # grab the result key from the completed tasks
                results = [c.result for c in child_tasks]

                set_task_result(id, sum(results))
                enqueue_success_callback(id, callback_id)
                return
            case AddTwoRandomValuesSerialState.ERROR_FETCHING_VALUE:
                # grab the error from the failed task
                errors = [c.error for c in child_tasks if c.status == TaskStatus.FAILED]

                error_string = "Errors: " + ", ".join(errors)
                set_task_error(id, error_string)
                enqueue_failure_callback(id, callback_id)
                return
            case _:
                raise InvalidTaskStateError(f"Invalid state {state} for task {id}")


class AddTwoRandomValuesParallelState(Enum):
    BEFORE_FETCH = "before_fetch"
    BEFORE_SECOND_FETCH = "before_second_fetch"
    BEFORE_SUCCESS = "one_success"
    BOTH_SUCCESS = "both_success"
    ERROR_FETCHING_VALUE = "error_fetching_value"


def add_two_random_values_parallel_task(
    id=None, data=None, callback_id=None, type="add_two_random_values_parallel_task"
):
    if type != "add_two_random_values_parallel_task":
        raise ValueError(
            f"Task type must be add_two_random_values_parallel_task, not {type}"
        )

    if id is None:
        id = create_task(type=type, callback_id=callback_id, data=data)["id"]

    # get lock
    task = get_task(id)
    with task.lock:
        if not validate_task_status(
            id,
            [TaskStatus.PENDING, TaskStatus.RETRYING, TaskStatus.CREATED],
            no_error=True,
        ):
            validate_task_status(id, [TaskStatus.SUCCESS, TaskStatus.FAILED])
            return
        validate_task(id, type, callback_id, data)

        set_task_status(id, TaskStatus.RUNNING)

        # find child tasks
        child_tasks = [t for t in task_db.values() if t.parent_id == id]

        # determine state
        if len(child_tasks) == 0:
            state = AddTwoRandomValuesParallelState.BEFORE_FETCH
        elif len(child_tasks) == 1:
            state = AddTwoRandomValuesParallelState.BEFORE_SECOND_FETCH
        elif len(child_tasks) == 2 and all(
            [t.status == TaskStatus.SUCCESS for t in child_tasks]
        ):
            state = AddTwoRandomValuesParallelState.BOTH_SUCCESS
        elif len(child_tasks) == 2 and any(
            [t.status == TaskStatus.FAILED for t in child_tasks]
        ):
            state = AddTwoRandomValuesParallelState.ERROR_FETCHING_VALUE
        elif len(child_tasks) == 2 and all(
            [
                t.status not in [TaskStatus.SUCCESS, TaskStatus.FAILED]
                for t in child_tasks
            ]
        ):
            state = AddTwoRandomValuesParallelState.BEFORE_SUCCESS
        else:
            raise InvalidTaskStateError(
                f"Child tasks have invalid statuses: {child_tasks[0].status}, {child_tasks[1].status} for task {id}"
            )

        match state:
            case AddTwoRandomValuesParallelState.BEFORE_FETCH:
                key1 = random.randint(1, 4)
                key2 = random.randint(1, 4)

                task1_id = str(uuid.uuid4())

                task1 = create_task(
                    id=task1_id,
                    type="fetch_value_task",
                    callback_id=id,
                    data={"key": f"v{key1}"},
                    parent_id=id,
                )
                m1 = create_message(
                    id=task1_id,
                    type="fetch_value_task",
                    data={"key": f"v{key1}"},
                    callback_id=id,
                )
                with q_lock:
                    q.put(m1)

                task2_id = str(uuid.uuid4())
                task2 = create_task(
                    id=task2_id,
                    type="fetch_value_task",
                    callback_id=id,
                    data={"key": f"v{key2}"},
                    parent_id=id,
                )
                m2 = create_message(
                    id=task2_id,
                    type="fetch_value_task",
                    data={"key": f"v{key2}"},
                    callback_id=id,
                )
                with q_lock:
                    q.put(m2)

                set_task_status(id, TaskStatus.PENDING)
                return
            case AddTwoRandomValuesParallelState.BEFORE_SECOND_FETCH | AddTwoRandomValuesParallelState.BEFORE_SUCCESS:
                set_task_status(id, TaskStatus.PENDING)
                return
            case AddTwoRandomValuesParallelState.BOTH_SUCCESS:
                # grab the result key from the completed tasks
                results = [c.result for c in child_tasks]

                set_task_result(id, sum(results))
                enqueue_success_callback(id, callback_id)
                return
            case AddTwoRandomValuesParallelState.ERROR_FETCHING_VALUE:
                # grab the error from the failed task
                errors = [c.error for c in child_tasks if c.status == TaskStatus.FAILED]

                error_string = "Errors: " + ", ".join(errors)
                set_task_error(id, error_string)
                enqueue_failure_callback(id, callback_id)
                return
            case _:
                raise InvalidTaskStateError(f"Invalid state {state} for task {id}")


task_map = {
    "fetch_value_task": fetch_value_task,
    "add_two_random_values_serial_task": add_two_random_values_serial_task,
    "add_two_random_values_parallel_task": add_two_random_values_parallel_task,
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
        type = message.type
        threading.Thread(target=task_map[type], kwargs=message.__dict__).start()


def main():
    for i in range(100):
        task = create_top_level_task(
            id=str(uuid.uuid4()),
            type="add_two_random_values_serial_task",
            data={},
        )
        message = create_message(
            type="add_two_random_values_serial_task",
            id=task.id,
            callback_id=None,
            data={},
        )
        with q_lock:
            q.put(message)

    for i in range(100):
        task = create_top_level_task(
            id=str(uuid.uuid4()),
            type="add_two_random_values_parallel_task",
            data={},
        )
        message = create_message(
            type="add_two_random_values_parallel_task",
            id=task.id,
            callback_id=None,
            data={},
        )
        with q_lock:
            q.put(message)

    thread = threading.Thread(target=queue_worker)

    thread.start()


if __name__ == "__main__":
    main()
