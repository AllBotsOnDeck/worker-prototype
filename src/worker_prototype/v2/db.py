from dataclasses import dataclass, field
from enum import Enum
import threading
import uuid

from worker_prototype.v2.errors import InvalidTaskIdError


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


task_db: dict[str, Task] = {}

mock_info_store = {
    "v1": 1,
    "v2": 2,
    "v3": 3,
}


def create_task(type, callback_id, data, id=None, parent_id=None):
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


def create_top_level_task(
    type,
    data,
    id=None,
):
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
