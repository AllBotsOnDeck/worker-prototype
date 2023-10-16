from dataclasses import dataclass, field
from enum import Enum
import threading
import uuid

from worker_prototype.v3.errors import InvalidTaskIdError

import logging

logging.basicConfig(level=logging.DEBUG)


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
    name: str
    version: str
    status: TaskStatus
    data: dict = None
    result: str = None
    error: str = None
    parent_id: str = None
    cache: dict = None
    lock: threading.Lock = field(default_factory=threading.Lock)


task_db: dict[str, Task] = {}

mock_info_store = {
    "v1": 1,
    "v2": 2,
    "v3": 3,
}


def task_exists(id):
    return id in task_db


def create_task(name, version, data, id=None, parent_id=None):
    if id is None:
        id = str(uuid.uuid4())
    if name is None:
        raise ValueError("type must be specified")
    if version is None:
        raise ValueError("version must be specified")
    if id in task_db:
        raise ValueError(f"Task with id {id} already exists")
    if id not in task_db:
        task_db[id] = Task(
            id=id,
            name=name,
            version=version,
            status=TaskStatus.CREATED,
            data=data,
            parent_id=parent_id,
            cache={},  # for locally generated values
        )
    logging.debug(f"Created task {id}, {name}")
    return task_db[id]


def create_top_level_task(
    name,
    version,
    data,
    id=None,
):
    if id is None:
        id = str(uuid.uuid4())
    if name is None:
        raise ValueError("name must be specified")
    if version is None:
        raise ValueError("version must be specified")
    if id in task_db:
        raise ValueError(f"Task with id {id} already exists")
    if id not in task_db:
        task_db[id] = Task(
            id=id,
            name=name,
            version=version,
            status=TaskStatus.CREATED,
            data=data,
            parent_id=None,
            cache={},  # for locally generated values
        )
    logging.debug(f"Created task {id}, {name}")
    return task_db[id]


def get_task(id):
    try:
        return task_db[id]
    except KeyError:
        raise InvalidTaskIdError(f"Task {id} not found")


def set_task_status(id, status):
    task = get_task(id)
    task.status = status
    logging.debug(f"Set task {id} status to {status}")


def set_task_result(id, result):
    task = get_task(id)
    task.result = result
    task.status = TaskStatus.SUCCESS
    logging.debug(f"Set task {id} result to {result}")


def set_task_error(id, error):
    task = get_task(id)
    task.error = error
    task.status = TaskStatus.FAILED
    logging.debug(f"Set task {id} error to {error}")


def set_task_cache(id, key, value):
    task = get_task(id)
    task.cache[key] = value
    logging.debug(f"Set task {id} cache key {key} to {value}")


def get_task_cache(id, key):
    task = get_task(id)
    return task.cache[key]


def check_exists_task_cache(id, key):
    task = get_task(id)
    return key in task.cache
