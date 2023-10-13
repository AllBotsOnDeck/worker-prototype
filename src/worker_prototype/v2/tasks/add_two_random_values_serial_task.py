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
from worker_prototype.v2.errors import InvalidTaskStateError
from worker_prototype.v2.q import create_message, q_lock, q
from worker_prototype.v2.task_utils import (
    enqueue_failure_callback,
    enqueue_success_callback,
    validate_task_status,
)
from enum import Enum
import uuid
import random


class AddTwoRandomValuesSerialState(Enum):
    BEFORE_FIRST_FETCH = "before_first_fetch"
    AFTER_FIRST_FETCH = "after_first_fetch"
    AFTER_SECOND_FETCH = "after_second_fetch"
    ERROR_FETCHING_VALUE = "error_fetching_value"


def add_two_random_values_serial_task(id):
    task = get_task(id)
    with task.lock:
        if not validate_task_status(
            id,
            [TaskStatus.PENDING, TaskStatus.RETRYING, TaskStatus.CREATED],
            no_error=True,
        ):
            validate_task_status(id, [TaskStatus.SUCCESS, TaskStatus.FAILED])
            return

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
                )

                # NOTE: if we were being stricter we might want to grab a lock on the subtask here
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
                )
                with q_lock:
                    q.put(m2)
                set_task_status(id, TaskStatus.PENDING)
                return
            case AddTwoRandomValuesSerialState.AFTER_SECOND_FETCH:
                # grab the result key from the completed tasks
                results = [c.result for c in child_tasks]

                set_task_result(id, sum(results))
                enqueue_success_callback(id)
                return
            case AddTwoRandomValuesSerialState.ERROR_FETCHING_VALUE:
                # grab the error from the failed task
                errors = [c.error for c in child_tasks if c.status == TaskStatus.FAILED]

                error_string = "Errors: " + ", ".join(errors)
                set_task_error(id, error_string)
                enqueue_failure_callback(id)
                return
            case _:
                raise InvalidTaskStateError(f"Invalid state {state} for task {id}")
