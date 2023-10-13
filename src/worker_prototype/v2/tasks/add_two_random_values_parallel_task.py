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


class AddTwoRandomValuesParallelState(Enum):
    BEFORE_FETCH = "before_fetch"
    BEFORE_SECOND_FETCH = "before_second_fetch"
    BEFORE_SUCCESS = "one_success"
    BOTH_SUCCESS = "both_success"
    ERROR_FETCHING_VALUE = "error_fetching_value"


def add_two_random_values_parallel_task(id):
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
                enqueue_success_callback(id)
                return
            case AddTwoRandomValuesParallelState.ERROR_FETCHING_VALUE:
                # grab the error from the failed task
                errors = [c.error for c in child_tasks if c.status == TaskStatus.FAILED]

                error_string = "Errors: " + ", ".join(errors)
                set_task_error(id, error_string)
                enqueue_failure_callback(id)
                return
            case _:
                raise InvalidTaskStateError(f"Invalid state {state} for task {id}")
