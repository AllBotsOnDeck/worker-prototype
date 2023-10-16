from worker_prototype.v3.db import get_task
from worker_prototype.v3.errors import InvalidTaskStatusError


def validate_task_status(id, expected_status_list, no_error=False):
    task = get_task(id)
    if task.status not in expected_status_list:
        if no_error:
            return False
        raise InvalidTaskStatusError(
            f"Task {id} has unexpected status {task.status}, expected one of {expected_status_list}"
        )
    return True
