import threading

### Saving and retrieving the parent task ID and current task ID

thread_local_data = threading.local()


def set_parent_task_id(task_id):
    thread_local_data.parent_task_id = task_id


def get_parent_task_id():
    return getattr(thread_local_data, "parent_task_id", None)


def set_task_id(task_id):
    thread_local_data.task_id = task_id


def get_task_id():
    return getattr(thread_local_data, "task_id", None)
