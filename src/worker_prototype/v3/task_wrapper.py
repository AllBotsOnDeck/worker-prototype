import functools
import hashlib
import json
import logging
import cloudpickle

from worker_prototype.v3.db import (
    TaskStatus,
    create_task,
    get_task,
    task_exists,
    set_task_status,
    set_task_result,
    set_task_error,
    create_top_level_task,
    get_task_cache,
    set_task_cache,
    check_exists_task_cache,
)
from worker_prototype.v3.q import enqueue_id
from worker_prototype.v3.task_registry import function_registry
from worker_prototype.v3.task_utils import validate_task_status
from worker_prototype.v3.thread_util import (
    get_parent_task_id,
    set_parent_task_id,
    get_task_id,
    set_task_id,
)

logging.basicConfig(level=logging.DEBUG)


class TaskError(Exception):
    pass


class SuspendTaskError(TaskError):
    pass


def hash_values(*values):
    # NOTE: is this too slow/memory intensive?
    hash_obj = hashlib.sha256()
    # NOTE: 2x as slow vs stringifying
    serialized_value = json.dumps(values, sort_keys=True, default=str)
    hash_obj.update(serialized_value.encode())
    return hash_obj.hexdigest()


def full_function_name(func):
    return f"{func.__module__}.{func.__qualname__}"


# functions are identified by name and version
# if a name is specified, we use that, otherwise we use the module and qualified function name
def get_func_name(func, name):
    if name is None:
        name = full_function_name(func)
    return name


# if a version is specified, we use that, otherwise we use a hash of the pickled code of the function
# NOTE: there are a number of other ideas I have here
# - use a hash of the source code of the function
#   - use inspect.getsource to get the source code, possibly with some recursive logic to get the source of dependencies
#   - use dill.source.importable with source=True to get a string of the source code
# I think realistically we should require a version and have the test suite warn if the cloudpickle hash changes
# like how jest requires you to re-snapshot values if they change
# I'm not sure how reliable it will be but I also don't like purely relying on humans to update it manually
def get_func_version(func, version):
    if version is None:
        try:
            version = hash_values(cloudpickle.dumps(func))
        except Exception as e:
            version = "unknown"
    return version


def generate_task_id(func, name, version, parent_task_id, kwargs):
    # Hash all the individual hashes together to form the final task ID
    task_id = hash_values(name, version, parent_task_id, kwargs)

    return task_id


# NOTE: I lock the task (and would mimic w/ a row level db lock) but perhaps that behavior isn't perfect
def async_task(
    retries=0,
    name=None,
    version=None,
    id_generator=generate_task_id,
):
    def decorator_task(func):
        # register the function
        function_name = get_func_name(func, name)
        function_version = get_func_version(func, version)
        func.name = function_name
        func.version = function_version

        @functools.wraps(func)
        def wrapper_task(**kwargs):
            logging.debug(f"Running task {function_name} with kwargs {kwargs}")
            # Ensure arguments are JSON serializable
            try:
                json.dumps(kwargs)
            except TypeError:
                raise TypeError("All arguments must be JSON serializable")

            parent_task_id = get_parent_task_id()
            thread_task_id = get_task_id()
            # NOTE: feels like a hack - prevents subtasks from using this task id
            set_task_id(None)

            if thread_task_id is not None:
                logging.debug(
                    f"Thread task id is {thread_task_id} so we are in the function runner"
                )
                task_id = thread_task_id
            else:
                logging.debug(
                    f"Thread task id is not set so we are running for the first time"
                )
                task_id = id_generator(
                    func=func,
                    name=function_name,
                    version=function_version,
                    parent_task_id=parent_task_id,
                    kwargs=kwargs,
                )

            if parent_task_id is not None:
                # we are within another task

                if task_exists(task_id):
                    task = get_task(task_id)

                    with task.lock:
                        if task:
                            if task.status == TaskStatus.SUCCESS:
                                return task.result
                            if task.status == TaskStatus.FAILED:
                                raise TaskError(
                                    f"Task {task_id} failed with error {task.error}"
                                )
                            else:
                                # NOTE: would be nice to double check the message queue here but we assume that if the task exists and hasn't completed, it's in the queue
                                # We could also take this opportunity to potentially check for task progress at this point
                                raise SuspendTaskError(
                                    f"Task {task_id} is still pending."
                                )
                else:
                    task = create_task(
                        name=function_name,
                        version=function_version,
                        parent_id=parent_task_id,
                        id=task_id,
                        data=kwargs,
                    )
                    with task.lock:
                        enqueue_id(task_id)
                        set_task_status(task_id, TaskStatus.PENDING)
                        raise SuspendTaskError(f"Task {task_id} is enqueued.")

            else:
                if task_exists(task_id):
                    task = get_task(task_id)
                    with task.lock:
                        # TODO: probably more validation possible in this function
                        if not validate_task_status(
                            task_id,
                            [
                                TaskStatus.PENDING,
                                TaskStatus.RETRYING,
                                TaskStatus.CREATED,
                            ],
                            no_error=True,
                        ):
                            # we probably already succeeded or failed
                            return
                        # In case of being the main (not within another task)
                        set_task_status(task_id, TaskStatus.RUNNING)

                        try:
                            set_parent_task_id(task_id)
                            # TODO: add retry logic (for now we can just allow people to wrap their function in a retry decorator)
                            result = func(**kwargs)
                            set_parent_task_id(None)
                            set_task_status(task_id, TaskStatus.SUCCESS)
                            set_task_result(task_id, result=result)

                            logging.debug(f"Task {task_id} succeeded! Info {task}")

                            # re-enqueue parent task
                            if task.parent_id is not None:
                                enqueue_id(task.parent_id)
                            return

                        except SuspendTaskError:
                            # Handle suspend - save the state or requeue, as needed.
                            set_task_status(task_id, TaskStatus.PENDING)

                            # this means a sub-task of this task is still running
                            return

                        except TaskError as e:
                            # Handle subtask error
                            set_task_status(task_id, TaskStatus.FAILED)

                            error_string = f"Task {task_id} failed becasue subtask failed with error {e}"

                            set_task_error(task_id, error=error_string)

                            logging.debug(f"Task {task_id} failed! Info {task}")

                            # re-enqueue parent task
                            if task.parent_id is not None:
                                enqueue_id(task.parent_id)
                            return

                        except Exception as e:
                            # Handle unexpected error
                            # NOTE: we could also handle retry logic here

                            set_task_status(task_id, TaskStatus.FAILED)

                            error_string = f"Task {task_id} failed with error {e}"
                            set_task_error(task_id, error=error_string)

                            logging.debug(f"Task {task_id} failed! Info {task}")

                            # re-enqueue parent task
                            if task.parent_id is not None:
                                enqueue_id(task.parent_id)
                            return
                else:
                    create_top_level_task(
                        name=function_name,
                        version=function_version,
                        id=task_id,
                        data=kwargs,
                    )
                    enqueue_id(task_id)
                    return

        function_registry.register(wrapper_task, function_name, function_version)

        return wrapper_task

    return decorator_task


# takes in a task list and returns a list of results
def run_in_parallel(tasks):
    suspend_exception = None
    fail_exception = None
    other_exception = None

    results = []

    for task in tasks:
        try:
            results.append(task())
        except SuspendTaskError as e:
            suspend_exception = e
        except TaskError as e:
            fail_exception = e
        except Exception as e:
            other_exception = e

    if other_exception is not None:
        raise other_exception
    elif fail_exception is not None:
        raise fail_exception
    elif suspend_exception is not None:
        raise suspend_exception
    else:
        return results


# NOTE: this is like a local version of the task decorator, not sure if there's any useful shared logic
def task_cache(key):
    def decorator_task_cache(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            task_id = get_parent_task_id()

            if check_exists_task_cache(task_id, key):
                return get_task_cache(task_id, key)
            else:
                value = func(*args, **kwargs)
                set_task_cache(task_id, key, value)
                return value

        return wrapper

    return decorator_task_cache
