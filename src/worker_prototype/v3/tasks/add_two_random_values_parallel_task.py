import random
import uuid
from worker_prototype.v3.task_wrapper import run_in_parallel, async_task, task_cache

from worker_prototype.v3.tasks.fetch_value_task import fetch_value_task


# NOTE: ideally there would be a better way to handle these helper functions
@task_cache(key="k1")
def get_first_random_number():
    return random.randint(1, 4)


@task_cache(key="k2")
def get_second_random_number():
    return random.randint(1, 4)


# I generate a random id for these because they don't take any arguments in and we expect the value to be different each time
@async_task(id_generator=lambda *args, **kwargs: str(uuid.uuid4()))
def add_two_random_values_parallel_task():
    key1 = get_first_random_number()
    key2 = get_second_random_number()
    [r1, r2] = run_in_parallel(
        [
            lambda: fetch_value_task(key=f"v{key1}"),
            lambda: fetch_value_task(key=f"v{key2}"),
        ]
    )
    return r1 + r2
