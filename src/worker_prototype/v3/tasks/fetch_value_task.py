# Fetch value from hashmap task
from worker_prototype.v3.db import (
    mock_info_store,
)
from worker_prototype.v3.task_wrapper import async_task


@async_task()
def fetch_value_task(key):
    value = mock_info_store[key]
    return value
