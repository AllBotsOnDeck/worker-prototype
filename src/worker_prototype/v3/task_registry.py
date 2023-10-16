# task registry
from worker_prototype.v3.db import get_task

import threading

from worker_prototype.v3.thread_util import set_task_id


class FunctionRegistry:
    def __init__(self):
        self._registry = {}

    def register(self, func, name, version):
        if (name, version) in self._registry:
            raise ValueError(
                f"Function {name} with version {version} already registered"
            )
        self._registry[(name, version)] = func

    def get(self, name, version):
        return self._registry[(name, version)]


function_registry = FunctionRegistry()


def function_runner(id):
    task = get_task(id)
    name = task.name
    version = task.version
    func = function_registry.get(name, version)
    set_task_id(id)
    func(**task.data)
