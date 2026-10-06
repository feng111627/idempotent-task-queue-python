import asyncio

import pytest

from app.service import TaskService
from app.store import TaskStore
from app.worker import TaskQueue


class HandlerSpy:
    """记录调用次数、可编程行为的测试处理器。"""

    def __init__(self, behavior=None):
        self.calls: list[str] = []
        self.behavior = behavior or (lambda task, n: {"ok": True})
        self.started = asyncio.Event()

    async def __call__(self, task):
        self.calls.append(task.task_id)
        self.started.set()
        return self.behavior(task, len(self.calls))


@pytest.fixture
async def env():
    spy = HandlerSpy()
    store = TaskStore()
    queue = TaskQueue(store, spy, workers=4, base_backoff=0.01,
                      attempt_timeout=0.5)
    await queue.start()
    service = TaskService(store, queue)
    yield service, queue, spy
    await queue.stop()


async def wait_status(service, task_id, statuses, timeout=5.0):
    """轮询直到任务进入目标状态之一。"""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        task = await service.get(task_id)
        if task.status in statuses:
            return task
        await asyncio.sleep(0.01)
    raise AssertionError(f"任务 {task_id} 未在 {timeout}s 内进入 {statuses}")
