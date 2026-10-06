"""场景四：并发执行下的状态一致性 —— 不重复生效、不漏做、状态单调。"""
import asyncio
import logging

from app.models import TaskStatus
from tests.conftest import wait_status

logger = logging.getLogger("test.concurrency")

_ORDER = [TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.FAILED,
          TaskStatus.SUCCEEDED, TaskStatus.DEAD]


async def test_重复入队不会重复执行(env):
    """同一任务被并发入队多次，4 个 worker 竞争下也只执行一次。"""
    service, queue, spy = env
    outcome = await service.submit("key-dup", {})
    task_id = outcome.task.task_id
    await asyncio.gather(*[queue.enqueue(task_id) for _ in range(50)])
    await wait_status(service, task_id, {TaskStatus.SUCCEEDED})
    await asyncio.sleep(0.1)
    assert spy.calls.count(task_id) == 1
    logger.info("50 次并发入队 + 4 worker: 处理器仅执行 1 次 ✓")


async def test_批量并发任务全部恰好执行一次(env):
    service, _, spy = env
    outcomes = await asyncio.gather(*[
        service.submit(f"key-batch-{i}", {"i": i}) for i in range(50)
    ])
    for o in outcomes:
        await wait_status(service, o.task.task_id, {TaskStatus.SUCCEEDED})
    await asyncio.sleep(0.05)
    assert sorted(spy.calls) == sorted(o.task.task_id for o in outcomes)
    logger.info("50 个并发任务: 每个恰好执行一次，无漏做无重复 ✓")


async def test_状态流转单调不回退(env):
    """成功任务的状态历史单调前进，终结后不再变化。"""
    service, _, _ = env
    outcome = await service.submit("key-mono", {})
    task_id = outcome.task.task_id
    seen: list[TaskStatus] = []

    async def watch():
        for _ in range(200):
            task = await service.get(task_id)
            if not seen or task.status != seen[-1]:
                seen.append(task.status)
            if task.status in (TaskStatus.SUCCEEDED, TaskStatus.DEAD):
                return
            await asyncio.sleep(0.002)

    await watch()
    final = await service.get(task_id)
    assert final.status is TaskStatus.SUCCEEDED
    positions = [_ORDER.index(s) for s in seen]
    assert positions == sorted(positions), f"状态回退: {seen}"
    await asyncio.sleep(0.1)
    assert (await service.get(task_id)).status is TaskStatus.SUCCEEDED
    logger.info("状态历史 %s 单调前进，终结后稳定 ✓",
                [s.value for s in seen])
