"""场景一：幂等受理 —— 重复提交不重复执行，结果一致且可区分。"""
import asyncio
import logging

from app.models import TaskStatus
from tests.conftest import wait_status

logger = logging.getLogger("test.idempotency")


async def test_重复提交命中已有任务且只执行一次(env):
    service, _, spy = env
    first = await service.submit("key-1", {"n": 1})
    assert first.created is True
    logger.info("首次提交: created=True task_id=%s", first.task.task_id)

    for _ in range(3):
        again = await service.submit("key-1", {"n": 1})
        assert again.created is False
        assert again.task.task_id == first.task.task_id
    logger.info("重复提交 3 次: created=False 且 task_id 一致")

    task = await wait_status(service, first.task.task_id, {TaskStatus.SUCCEEDED})
    await asyncio.sleep(0.05)
    assert spy.calls.count(first.task.task_id) == 1
    logger.info("任务终态=%s，处理器仅执行 1 次 ✓", task.status)


async def test_并发提交同一幂等键只建一个任务(env):
    service, _, spy = env
    outcomes = await asyncio.gather(*[
        service.submit("key-race", {"n": 9}) for _ in range(20)
    ])
    created = [o for o in outcomes if o.created]
    assert len(created) == 1
    ids = {o.task.task_id for o in outcomes}
    assert len(ids) == 1
    task_id = ids.pop()
    await wait_status(service, task_id, {TaskStatus.SUCCEEDED})
    await asyncio.sleep(0.05)
    assert spy.calls.count(task_id) == 1
    logger.info("20 路并发提交: 仅 1 个新建、1 个 task_id、执行 1 次 ✓")


async def test_不同幂等键各自独立执行(env):
    service, _, spy = env
    outcomes = await asyncio.gather(*[
        service.submit(f"key-{i}", {"i": i}) for i in range(10)
    ])
    assert all(o.created for o in outcomes)
    for o in outcomes:
        await wait_status(service, o.task.task_id, {TaskStatus.SUCCEEDED})
    assert len(spy.calls) == 10
    logger.info("10 个不同键全部独立执行成功 ✓")
