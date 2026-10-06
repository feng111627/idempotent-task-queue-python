"""场景三：失败重试与死信 —— 退避可解释，重试耗尽/不可重试进死信。"""
import asyncio
import logging

from app.models import FatalError, RetryableError, TaskStatus
from tests.conftest import wait_status

logger = logging.getLogger("test.retry")


async def test_可重试失败按退避重试后成功(env):
    service, _, spy = env
    attempts = 0

    def behavior(task, n):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RetryableError(f"第 {attempts} 次失败")
        return {"recovered": True}

    spy.behavior = behavior
    outcome = await service.submit("key-flaky", {}, max_attempts=3)
    task = await wait_status(service, outcome.task.task_id, {TaskStatus.SUCCEEDED})
    assert task.attempts == 3
    assert task.result == {"recovered": True}
    logger.info("失败 2 次后第 3 次成功: attempts=%d status=%s ✓",
                task.attempts, task.status)


async def test_重试耗尽进入死信并保留原因(env):
    service, _, spy = env

    def behavior(task, n):
        raise RetryableError("一直失败")

    spy.behavior = behavior
    outcome = await service.submit("key-doomed", {}, max_attempts=3)
    task = await wait_status(service, outcome.task.task_id, {TaskStatus.DEAD})
    assert task.attempts == 3
    assert "一直失败" in task.last_error
    assert "exhausted" in task.last_error
    await asyncio.sleep(0.1)
    assert len(spy.calls) == 3  # 进入死信后不再重试
    logger.info("重试耗尽: attempts=%d status=DEAD last_error=%r ✓",
                task.attempts, task.last_error)


async def test_不可重试失败直接进死信不重试(env):
    service, _, spy = env

    def behavior(task, n):
        raise FatalError("参数语义错误")

    spy.behavior = behavior
    outcome = await service.submit("key-fatal", {}, max_attempts=5)
    task = await wait_status(service, outcome.task.task_id, {TaskStatus.DEAD})
    assert task.attempts == 1
    assert "fatal" in task.last_error
    await asyncio.sleep(0.1)
    assert len(spy.calls) == 1
    logger.info("FatalError: 仅执行 1 次直接进死信，未反复重试 ✓")


async def test_执行超时被回收并按可重试处理(env):
    service, _, spy = env

    async def slow(task):
        await asyncio.sleep(10)

    service._queue._handler = slow
    outcome = await service.submit("key-slow", {}, max_attempts=2)
    task = await wait_status(service, outcome.task.task_id, {TaskStatus.DEAD})
    assert task.attempts == 2
    assert "timeout" in task.last_error
    logger.info("超时任务: attempts=%d 进入死信，未卡死 ✓", task.attempts)
