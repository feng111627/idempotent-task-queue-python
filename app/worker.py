"""后台执行器：worker 池、指数退避重试、死信与租约超时回收。"""
from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable

from .models import FatalError, Task, TaskStatus
from .store import TaskStore

Handler = Callable[[Task], Awaitable[object]]

logger = logging.getLogger("taskqueue.worker")


class TaskQueue:
    """异步任务队列：提交即受理，后台 worker 池执行。

    一致性约定：
    - 同一任务同一时刻至多一个在途执行（_pending 去重 + 租约 CAS）；
    - 执行前获取租约（QUEUED->RUNNING，写入 run_token），
      只有持有当前 token 的执行者才能写回结果，超时/中断不会产生脏写；
    - 可重试失败按 base * 2^(attempts-1) 指数退避后重新入队；
      重试耗尽或 FatalError 进入 DEAD 并保留失败原因。
    """

    def __init__(self, store: TaskStore, handler: Handler, *,
                 workers: int = 4,
                 base_backoff: float = 0.05,
                 attempt_timeout: float = 5.0) -> None:
        self._store = store
        self._handler = handler
        self._workers = workers
        self._base_backoff = base_backoff
        self._attempt_timeout = attempt_timeout
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._pending: set[str] = set()  # 已入队但未开始执行的任务，防重复入队
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._retry_tasks: set[asyncio.Task[None]] = set()
        self._stopped = False

    async def enqueue(self, task_id: str) -> None:
        """入队；同一任务在途时重复入队会被去重。"""
        if task_id in self._pending:
            return
        self._pending.add(task_id)
        await self._queue.put(task_id)

    async def start(self) -> None:
        self._stopped = False
        self._worker_tasks = [
            asyncio.create_task(self._worker_loop(i), name=f"worker-{i}")
            for i in range(self._workers)
        ]

    async def stop(self) -> None:
        self._stopped = True
        for t in self._worker_tasks:
            t.cancel()
        for t in self._retry_tasks:
            t.cancel()
        await asyncio.gather(*self._worker_tasks, *self._retry_tasks,
                             return_exceptions=True)
        self._worker_tasks.clear()
        self._retry_tasks.clear()

    async def _worker_loop(self, idx: int) -> None:
        while True:
            task_id = await self._queue.get()
            self._pending.discard(task_id)
            try:
                await self._run_once(task_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("worker-%d 执行任务 %s 时出现未预期错误", idx, task_id)
            finally:
                self._queue.task_done()

    async def _run_once(self, task_id: str) -> None:
        task = await self._store.get(task_id)
        if task is None or task.status is not TaskStatus.QUEUED:
            return  # 已被其他执行单元处理或已终结，跳过

        token = uuid.uuid4().hex
        leased = await self._store.transition(
            task_id, task.version, TaskStatus.RUNNING,
            attempts=task.attempts + 1, run_token=token)
        if leased is None:
            return  # 租约竞争失败，其他执行者已接管

        attempt = leased.attempts
        try:
            result = await asyncio.wait_for(
                self._handler(leased), timeout=self._attempt_timeout)
        except FatalError as exc:
            await self._writeback(task_id, token, TaskStatus.DEAD,
                                  last_error=f"fatal: {exc}")
            logger.info("任务 %s 不可重试失败，进入死信: %s", task_id, exc)
            return
        except Exception as exc:  # noqa: BLE001 — 可重试失败（含超时）
            reason = (f"timeout after {self._attempt_timeout}s"
                      if isinstance(exc, asyncio.TimeoutError) else str(exc))
            if attempt >= leased.max_attempts:
                await self._writeback(task_id, token, TaskStatus.DEAD,
                                      last_error=f"attempts exhausted: {reason}")
                logger.info("任务 %s 重试耗尽(%d次)，进入死信: %s",
                            task_id, attempt, reason)
            else:
                await self._writeback(task_id, token, TaskStatus.FAILED,
                                      last_error=reason)
                self._schedule_retry(task_id, attempt)
                logger.info("任务 %s 第 %d 次尝试失败，将退避重试: %s",
                            task_id, attempt, reason)
            return

        done = await self._writeback(task_id, token, TaskStatus.SUCCEEDED,
                                     result=result, last_error=None)
        if done:
            logger.info("任务 %s 执行成功", task_id)

    async def _writeback(self, task_id: str, token: str,
                         to: TaskStatus, **updates: object) -> Task | None:
        """仅当仍持有租约 token 时写回，防止过期执行者覆盖新状态。"""
        task = await self._store.get(task_id)
        if task is None or task.run_token != token:
            logger.warning("任务 %s 写回被拒绝：租约已易主", task_id)
            return None
        updates.setdefault("run_token", None)
        return await self._store.transition(task_id, task.version, to, **updates)

    def _schedule_retry(self, task_id: str, attempt: int) -> None:
        delay = self._base_backoff * (2 ** (attempt - 1))

        async def _retry() -> None:
            await asyncio.sleep(delay)
            if self._stopped:
                return
            task = await self._store.get(task_id)
            if task is None or task.status is not TaskStatus.FAILED:
                return
            queued = await self._store.transition(
                task_id, task.version, TaskStatus.QUEUED)
            if queued is not None:
                await self.enqueue(task_id)

        t = asyncio.create_task(_retry(), name=f"retry-{task_id}")
        self._retry_tasks.add(t)
        t.add_done_callback(self._retry_tasks.discard)
