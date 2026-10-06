"""后台执行单元。

每个 :class:`Worker` 周期性地从存储中原子领取到期任务，执行处理器，并依据
结果做状态写回：

成功：     RUNNING -> SUCCEEDED
可重试失败：RUNNING -> FAILED（按指数退避设置 available_at，等待重新领取）
不可重试：  RUNNING -> DEAD（立即死信，不反复重试）
重试耗尽：  RUNNING -> DEAD（保留 last_error）

执行期间定时续租；若租约已被其他单元接管（例如本单元卡顿导致租约过期），
心跳会检测到并取消正在执行的处理器，最终写回也会被 CAS 拒绝，
保证过期执行者的结果不会重复生效。
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import itertools
import logging
import time
import uuid
from typing import Any, Awaitable, Callable

from .models import Task, TaskStatus
from .retry import RetryPolicy, is_retryable
from .store import ConcurrentStateError, TaskStore

logger = logging.getLogger("idempotent_queue.worker")

Handler = Callable[[dict[str, Any]], Any]


class Worker:
    """轮询式后台执行单元。"""

    def __init__(
        self,
        store: TaskStore,
        handlers: dict[str, Handler],
        retry_policy: RetryPolicy | None = None,
        *,
        poll_interval: float = 0.02,
        lease_seconds: float = 5.0,
        execution_timeout: float = 30.0,
        name: str | None = None,
    ) -> None:
        self._store = store
        self._handlers = handlers
        self._policy = retry_policy or RetryPolicy()
        self._poll_interval = poll_interval
        self._lease_seconds = lease_seconds
        self._execution_timeout = execution_timeout
        self.owner = f"worker-{name or uuid.uuid4().hex[:8]}"
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    # ---- 生命周期 -------------------------------------------------------

    def start(self) -> None:
        if self._task is None:
            self._stopping.clear()
            self._task = asyncio.create_task(self._run(), name=self.owner)
            logger.info("%s 已启动", self.owner)

    async def stop(self, timeout: float = 5.0) -> None:
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout)
            self._task = None
            logger.info("%s 已停止", self.owner)

    async def run_once(self) -> bool:
        """尝试领取并处理一个任务；返回是否处理了任务。供测试驱动。"""
        task = self._store.claim_due(self._lease_seconds, self.owner)
        if task is None:
            return False
        await self._execute(task)
        return True

    # ---- 主循环 ---------------------------------------------------------

    async def _run(self) -> None:
        try:
            while not self._stopping.is_set():
                handled = await self.run_once()
                if not handled:
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            self._stopping.wait(), timeout=self._poll_interval
                        )
        except asyncio.CancelledError:
            raise

    # ---- 执行与写回 -----------------------------------------------------

    async def _execute(self, task: Task) -> None:
        handler = self._handlers.get(task.task_type)
        if handler is None:
            # 未注册的任务类型属于确定性错误，重试无意义，直接死信。
            self._fail(
                task,
                RuntimeError(f"未注册的任务类型: {task.task_type}"),
                retryable=False,
            )
            return
        try:
            result = await self._run_with_lease(task, handler)
        except asyncio.TimeoutError:
            self._fail(
                task,
                TimeoutError(f"执行超过 {self._execution_timeout}s 超时"),
                retryable=True,
            )
            return
        except asyncio.CancelledError:
            # 被关停或租约丢失而取消：不做任何写回，交由租约恢复机制重新调度。
            logger.warning("%s 执行被取消: %s", self.owner, task.id)
            raise
        except BaseException as exc:  # noqa: BLE001 - 处理器异常必须被兜底分类
            self._fail(task, exc, retryable=is_retryable(exc))
            return
        self._succeed(task, result)

    async def _run_with_lease(self, task: Task, handler: Handler) -> Any:
        runner = self._invoke(handler, task.payload)
        exec_task = asyncio.ensure_future(runner)
        heartbeat = asyncio.ensure_future(self._heartbeat(task.id, exec_task))
        try:
            return await asyncio.wait_for(exec_task, timeout=self._execution_timeout)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _invoke(self, handler: Handler, payload: dict[str, Any]) -> Any:
        if inspect.iscoroutinefunction(handler):
            return await handler(payload)
        return await asyncio.to_thread(handler, payload)

    async def _heartbeat(self, task_id: str, exec_task: asyncio.Task[Any]) -> None:
        """定时续租；发现租约被接管时取消执行，防止过期执行者继续生效。"""
        interval = max(self._lease_seconds / 2, 0.001)
        try:
            for _ in itertools.count():
                await asyncio.sleep(interval)
                self._renew(task_id)
        except (ConcurrentStateError, LookupError):
            logger.error("%s 检测到租约丢失，取消任务 %s 的执行", self.owner, task_id)
            exec_task.cancel()

    def _renew(self, task_id: str) -> None:
        def do(task: Task) -> None:
            if task.lease_owner != self.owner or task.status is not TaskStatus.RUNNING:
                raise ConcurrentStateError("租约已不属于本执行单元")
            task.lease_expires_at = time.time() + self._lease_seconds

        self._store.transition(
            task_id,
            TaskStatus.RUNNING,
            expect=TaskStatus.RUNNING,
            lease_owner=self.owner,
            message=f"{self.owner} 续租",
            mutate=do,
        )

    def _succeed(self, task: Task, result: Any) -> None:
        try:
            self._store.transition(
                task.id,
                TaskStatus.SUCCEEDED,
                expect=TaskStatus.RUNNING,
                lease_owner=self.owner,
                message="执行成功",
                mutate=lambda t: (
                    setattr(t, "result", result),
                    setattr(t, "last_error", None),
                    setattr(t, "lease_owner", None),
                    setattr(t, "lease_expires_at", None),
                    setattr(t, "available_at", time.time()),
                ),
            )
            logger.info("%s 任务成功: %s", self.owner, task.id)
        except ConcurrentStateError:
            logger.warning("%s 的成功结果因租约竞争被丢弃: %s", self.owner, task.id)

    def _fail(self, task: Task, exc: BaseException, *, retryable: bool) -> None:
        message = f"{type(exc).__name__}: {exc}"
        attempts_used = self._store.get(task.id).attempt
        max_attempts = task.max_attempts
        if retryable and attempts_used < max_attempts:
            delay = self._policy.delay_for(attempts_used)

            def schedule_retry(t: Task) -> None:
                t.last_error = message
                t.available_at = time.time() + delay
                t.lease_owner = None
                t.lease_expires_at = None

            self._store.transition(
                task.id,
                TaskStatus.FAILED,
                expect=TaskStatus.RUNNING,
                lease_owner=self.owner,
                message=f"可重试失败，{delay:.3f}s 后第 {attempts_used + 1} 次重试",
                mutate=schedule_retry,
            )
            logger.warning(
                "%s 任务失败将重试(%s/%s): %s -> %s",
                self.owner, attempts_used, max_attempts, task.id, message,
            )
        else:
            reason = (
                "重试次数已耗尽" if retryable else "命中不可重试错误"
            )

            def to_dead(t: Task) -> None:
                t.last_error = message
                t.lease_owner = None
                t.lease_expires_at = None
                t.available_at = time.time()

            self._store.transition(
                task.id,
                TaskStatus.DEAD,
                expect=TaskStatus.RUNNING,
                lease_owner=self.owner,
                message=f"进入死信：{reason}",
                mutate=to_dead,
            )
            logger.error(
                "%s 任务进入死信(%s): %s -> %s",
                self.owner, reason, task.id, message,
            )
