"""任务队列门面。

负责：请求参数校验、幂等受理（区分新建/命中/冲突）、处理器注册以及
后台 worker 的生命周期管理。
"""

from __future__ import annotations

import contextlib
from typing import Any

from .errors import IdempotencyConflictError, InvalidTaskError
from .models import Task
from .retry import RetryPolicy
from .store import TaskStore
from .worker import Handler, Worker


class TaskQueue:
    """幂等异步任务队列。"""

    def __init__(
        self,
        retry_policy: RetryPolicy | None = None,
        *,
        worker_count: int = 1,
        poll_interval: float = 0.02,
        lease_seconds: float = 5.0,
        execution_timeout: float = 30.0,
    ) -> None:
        self._policy = retry_policy or RetryPolicy()
        self._store = TaskStore()
        self._handlers: dict[str, Handler] = {}
        self._workers: list[Worker] = []
        self._worker_kwargs = {
            "poll_interval": poll_interval,
            "lease_seconds": lease_seconds,
            "execution_timeout": execution_timeout,
        }
        for i in range(worker_count):
            self._workers.append(
                Worker(
                    self._store,
                    self._handlers,
                    self._policy,
                    name=str(i + 1),
                    **self._worker_kwargs,
                )
            )

    @property
    def store(self) -> TaskStore:
        return self._store

    # ---- 处理器注册 -----------------------------------------------------

    def handler(self, task_type: str) -> Any:
        """以装饰器方式注册处理器：``@queue.handler("email")``。"""

        def register(fn: Handler) -> Handler:
            self.register_handler(task_type, fn)
            return fn

        return register

    def register_handler(self, task_type: str, fn: Handler) -> None:
        self._handlers[task_type] = fn

    # ---- 提交与查询 -----------------------------------------------------

    def submit(
        self,
        *,
        idempotency_key: str,
        task_type: str,
        payload: dict[str, Any] | None = None,
        max_attempts: int | None = None,
    ) -> tuple[Task, bool]:
        """幂等提交任务。

        返回 ``(task, created)``：created=True 为新建，False 为命中已有任务。
        参数非法抛 :class:`InvalidTaskError`；同幂等键但任务类型/参数不一致
        抛 :class:`IdempotencyConflictError`。
        """
        key = self._validate_key(idempotency_key)
        if not isinstance(task_type, str) or not task_type.strip():
            raise InvalidTaskError("task_type 必须是非空字符串")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise InvalidTaskError("payload 必须是 JSON 对象")
        if max_attempts is None:
            max_attempts = self._policy.max_attempts
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
            raise InvalidTaskError("max_attempts 必须是 >= 1 的整数")

        existing = self._store.get_by_idempotency_key(key)
        if existing is not None:
            if existing.task_type != task_type:
                raise IdempotencyConflictError(
                    f"幂等键 {key} 已存在任务类型 {existing.task_type}，"
                    f"与本次提交的 {task_type} 冲突"
                )
            if existing.payload != payload:
                raise IdempotencyConflictError(
                    f"幂等键 {key} 已存在任务的 payload 与本次提交不一致"
                )
            return existing, False

        task = Task.new(key, task_type, payload, max_attempts)
        if self._store.create_if_absent(task):
            return task, True
        # 并发提交的输家：重读已有任务并走冲突/命中判断。
        existing = self._store.get_by_idempotency_key(key)
        assert existing is not None
        if existing.task_type != task_type or existing.payload != payload:
            raise IdempotencyConflictError(
                f"幂等键 {key} 在并发提交中已被不同参数的任务占用"
            )
        return existing, False

    def get(self, task_id: str) -> Task:
        return self._store.get(task_id)

    @staticmethod
    def _validate_key(key: str) -> str:
        if not isinstance(key, str):
            raise InvalidTaskError("idempotency_key 必须是字符串")
        key = key.strip()
        if not key:
            raise InvalidTaskError("idempotency_key 不能为空")
        if len(key) > 128:
            raise InvalidTaskError("idempotency_key 长度不能超过 128")
        return key

    # ---- worker 生命周期 ------------------------------------------------

    def start(self) -> None:
        for w in self._workers:
            w.start()

    async def stop(self) -> None:
        for w in self._workers:
            await w.stop()

    async def run_worker_once(self, index: int = 0) -> bool:
        """直接驱动某个 worker 处理一轮（测试与本地验证用）。"""
        return await self._workers[index].run_once()

    async def wait_until_drained(self, timeout: float = 5.0) -> bool:
        """等待所有非终态任务被处理完（退避中的任务也算未完成）。"""
        import asyncio

        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            with contextlib.suppress(Exception):
                tasks = self._store.all_tasks()
            if all(t.is_terminal for t in self._store.all_tasks()):
                return True
            await asyncio.sleep(0.01)
        return all(t.is_terminal for t in self._store.all_tasks())
