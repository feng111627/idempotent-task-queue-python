"""任务存储：内存实现，提供幂等索引与乐观锁状态流转。"""
from __future__ import annotations

import asyncio
import time

from .models import ALLOWED_TRANSITIONS, Task, TaskStatus


class TaskStore:
    """协程安全的任务存储。

    - 按 idempotency_key 唯一索引，保证同一键只建一个任务；
    - 状态流转走 CAS（检查 version），并发下不产生重复生效或状态回退；
    - 所有流转必须落在 ALLOWED_TRANSITIONS 内，保证单调可解释。
    """

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}
        self._by_key: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(self, task: Task) -> tuple[Task, bool]:
        """按幂等键获取或新建任务，返回 (task, created)。"""
        async with self._lock:
            existing_id = self._by_key.get(task.idempotency_key)
            if existing_id is not None:
                return self._tasks[existing_id], False
            now = time.monotonic()
            task.created_at = task.updated_at = now
            self._tasks[task.task_id] = task
            self._by_key[task.idempotency_key] = task.task_id
            return task, True

    async def get(self, task_id: str) -> Task | None:
        async with self._lock:
            return self._tasks.get(task_id)

    async def get_by_key(self, idempotency_key: str) -> Task | None:
        async with self._lock:
            task_id = self._by_key.get(idempotency_key)
            return self._tasks.get(task_id) if task_id else None

    async def transition(self, task_id: str, expect_version: int,
                         to: TaskStatus, **updates: object) -> Task | None:
        """CAS 状态流转：版本不匹配或流转非法时返回 None，表示竞争失败。"""
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task.version != expect_version:
                return None
            if to not in ALLOWED_TRANSITIONS[task.status]:
                return None
            task.status = to
            task.version += 1
            task.updated_at = time.monotonic()
            for key, value in updates.items():
                setattr(task, key, value)
            return task

    async def snapshot(self, task_id: str) -> Task | None:
        """返回任务的当前引用（内存实现下与 get 相同，供读取侧使用）。"""
        return await self.get(task_id)
