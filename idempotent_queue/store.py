"""任务存储（内存实现）。

并发一致性约定：

- 一把 :class:`threading.RLock` 串行化所有状态读写，所有变更在锁内完成；
- 幂等键唯一索引保证同一幂等键最多创建一个任务（insert-if-absent）；
- 所有状态变更都必须经 :meth:`TaskStore.transition`，该方法用 CAS 语义
  校验前置状态、租约持有者与合法流转表，任何并发竞争中的"输家"都会得到
  ``False``，从而不会重复生效或让状态回退。
"""

from __future__ import annotations

import threading
from typing import Callable, TypeVar

from .errors import InvalidTaskError, TaskNotFoundError
from .models import ALLOWED_TRANSITIONS, StatusEvent, Task, TaskStatus

T = TypeVar("T")


class TaskStore:
    """线程安全的内存任务存储。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._by_id: dict[str, Task] = {}
        self._by_idem_key: dict[str, str] = {}

    # ---- 基础读取 -------------------------------------------------------

    def _get_locked(self, task_id: str) -> Task:
        task = self._by_id.get(task_id)
        if task is None:
            raise TaskNotFoundError(f"任务不存在: {task_id}")
        return task

    def get(self, task_id: str) -> Task:
        with self._lock:
            return self._get_locked(task_id)

    def get_optional(self, task_id: str) -> Task | None:
        with self._lock:
            return self._by_id.get(task_id)

    def get_by_idempotency_key(self, key: str) -> Task | None:
        with self._lock:
            task_id = self._by_idem_key.get(key)
            return self._by_id.get(task_id) if task_id else None

    def all_tasks(self) -> list[Task]:
        with self._lock:
            return list(self._by_id.values())

    # ---- 幂等创建 -------------------------------------------------------

    def create_if_absent(self, task: Task) -> bool:
        """幂等插入。

        返回 ``True`` 表示本次新建；``False`` 表示幂等键已存在（调用方应
        改为返回已有任务，而非再次创建）。
        """
        with self._lock:
            if task.idempotency_key in self._by_idem_key:
                return False
            self._by_id[task.id] = task
            self._by_idem_key[task.idempotency_key] = task.id
            return True

    # ---- CAS 状态流转 ---------------------------------------------------

    def transition(
        self,
        task_id: str,
        target: TaskStatus,
        *,
        expect: TaskStatus | None = None,
        lease_owner: str | None = None,
        message: str = "",
        mutate: Callable[[Task], None] | None = None,
    ) -> Task:
        """在锁内执行 CAS 状态流转。

        - ``expect``：要求任务当前必须处于该状态，否则抛
          :class:`ConcurrentStateError`；
        - ``lease_owner````：若任务带有租约，则必须与持有者一致，
          防止超时的旧执行者写回结果；
        - 流转必须在 :data:`~idempotent_queue.models.ALLOWED_TRANSITIONS` 中。

        成功时追加 history 事件并更新 ``updated_at``，返回最新任务。
        """
        with self._lock:
            task = self._get_locked(task_id)
            if expect is not None and task.status is not expect:
                raise ConcurrentStateError(
                    f"状态竞争: 期望 {expect.value}，实际 {task.status.value}"
                )
            if task.lease_owner is not None and lease_owner is not None:
                if task.lease_owner != lease_owner:
                    raise ConcurrentStateError(
                        "租约已被其他执行单元接管，拒绝写回"
                    )
            if target is not task.status and target not in ALLOWED_TRANSITIONS[task.status]:
                raise InvalidTaskError(
                    f"非法状态流转: {task.status.value} -> {target.value}"
                )
            from_status = task.status
            if target is not from_status:
                task.status = target
            if mutate is not None:
                mutate(task)
            task.updated_at = _now()
            task.history.append(
                StatusEvent(
                    at=task.updated_at,
                    attempt=task.attempt,
                    from_status=from_status.value,
                    to_status=target.value,
                    message=message,
                )
            )
            return task

    # ---- 调度侧快照 -----------------------------------------------------

    def claim_due(self, lease_seconds: float, owner: str) -> Task | None:
        """原子领取一个到期、非终态且无有效租约的任务。

        QUEUED 直接可领取；FAILED 在 ``available_at`` 退避到期后可领取；
        RUNNING 仅在租约过期后可被接管（用于执行者崩溃/超时的恢复）。
        领取后进入 RUNNING 并持有新租约，attempt +1。
        """
        with self._lock:
            now = _now()
            for task in self._by_id.values():
                if task.is_terminal:
                    continue
                due = task.available_at <= now
                lease_expired = (
                    task.status is TaskStatus.RUNNING
                    and task.lease_expires_at is not None
                    and task.lease_expires_at <= now
                )
                if task.status is TaskStatus.RUNNING and not lease_expired:
                    continue
                if not due and not lease_expired:
                    continue
                old = task.status
                task.status = TaskStatus.RUNNING
                task.attempt += 1
                task.lease_owner = owner
                task.lease_expires_at = now + lease_seconds
                task.updated_at = now
                task.history.append(
                    StatusEvent(
                        at=now,
                        attempt=task.attempt,
                        from_status=old.value,
                        to_status=TaskStatus.RUNNING.value,
                        message=f"被执行单元 {owner} 领取"
                        + ("（接管过期租约）" if lease_expired else ""),
                    )
                )
                return task
            return None

    def heartbeat_due(self, now: float | None = None) -> list[str]:
        """返回当前存在过期租约风险的 RUNNING 任务 id（供监控/测试观察）。"""
        with self._lock:
            now = now if now is not None else _now()
            return [
                t.id
                for t in self._by_id.values()
                if t.status is TaskStatus.RUNNING
                and t.lease_expires_at is not None
                and t.lease_expires_at <= now
            ]


class ConcurrentStateError(InvalidTaskError):
    """CAS 失败：状态已被并发执行单元推进。"""

    code = "concurrent_state"
    http_status = 409


def _now() -> float:
    import time

    return time.time()
