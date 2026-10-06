"""领域模型：任务状态、任务实体与合法状态流转表。"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    """任务状态。

    - QUEUED:    排队中（首次提交，或重试退避结束后重新入队）
    - RUNNING:   执行中（已被某个执行单元租约锁定）
    - SUCCEEDED: 成功（终态）
    - FAILED:    可重试失败，退避等待中（非终态，仍会被再次调度）
    - DEAD:      死信（终态：重试耗尽或命中不可重试错误，保留失败原因）
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD = "dead"


TERMINAL_STATUSES = frozenset({TaskStatus.SUCCEEDED, TaskStatus.DEAD})

#: 合法的单调状态流转表；不在表中的流转一律拒绝，杜绝状态回退。
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.QUEUED: frozenset(
        {TaskStatus.RUNNING}
    ),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.DEAD}
    ),
    TaskStatus.FAILED: frozenset(
        {TaskStatus.RUNNING}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.DEAD: frozenset(),
}


@dataclass
class StatusEvent:
    """状态流转事件，用于让任务的结论可解释、可追溯。"""

    at: float
    attempt: int
    from_status: str | None
    to_status: str
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "at": self.at,
            "attempt": self.attempt,
            "from": self.from_status,
            "to": self.to_status,
            "message": self.message,
        }


@dataclass
class Task:
    """任务实体。"""

    id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any]
    max_attempts: int
    status: TaskStatus = TaskStatus.QUEUED
    attempt: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    available_at: float = field(default_factory=time.time)
    lease_owner: str | None = None
    lease_expires_at: float | None = None
    last_error: str | None = None
    result: Any = None
    history: list[StatusEvent] = field(default_factory=list)

    @staticmethod
    def new(
        idempotency_key: str,
        task_type: str,
        payload: dict[str, Any],
        max_attempts: int,
    ) -> "Task":
        now = time.time()
        task = Task(
            id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            task_type=task_type,
            payload=payload,
            max_attempts=max_attempts,
            created_at=now,
            updated_at=now,
            available_at=now,
        )
        task.history.append(
            StatusEvent(now, 0, None, TaskStatus.QUEUED.value, "任务已受理并入队")
        )
        return task

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "idempotency_key": self.idempotency_key,
            "task_type": self.task_type,
            "payload": self.payload,
            "status": self.status.value,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "available_at": self.available_at,
            "lease_owner": self.lease_owner,
            "lease_expires_at": self.lease_expires_at,
            "last_error": self.last_error,
            "result": self.result,
            "history": [e.to_dict() for e in self.history],
        }
