"""任务领域模型与错误类型定义（骨架：仅类型定义）。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TaskStatus(str, enum.Enum):
    """任务状态机：QUEUED -> RUNNING -> SUCCEEDED / FAILED(可重试回 QUEUED) / DEAD。"""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"      # 一次尝试失败，等待重试（会重新回到 QUEUED）
    DEAD = "dead"          # 死信：重试耗尽或不可重试失败


# 允许的状态流转（单调、可解释）
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.QUEUED: frozenset({TaskStatus.RUNNING, TaskStatus.DEAD}),
    TaskStatus.RUNNING: frozenset({TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.DEAD, TaskStatus.QUEUED}),
    TaskStatus.FAILED: frozenset({TaskStatus.QUEUED, TaskStatus.DEAD}),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.DEAD: frozenset(),
}


class SubmitError(Exception):
    """提交被拒绝的基类，携带可区分的原因码。"""

    code = "submit_error"


class InvalidPayloadError(SubmitError):
    code = "invalid_payload"


class IdempotencyConflictError(SubmitError):
    code = "idempotency_conflict"


class RetryableError(Exception):
    """任务处理器抛出的可重试失败。"""


class FatalError(Exception):
    """任务处理器抛出的不可重试失败，直接进入死信。"""


@dataclass
class Task:
    task_id: str
    idempotency_key: str
    payload: dict[str, Any]
    status: TaskStatus = TaskStatus.QUEUED
    attempts: int = 0
    max_attempts: int = 3
    last_error: str | None = None
    result: Any = None
    version: int = 0  # 乐观锁，每次状态流转 +1
    created_at: float = 0.0
    updated_at: float = 0.0
    run_token: str | None = None  # 当前执行租约令牌，防止过期执行者写回


@dataclass
class SubmitOutcome:
    task: Task
    created: bool  # True=新建任务，False=命中已有幂等键
