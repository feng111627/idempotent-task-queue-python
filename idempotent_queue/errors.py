"""错误类型定义。

所有可区分的失败原因都使用独立的异常类型与 ``code``，
接口层据此映射为不同的 HTTP 状态码与错误码，绝不静默接受。
"""

from __future__ import annotations


class TaskQueueError(Exception):
    """任务队列错误基类。"""

    code: str = "task_queue_error"
    http_status: int = 400

    def __init__(self, message: str = "") -> None:
        super().__init__(message or self.code)
        self.message = message or self.code


class InvalidTaskError(TaskQueueError):
    """请求参数非法（空幂等键、空任务类型、payload 类型错误等）。"""

    code = "invalid_task"
    http_status = 422


class IdempotencyConflictError(TaskQueueError):
    """同一幂等键已存在任务，但本次提交的类型或参数与之冲突。"""

    code = "idempotency_conflict"
    http_status = 409


class TaskNotFoundError(TaskQueueError):
    """查询的任务不存在。"""

    code = "task_not_found"
    http_status = 404
