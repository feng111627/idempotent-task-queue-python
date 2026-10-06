"""任务服务：受理校验、幂等提交、查询。"""
from __future__ import annotations

import json
import uuid
from typing import Any

from .models import (IdempotencyConflictError, InvalidPayloadError,
                     SubmitOutcome, Task)
from .store import TaskStore
from .worker import TaskQueue

_MAX_KEY_LEN = 128
_MAX_PAYLOAD_BYTES = 64 * 1024


class TaskService:
    def __init__(self, store: TaskStore, queue: TaskQueue) -> None:
        self._store = store
        self._queue = queue

    async def submit(self, idempotency_key: str, payload: dict[str, Any],
                     max_attempts: int = 3) -> SubmitOutcome:
        """幂等受理。

        - 参数非法：InvalidPayloadError（原因可区分）；
        - 键已存在且载荷一致：返回已有任务，created=False；
        - 键已存在但载荷不同：IdempotencyConflictError，不静默接受。
        """
        self._validate(idempotency_key, payload, max_attempts)

        task = Task(task_id=uuid.uuid4().hex,
                    idempotency_key=idempotency_key,
                    payload=payload,
                    max_attempts=max_attempts)
        stored, created = await self._store.get_or_create(task)
        if not created:
            if stored.payload != payload:
                raise IdempotencyConflictError(
                    f"幂等键 {idempotency_key!r} 已关联不同的任务载荷")
            return SubmitOutcome(task=stored, created=False)
        await self._queue.enqueue(stored.task_id)
        return SubmitOutcome(task=stored, created=True)

    async def get(self, task_id: str) -> Task | None:
        return await self._store.get(task_id)

    @staticmethod
    def _validate(idempotency_key: str, payload: dict[str, Any],
                  max_attempts: int) -> None:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise InvalidPayloadError("idempotency_key 必须是非空字符串")
        if len(idempotency_key) > _MAX_KEY_LEN:
            raise InvalidPayloadError(
                f"idempotency_key 长度超过 {_MAX_KEY_LEN} 字符")
        if not isinstance(payload, dict):
            raise InvalidPayloadError("payload 必须是 JSON 对象")
        try:
            size = len(json.dumps(payload, ensure_ascii=False))
        except (TypeError, ValueError) as exc:
            raise InvalidPayloadError(f"payload 必须可 JSON 序列化: {exc}") from exc
        if size > _MAX_PAYLOAD_BYTES:
            raise InvalidPayloadError(
                f"payload 超过 {_MAX_PAYLOAD_BYTES} 字节")
        if not isinstance(max_attempts, int) or max_attempts < 1:
            raise InvalidPayloadError("max_attempts 必须是 >= 1 的整数")
