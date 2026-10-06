"""HTTP 接口层：任务提交与查询。"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Response
from pydantic import BaseModel, Field

from .models import (IdempotencyConflictError, InvalidPayloadError, Task,
                     TaskStatus)
from .service import TaskService


class SubmitRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1)


class TaskView(BaseModel):
    task_id: str
    idempotency_key: str
    status: TaskStatus
    attempts: int
    max_attempts: int
    last_error: str | None
    result: Any = None


def _view(task: Task) -> TaskView:
    return TaskView(task_id=task.task_id,
                    idempotency_key=task.idempotency_key,
                    status=task.status,
                    attempts=task.attempts,
                    max_attempts=task.max_attempts,
                    last_error=task.last_error,
                    result=task.result)


def create_app(service: TaskService) -> FastAPI:
    app = FastAPI(title="idempotent-task-queue")

    @app.post("/tasks", status_code=201)
    async def submit(req: SubmitRequest, response: Response) -> dict[str, Any]:
        try:
            outcome = await service.submit(req.idempotency_key, req.payload,
                                           req.max_attempts)
        except InvalidPayloadError as exc:
            response.status_code = 422
            return {"error": {"code": exc.code, "message": str(exc)}}
        except IdempotencyConflictError as exc:
            response.status_code = 409
            return {"error": {"code": exc.code, "message": str(exc)}}
        if not outcome.created:
            response.status_code = 200
        return {"created": outcome.created, "task": _view(outcome.task).model_dump()}

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str, response: Response) -> dict[str, Any]:
        task = await service.get(task_id)
        if task is None:
            response.status_code = 404
            return {"error": {"code": "not_found", "message": "任务不存在"}}
        return {"task": _view(task).model_dump()}

    return app
