"""HTTP 接口层（FastAPI）。

接口：

- ``POST /tasks``         幂等提交任务
- ``GET  /tasks/{id}``    查询任务（含状态流转 history）
- ``GET  /health``        健康检查

错误一律返回结构化 JSON ``{"error": {"code", "message"}}``，不同原因
对应不同 HTTP 状态码与错误码，不会被静默接受。
"""

from __future__ import annotations

import contextlib
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import TaskQueueError
from .queue import TaskQueue


class SubmitTaskRequest(BaseModel):
    idempotency_key: str = Field(..., description="幂等键，同一键重复提交不重复执行")
    task_type: str = Field(..., description="任务类型，需与已注册处理器对应")
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, description="本任务最大执行次数")


def create_app(queue: TaskQueue | None = None, *, start_workers: bool = True) -> FastAPI:
    queue = queue or TaskQueue()
    app = FastAPI(title="幂等异步任务队列", version="0.1.0")
    app.state.queue = queue

    if start_workers:
        @contextlib.asynccontextmanager
        async def lifespan(app: FastAPI):
            queue.start()
            try:
                yield
            finally:
                await queue.stop()

        app.router.lifespan_context = lifespan

    @app.exception_handler(TaskQueueError)
    async def handle_task_queue_error(_: Request, exc: TaskQueueError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    @app.post("/tasks", status_code=202)
    async def submit_task(req: SubmitTaskRequest) -> dict[str, Any]:
        task, created = queue.submit(
            idempotency_key=req.idempotency_key,
            task_type=req.task_type,
            payload=req.payload,
            max_attempts=req.max_attempts,
        )
        return {
            "accepted": True,
            "created": created,
            "task": task.to_dict(),
        }

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict[str, Any]:
        return queue.get(task_id).to_dict()

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
