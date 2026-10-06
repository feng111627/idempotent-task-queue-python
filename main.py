"""本地启动入口：uv run python main.py

注册了一个示例任务类型 ``echo``，可通过 POST /tasks 提交。
"""

from __future__ import annotations

import uvicorn

from idempotent_queue.api import create_app
from idempotent_queue.queue import TaskQueue
from idempotent_queue.retry import TaskFailure

queue = TaskQueue(worker_count=2, poll_interval=0.02, lease_seconds=5.0, execution_timeout=30.0)


@queue.handler("echo")
def echo(payload: dict) -> dict:
    """示例处理器：原样返回 payload；传入 fail=true 可演示失败与死信。"""
    if payload.get("fail") == "fatal":
        raise TaskFailure("确定性业务错误，不可重试", retryable=False)
    if payload.get("fail"):
        raise RuntimeError("模拟瞬时失败")
    return {"echoed": payload}


app = create_app(queue)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
