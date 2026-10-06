"""本地启动入口：python main.py 后访问 http://127.0.0.1:8000/docs"""
import asyncio
import logging

import uvicorn

from app.api import create_app
from app.models import RetryableError
from app.service import TaskService
from app.store import TaskStore
from app.worker import TaskQueue

logging.basicConfig(level=logging.INFO)


async def demo_handler(task):
    """示例处理器：payload 里 fail=true 时可重试失败，fatal=true 时不可重试。"""
    await asyncio.sleep(0.01)
    if task.payload.get("fatal"):
        from app.models import FatalError
        raise FatalError("演示不可重试失败")
    if task.payload.get("fail"):
        raise RetryableError("演示可重试失败")
    return {"echo": task.payload}


store = TaskStore()
queue = TaskQueue(store, demo_handler)
service = TaskService(store, queue)
app = create_app(service)


@app.on_event("startup")
async def _startup() -> None:
    await queue.start()


@app.on_event("shutdown")
async def _shutdown() -> None:
    await queue.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
