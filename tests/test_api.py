"""场景五：HTTP 接口 —— 状态码与错误码可区分。"""
import asyncio
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import create_app
from app.service import TaskService
from app.store import TaskStore
from app.worker import TaskQueue

logger = logging.getLogger("test.api")


@pytest.fixture
async def client():
    async def handler(task):
        return {"echo": task.payload}

    store = TaskStore()
    queue = TaskQueue(store, handler, base_backoff=0.01)
    await queue.start()
    app = create_app(TaskService(store, queue))
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url="http://t") as c:
        yield c
    await queue.stop()


async def test_提交与幂等重放的状态码(client):
    r1 = await client.post("/tasks", json={"idempotency_key": "k1",
                                           "payload": {"a": 1}})
    assert r1.status_code == 201 and r1.json()["created"] is True
    r2 = await client.post("/tasks", json={"idempotency_key": "k1",
                                           "payload": {"a": 1}})
    assert r2.status_code == 200 and r2.json()["created"] is False
    assert r2.json()["task"]["task_id"] == r1.json()["task"]["task_id"]
    logger.info("POST /tasks: 首次 201 created=true，重放 200 created=false ✓")


async def test_冲突与非法输入的错误码(client):
    await client.post("/tasks", json={"idempotency_key": "k2",
                                      "payload": {"a": 1}})
    r = await client.post("/tasks", json={"idempotency_key": "k2",
                                          "payload": {"a": 2}})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"
    r = await client.post("/tasks", json={"idempotency_key": "k3",
                                          "max_attempts": 0})
    assert r.status_code == 422
    logger.info("冲突 -> 409 idempotency_conflict，非法输入 -> 422 ✓")


async def test_查询任务终态(client):
    r = await client.post("/tasks", json={"idempotency_key": "k4"})
    task_id = r.json()["task"]["task_id"]
    for _ in range(100):
        got = await client.get(f"/tasks/{task_id}")
        if got.json()["task"]["status"] == "succeeded":
            break
        await asyncio.sleep(0.01)
    assert got.json()["task"]["status"] == "succeeded"
    assert (await client.get("/tasks/nope")).status_code == 404
    logger.info("GET /tasks/{id}: 终态 succeeded，未知 id -> 404 ✓")
