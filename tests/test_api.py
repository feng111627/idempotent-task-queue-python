"""场景五：HTTP 接口契约（状态码、错误码、幂等结论）。"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from idempotent_queue.api import create_app
from idempotent_queue.models import TaskStatus
from idempotent_queue.queue import TaskQueue
from idempotent_queue.retry import RetryPolicy, TaskFailure


@pytest.fixture
async def client():
    q = TaskQueue(
        RetryPolicy(max_attempts=2, base_delay=0.01, jitter=0.0),
        poll_interval=0.01,
    )

    @q.handler("echo")
    def echo(p):
        return {"got": p}

    @q.handler("boom")
    def boom(p):
        raise RuntimeError("服务端瞬时故障")

    @q.handler("fatal")
    def fatal(p):
        raise TaskFailure("不合法的业务操作", retryable=False)

    app = create_app(q, start_workers=False)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        c.queue = q  # type: ignore[attr-defined]
        yield c


class TestHTTP接口:
    async def test_提交返回202且标明created(self, client, case):
        case("HTTP 接口", "首次提交 201/202", "202 + accepted + created=true")
        resp = await client.post(
            "/tasks",
            json={"idempotency_key": "api-1", "task_type": "echo", "payload": {"x": 1}},
        )
        assert resp.status_code == 202
        body = resp.json()
        assert body["accepted"] is True
        assert body["created"] is True
        assert body["task"]["status"] == TaskStatus.QUEUED.value
        assert body["task"]["payload"] == {"x": 1}

    async def test_重复提交返回一致受理结果(self, client, case):
        case("HTTP 接口", "幂等命中", "created=false 且 task.id 相同")
        payload = {"idempotency_key": "api-dup", "task_type": "echo"}
        r1 = await client.post("/tasks", json=payload)
        r2 = await client.post("/tasks", json=payload)
        assert r1.status_code == r2.status_code == 202
        b1, b2 = r1.json(), r2.json()
        assert b1["created"] is True and b2["created"] is False
        assert b1["task"]["id"] == b2["task"]["id"]

    async def test_冲突请求返回409与专用错误码(self, client, case):
        case("HTTP 接口", "冲突 409", "error.code=idempotency_conflict")
        await client.post("/tasks", json={"idempotency_key": "api-conflict", "task_type": "echo"})
        resp = await client.post(
            "/tasks", json={"idempotency_key": "api-conflict", "task_type": "boom"}
        )
        assert resp.status_code == 409
        err = resp.json()["error"]
        assert err["code"] == "idempotency_conflict"
        assert "冲突" in err["message"]

    async def test_非法参数返回422且不被静默接受(self, client, case):
        case("HTTP 接口", "非法参数", "422 校验错误")
        # 缺少必填字段
        resp = await client.post("/tasks", json={"task_type": "echo"})
        assert resp.status_code == 422
        # 空幂等键走领域校验
        resp2 = await client.post(
            "/tasks", json={"idempotency_key": "  ", "task_type": "echo"}
        )
        assert resp2.status_code == 422
        assert resp2.json()["error"]["code"] == "invalid_task"

    async def test_查询接口返回完整状态流转(self, client, case):
        case("HTTP 接口", "查询任务", "含 status/attempt/history")
        r = await client.post(
            "/tasks", json={"idempotency_key": "api-query", "task_type": "echo"}
        )
        task_id = r.json()["task"]["id"]
        q: TaskQueue = client.queue  # type: ignore[attr-defined]
        assert await q.run_worker_once(0)
        resp = await client.get(f"/tasks/{task_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == TaskStatus.SUCCEEDED.value
        assert body["attempt"] == 1
        assert body["history"], "history 必须包含可解释的流转记录"
        assert body["history"][0]["to"] == "queued"

    async def test_查询不存在任务返回404(self, client, case):
        case("HTTP 接口", "404", "结构化错误码 task_not_found")
        resp = await client.get("/tasks/missing")
        assert resp.status_code == 404
        assert resp.json()["error"]["code"] == "task_not_found"

    async def test_死信任务可通过接口追溯失败原因(self, client, case):
        case("HTTP 接口", "死信可追溯", "DEAD + last_error + history")
        await client.post(
            "/tasks", json={"idempotency_key": "api-dead", "task_type": "fatal"}
        )
        q: TaskQueue = client.queue  # type: ignore[attr-defined]
        assert await q.run_worker_once(0)
        dead_task = q.store.get_by_idempotency_key("api-dead")
        assert dead_task is not None
        resp = await client.get(f"/tasks/{dead_task.id}")
        body = resp.json()
        assert body["status"] == TaskStatus.DEAD.value
        assert "不合法的业务操作" in body["last_error"]
        assert body["attempt"] == 1

    async def test_健康检查(self, client, case):
        case("HTTP 接口", "健康检查", "200 ok")
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
