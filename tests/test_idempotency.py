"""场景一：任务提交与幂等受理。"""

from __future__ import annotations

import asyncio

import pytest

from idempotent_queue.errors import IdempotencyConflictError
from idempotent_queue.models import TaskStatus


class Test幂等受理:
    def test_首次提交返回新建标记(self, make_queue, case):
        case("幂等受理", "首次提交返回 created=True", "受理成功且 created=True")
        q = make_queue({"ok": lambda p: {"r": 1}})
        task, created = q.submit(idempotency_key="key-1", task_type="ok")
        assert created is True
        assert task.status is TaskStatus.QUEUED

    async def test_重复提交命中已有任务且只执行一次(self, make_queue, drain, case):
        case("幂等受理", "重复提交命中", "created=False、同一任务、只执行一次")
        calls = {"n": 0}

        def handler(p):
            calls["n"] += 1
            return {"n": calls["n"]}

        q = make_queue({"ok": handler})
        t1, c1 = q.submit(idempotency_key="dup-key", task_type="ok", payload={"a": 1})
        t2, c2 = q.submit(idempotency_key="dup-key", task_type="ok", payload={"a": 1})
        t3, c3 = q.submit(idempotency_key="dup-key", task_type="ok", payload={"a": 1})

        assert c1 is True and c2 is False and c3 is False
        assert t1.id == t2.id == t3.id
        assert await drain(q)
        done = q.get(t1.id)
        assert done.status is TaskStatus.SUCCEEDED
        assert calls["n"] == 1, "幂等命中不得触发重复执行"

    async def test_并发提交同一幂等键只有一个生效(self, make_queue, drain, case):
        case("幂等受理", "并发提交", "多线程并发下仅新建一个任务")
        q = make_queue({"ok": lambda p: "ok"})

        def submit():
            return q.submit(idempotency_key="race", task_type="ok", payload={})

        results = await asyncio.to_thread(
            lambda: [submit() for _ in range(20)]
        )
        created = [tid for tid, c in results if c]
        ids = {tid.id for tid, _ in results}
        assert len(created) == 1
        assert len(ids) == 1

    def test_同键不同任务类型返回冲突(self, make_queue, case):
        case("幂等受理", "任务类型冲突", "抛 IdempotencyConflictError，不静默接受")
        q = make_queue({"a": lambda p: 1, "b": lambda p: 2})
        q.submit(idempotency_key="k", task_type="a")
        with pytest.raises(IdempotencyConflictError) as ei:
            q.submit(idempotency_key="k", task_type="b")
        assert "冲突" in str(ei.value)

    def test_同键不同参数返回冲突(self, make_queue, case):
        case("幂等受理", "payload 冲突", "抛 IdempotencyConflictError")
        q = make_queue({"a": lambda p: 1})
        q.submit(idempotency_key="k", task_type="a", payload={"v": 1})
        with pytest.raises(IdempotencyConflictError):
            q.submit(idempotency_key="k", task_type="a", payload={"v": 2})

    async def test_重复提交在任务成功后仍返回一致结论(self, make_queue, drain, case):
        case("幂等受理", "终态后重复提交", "命中同一已完成任务，结果一致")
        q = make_queue({"ok": lambda p: {"final": True}})
        t1, c1 = q.submit(idempotency_key="once", task_type="ok")
        assert await drain(q)
        t2, c2 = q.submit(idempotency_key="once", task_type="ok")
        assert c2 is False and t2.id == t1.id
        assert q.get(t1.id).status is TaskStatus.SUCCEEDED
        assert q.get(t1.id).result == {"final": True}
