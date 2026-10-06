"""场景三：并发下的状态一致性。"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from idempotent_queue.errors import InvalidTaskError
from idempotent_queue.models import Task, TaskStatus
from idempotent_queue.queue import TaskQueue
from idempotent_queue.retry import RetryPolicy
from idempotent_queue.store import ConcurrentStateError, TaskStore
from idempotent_queue.worker import Worker


class Test并发竞争:
    def test_多执行单元并发领取只有一个生效(self, case):
        case("并发竞争", "并发 claim", "20 个线程争抢同一任务，仅 1 个领到")
        store = TaskStore()
        task = Task.new("k", "ok", {}, 3)
        store.create_if_absent(task)
        winners: list[str] = []

        def claim(i: int) -> None:
            got = store.claim_due(lease_seconds=10, owner=f"w{i}")
            if got is not None:
                winners.append(f"w{i}")

        threads = [threading.Thread(target=claim, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(winners) == 1
        assert store.get(task.id).lease_owner == winners[0]

    async def test_租约过期后可被接管且任务最终完成(self, case):
        case("并发竞争", "崩溃恢复", "执行者中断后租约过期，另一单元接管成功")
        ran_by: list[str] = []

        def handler(_):
            ran_by.append("done")
            return {"recovered": True}

        q = TaskQueue(
            RetryPolicy(max_attempts=3, base_delay=0.01, jitter=0.0),
            worker_count=1,
            lease_seconds=0.05,
            poll_interval=0.005,
        )
        q.register_handler("ok", handler)
        t, _ = q.submit(idempotency_key="crash", task_type="ok")

        # 模拟某个执行单元领到任务后进程崩溃：什么也不做，等待租约过期
        crashed = Worker(q.store, q._handlers, lease_seconds=0.05, name="crashed")
        claimed = crashed._store.claim_due(0.05, crashed.owner)
        assert claimed is not None and claimed.status is TaskStatus.RUNNING

        # 租约未过期前，正常 worker 不能重复领取
        assert q.store.claim_due(0.05, "other") is None
        time.sleep(0.06)
        takeover = q.store.claim_due(0.05, "other")
        assert takeover is not None and takeover.lease_owner == "other"
        assert takeover.attempt == 2, "接管属于一次新的执行尝试"

        # 崩溃的旧持有者写回必须被拒绝，不能让旧结果生效
        with pytest.raises(ConcurrentStateError):
            q.store.transition(
                t.id,
                TaskStatus.SUCCEEDED,
                expect=TaskStatus.RUNNING,
                lease_owner=crashed.owner,
            )

        # 由接管者走完正常流程
        t_now = q.store.get(t.id)
        q.store.transition(
            t.id, TaskStatus.SUCCEEDED,
            expect=TaskStatus.RUNNING, lease_owner="other",
            mutate=lambda x: setattr(x, "result", {"recovered": True}),
        )
        assert q.get(t.id).status is TaskStatus.SUCCEEDED

    async def test_租约丢失时心跳取消正在执行的处理器(self, case):
        case("并发竞争", "租约丢失取消", "执行被 cancel，过期执行者不产生效果")
        q = TaskQueue(lease_seconds=0.1, execution_timeout=5.0)
        q.register_handler("ok", lambda p: "never")
        t, _ = q.submit(idempotency_key="lease-lost", task_type="ok")
        worker = q._workers[0]
        claimed = q.store.claim_due(0.1, worker.owner)
        assert claimed is not None

        started = asyncio.Event()

        async def slow_handler(_payload):
            started.set()
            await asyncio.sleep(5)
            return "stale"

        task = asyncio.ensure_future(worker._run_with_lease(t, slow_handler))
        await started.wait()

        # 模拟租约被其它单元接管
        def steal():
            tt = q.store.get(t.id)
            tt.lease_owner = "intruder"

        await asyncio.to_thread(steal)

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2)

    async def test_执行超时按可重试失败处理而不是卡死(self, make_queue, drain, case):
        case("并发竞争", "执行超时", "超时后 FAILED 退避，重试耗尽 DEAD，任务不卡死")
        q = make_queue(
            {"slow": lambda p: time.sleep(1)},
            execution_timeout=0.05,
        )
        t, _ = q.submit(idempotency_key="timeout", task_type="slow")
        assert await drain(q)
        dead = q.get(t.id)
        assert dead.status is TaskStatus.DEAD
        assert "超时" in (dead.last_error or "")
        assert dead.attempt == 3

    def test_状态流转单调不允许回退(self, case):
        case("并发竞争", "状态单调", "终态/非法跳转一律被拒绝")
        store = TaskStore()
        t = Task.new("k", "ok", {}, 2)
        store.create_if_absent(t)
        claimed = store.claim_due(10, "w1")
        assert claimed is not None
        # QUEUED -> SUCCEEDED 非法（必须先 RUNNING）
        t2 = Task.new("k2", "ok", {}, 2)
        store.create_if_absent(t2)
        with pytest.raises(InvalidTaskError):
            store.transition(t2.id, TaskStatus.SUCCEEDED,
                             expect=TaskStatus.QUEUED, lease_owner="x")
        # 成功后不能回 RUNNING/QUEUED
        store.transition(t.id, TaskStatus.SUCCEEDED,
                         expect=TaskStatus.RUNNING, lease_owner="w1")
        with pytest.raises(InvalidTaskError):
            store.transition(t.id, TaskStatus.RUNNING,
                             expect=TaskStatus.SUCCEEDED, lease_owner="w1")
        # 期望状态与实际不符（并发竞争输家）也被拒绝
        t3 = Task.new("k3", "ok", {}, 2)
        store.create_if_absent(t3)
        with pytest.raises(ConcurrentStateError):
            store.transition(t3.id, TaskStatus.SUCCEEDED,
                             expect=TaskStatus.RUNNING, lease_owner="x")

    async def test_多worker后台并发运行不重复执行(self, case):
        case("并发竞争", "多 worker 集成", "两个真实 worker 抢任务，任务只成功一次")
        q = TaskQueue(
            RetryPolicy(max_attempts=1, jitter=0.0),
            worker_count=2,
            poll_interval=0.005,
            lease_seconds=10,
        )
        exec_count = {"n": 0}

        def handler(_):
            time.sleep(0.02)  # 制造执行窗口，迫使两个 worker 有机会争抢
            exec_count["n"] += 1
            return "ok"

        q.register_handler("ok", handler)
        ids = []
        for i in range(5):
            t, _ = q.submit(idempotency_key=f"multi-{i}", task_type="ok")
            ids.append(t.id)
        q.start()
        try:
            assert await q.wait_until_drained(timeout=5)
        finally:
            await q.stop()
        assert exec_count["n"] == 5
        for tid in ids:
            assert q.get(tid).status is TaskStatus.SUCCEEDED
