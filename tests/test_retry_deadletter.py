"""场景二：失败重试与死信。"""

from __future__ import annotations

import time

from idempotent_queue.models import TaskStatus
from idempotent_queue.retry import RetryPolicy, TaskFailure


class Test重试与死信:
    def test_退避时长按指数增长且可解释(self, case):
        case("重试与死信", "指数退避", "delay = base * multiplier**(n-1)，封顶 max_delay")
        policy = RetryPolicy(base_delay=0.1, multiplier=2.0, max_delay=1.0, jitter=0.0)
        assert policy.delay_for(1) == 0.1
        assert policy.delay_for(2) == 0.2
        assert policy.delay_for(3) == 0.4
        assert policy.delay_for(10) == 1.0  # 封顶
        assert policy.can_retry(1) is True
        assert policy.can_retry(3) is False  # 3 次用尽

    async def test_可重试失败耗尽后进入死信并保留原因(self, make_queue, drain, case):
        case("重试与死信", "重试耗尽", "FAILED 退避重试，最后 DEAD，last_error 保留")
        attempts = {"n": 0}

        def always_fail(_):
            attempts["n"] += 1
            raise RuntimeError(f"瞬时错误 #{attempts['n']}")

        q = make_queue({"flaky": always_fail})
        t, _ = q.submit(idempotency_key="retry-exhaust", task_type="flaky")

        # 第一次失败：进入 FAILED 退避，而不是直接死信
        assert await q.run_worker_once(0)
        mid = q.get(t.id)
        assert mid.status is TaskStatus.FAILED
        assert mid.attempt == 1
        assert "瞬时错误 #1" in (mid.last_error or "")
        assert mid.available_at > time.time() - 0.001  # 退避时间被记录，可解释

        assert await drain(q)
        dead = q.get(t.id)
        assert dead.status is TaskStatus.DEAD
        assert dead.attempt == 3
        assert "瞬时错误 #3" in (dead.last_error or "")
        # history 完整记录 queued->running->failed->running->failed->running->dead
        seq = [(h.from_status, h.to_status) for h in dead.history]
        assert ("running", "failed") in seq
        assert seq[-1] == ("running", "dead")

    async def test_不可重试错误直接死信不反复重试(self, make_queue, drain, case):
        case("重试与死信", "不可重试失败", "仅执行 1 次即 DEAD，不经过 FAILED")
        calls = {"n": 0}

        def fatal(_):
            calls["n"] += 1
            raise TaskFailure("参数语义非法", retryable=False)

        q = make_queue({"fatal": fatal})
        t, _ = q.submit(idempotency_key="fatal-1", task_type="fatal")
        assert await drain(q)
        dead = q.get(t.id)
        assert dead.status is TaskStatus.DEAD
        assert dead.attempt == 1
        assert calls["n"] == 1
        assert "参数语义非法" in (dead.last_error or "")
        assert all(h.to_status != "failed" for h in dead.history)

    async def test_重试期间成功则不再重试(self, make_queue, drain, case):
        case("重试与死信", "重试后恢复", "第 2 次成功 -> SUCCEEDED，attempt=2")
        state = {"n": 0}

        def flaky_then_ok(_):
            state["n"] += 1
            if state["n"] == 1:
                raise RuntimeError("第一次失败")
            return {"ok": True}

        q = make_queue({"job": flaky_then_ok})
        t, _ = q.submit(idempotency_key="recover", task_type="job")
        assert await drain(q)
        done = q.get(t.id)
        assert done.status is TaskStatus.SUCCEEDED
        assert done.attempt == 2
        assert done.result == {"ok": True}
        assert done.last_error is None

    async def test_失败状态与死信状态严格可区分(self, make_queue, case):
        case("重试与死信", "FAILED vs DEAD", "退避中为 FAILED，终态为 DEAD")
        q = make_queue({"flaky": lambda p: (_ for _ in ()).throw(RuntimeError("x"))})
        t, _ = q.submit(idempotency_key="distinguish", task_type="flaky")
        await q.run_worker_once(0)
        failed = q.get(t.id)
        assert failed.status is TaskStatus.FAILED
        assert failed.is_terminal is False
        # 模拟退避到期后被多次领取直至耗尽
        for _ in range(10):
            task_now = q.store.get(t.id)
            if task_now.is_terminal:
                break
            q.store.get(t.id).available_at = 0.0
            await q.run_worker_once(0)
        dead = q.get(t.id)
        assert dead.status is TaskStatus.DEAD
        assert dead.is_terminal is True

    async def test_未注册任务类型直接进入死信(self, make_queue, drain, case):
        case("重试与死信", "未知任务类型", "不反复重试，DEAD 且原因说明未注册")
        q = make_queue({})
        t, _ = q.submit(idempotency_key="unknown-type", task_type="not-registered")
        assert await drain(q)
        dead = q.get(t.id)
        assert dead.status is TaskStatus.DEAD
        assert "未注册" in (dead.last_error or "")
