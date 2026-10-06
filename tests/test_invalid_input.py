"""场景四：非法输入必须被拒绝并给出可区分原因。"""

from __future__ import annotations

import pytest

from idempotent_queue.errors import (
    IdempotencyConflictError,
    InvalidTaskError,
    TaskNotFoundError,
)
from idempotent_queue.models import TaskStatus


class Test非法输入:
    @pytest.mark.parametrize(
        "key",
        ["", "   ", None, 123],
        ids=["空字符串", "纯空白", "None", "非字符串"],
    )
    def test_幂等键非法被拒绝(self, make_queue, key, case):
        case("非法输入", f"幂等键非法:{key!r}", "InvalidTaskError，不静默接受")
        q = make_queue({"ok": lambda p: 1})
        with pytest.raises(InvalidTaskError):
            q.submit(idempotency_key=key, task_type="ok")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "task_type",
        ["", "   ", None, 7],
        ids=["空类型", "纯空白类型", "None", "非字符串"],
    )
    def test_任务类型非法被拒绝(self, make_queue, task_type, case):
        case("非法输入", f"任务类型非法:{task_type!r}", "InvalidTaskError")
        q = make_queue()
        with pytest.raises(InvalidTaskError):
            q.submit(idempotency_key="k", task_type=task_type)  # type: ignore[arg-type]

    def test_幂等键超长被拒绝(self, make_queue, case):
        case("非法输入", "幂等键超长", "InvalidTaskError")
        q = make_queue()
        with pytest.raises(InvalidTaskError):
            q.submit(idempotency_key="x" * 129, task_type="ok")

    @pytest.mark.parametrize(
        "max_attempts",
        [0, -1, 1.5, True, "2"],
        ids=["零", "负数", "小数", "布尔值", "字符串"],
    )
    def test_最大尝试次数非法被拒绝(self, make_queue, max_attempts, case):
        case("非法输入", f"max_attempts={max_attempts!r}", "InvalidTaskError")
        q = make_queue()
        with pytest.raises(InvalidTaskError):
            q.submit(
                idempotency_key="k",
                task_type="ok",
                max_attempts=max_attempts,  # type: ignore[arg-type]
            )

    def test_payload非对象被拒绝(self, make_queue, case):
        case("非法输入", "payload 为列表", "InvalidTaskError")
        q = make_queue()
        with pytest.raises(InvalidTaskError):
            q.submit(idempotency_key="k", task_type="ok", payload=[1, 2])  # type: ignore[arg-type]

    def test_冲突与非法是两类不同错误(self, make_queue, case):
        case("非法输入", "错误原因可区分", "422 invalid_task vs 409 idempotency_conflict")
        q = make_queue({"a": lambda p: 1, "b": lambda p: 2})
        with pytest.raises(InvalidTaskError) as e1:
            q.submit(idempotency_key=" ", task_type="a")
        with pytest.raises(IdempotencyConflictError) as e2:
            q.submit(idempotency_key="dup", task_type="a")
            q.submit(idempotency_key="dup", task_type="b")
        assert e1.value.code == "invalid_task" and e1.value.http_status == 422
        assert e2.value.code == "idempotency_conflict" and e2.value.http_status == 409

    def test_查询不存在任务返回404类错误(self, make_queue, case):
        case("非法输入", "查询缺失任务", "TaskNotFoundError(404)")
        q = make_queue()
        with pytest.raises(TaskNotFoundError) as e:
            q.get("not-exist")
        assert e.value.http_status == 404

    async def test_非法请求不会产生任何任务(self, make_queue, case):
        case("非法输入", "拒绝不落库", "存储中不残留任务，状态仍干净")
        q = make_queue()
        with pytest.raises(InvalidTaskError):
            q.submit(idempotency_key="", task_type="ok")
        with pytest.raises(InvalidTaskError):
            q.submit(idempotency_key="k", task_type="")
        assert q.store.all_tasks() == []
        # 合法请求仍可正常提交
        t, created = q.submit(idempotency_key="valid", task_type="ok")
        assert created is True
        assert t.status is TaskStatus.QUEUED
