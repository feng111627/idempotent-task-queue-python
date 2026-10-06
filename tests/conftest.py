"""测试公共夹具与场景化日志。

运行 ``uv run pytest`` 时，控制台会先打印收集到的"场景 -> 用例"清单，
每个用例执行后打印结果（通过/失败），用例内部再用 ``case`` 夹具记录
场景、用例名与关键断言，确保日志能直接看出覆盖范围与每个场景的结果。
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

import pytest

from idempotent_queue.queue import TaskQueue
from idempotent_queue.retry import RetryPolicy

# 让应用与 worker 的日志在测试输出中可见（-rA / --log-cli 时更详细）
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
case_logger = logging.getLogger("scenario")
case_logger.setLevel(logging.INFO)


@pytest.fixture
def fast_policy() -> RetryPolicy:
    """无抖动、极短退避的确定性重试策略。"""
    return RetryPolicy(
        max_attempts=3,
        base_delay=0.01,
        multiplier=2.0,
        jitter=0.0,
    )


@pytest.fixture
def make_queue(fast_policy: RetryPolicy):
    queues: list[TaskQueue] = []

    def _make(handlers: dict[str, Any] | None = None, **kwargs: Any) -> TaskQueue:
        policy = kwargs.pop("retry_policy", fast_policy)
        q = TaskQueue(retry_policy=policy, worker_count=1, poll_interval=0.005, **kwargs)
        for name, fn in (handlers or {}).items():
            q.register_handler(name, fn)
        queues.append(q)
        return q

    return _make


@pytest.fixture
async def drain():
    """驱动队列中所有 worker 轮询，直到全部任务进入终态。"""

    async def _drain(q: TaskQueue, max_iters: int = 500) -> bool:
        for _ in range(max_iters):
            if all(t.is_terminal for t in q.store.all_tasks()):
                return True
            await q.run_worker_once(0)
            await asyncio.sleep(0.002)
        return all(t.is_terminal for t in q.store.all_tasks())

    return _drain


@pytest.fixture
def case(caplog):
    """记录单条用例的场景、名称、期望与结果。"""

    def _record(scenario: str, name: str, expect: str, **extra: Any) -> None:
        caplog.set_level(logging.INFO, logger="scenario")
        suffix = " ".join(f"{k}={v}" for k, v in extra.items())
        case_logger.info("场景[%s] 用例[%s] 期望[%s] %s", scenario, name, expect, suffix)

    return _record


# ---- 收集阶段：打印场景 -> 用例覆盖清单 ----------------------------------


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    grouped: dict[str, list[str]] = defaultdict(list)
    for item in items:
        scenario_file = item.path.name
        grouped[scenario_file].append(item.name)
    writer = config.get_terminal_writer()
    writer.line("\n" + "=" * 72, bold=True)
    writer.line("测试覆盖的关键场景与用例：", bold=True)
    titles = {
        "test_idempotency.py": "场景一：幂等受理（重复提交不重复执行、可区分新建/命中/冲突）",
        "test_retry_deadletter.py": "场景二：失败重试与死信（退避可解释、耗尽入死信、不可重试不重试）",
        "test_concurrency.py": "场景三：并发一致性（竞争领取、租约恢复、超时、状态单调）",
        "test_invalid_input.py": "场景四：非法输入（可区分的拒绝原因）",
        "test_api.py": "场景五：HTTP 接口（状态码、错误码与受理结论一致）",
    }
    for file in sorted(grouped, key=lambda f: list(titles).index(f) if f in titles else 99):
        names = grouped[file]
        writer.line(f"- {titles.get(file, file)}", bold=True)
        for n in names:
            writer.line(f"    * {n}")
    writer.line("=" * 72 + "\n", bold=True)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_logreport(report: pytest.TestReport):
    yield
    if report.when == "call":
        if report.passed:
            print(f"结果 ✅ PASS  {report.nodeid}")
        elif report.failed:
            print(f"结果 ❌ FAIL  {report.nodeid}")
        elif report.skipped:
            print(f"结果 ⏭  SKIP  {report.nodeid}")
