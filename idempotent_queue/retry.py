"""重试策略：退避计算与失败分类。

退避策略为**指数退避 + 可选抖动**：

    delay(n) = min(max_delay, base_delay * multiplier ** (n - 1))

其中 n 为第 n 次执行失败（n 从 1 开始）。叠加 ±jitter 比例的均匀抖动
以避免多个任务同时重试造成惊群；jitter=0 时完全确定，便于测试与解释。
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class RetryPolicy:
    """重试策略配置。"""

    #: 最大执行次数（含首次）。attempt 达到该值仍失败则进入死信。
    max_attempts: int = 3
    #: 首次失败后的基础退避秒数。
    base_delay: float = 0.05
    #: 退避秒数上限。
    max_delay: float = 5.0
    #: 退避倍数。
    multiplier: float = 2.0
    #: 抖动比例（0~1），在确定性退避上叠加 ±jitter 的均匀抖动；0 表示无抖动。
    jitter: float = 0.1

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts 必须 >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("退避时间不能为负")
        if self.multiplier < 1:
            raise ValueError("multiplier 必须 >= 1")
        if not 0 <= self.jitter < 1:
            raise ValueError("jitter 必须位于 [0, 1)")

    def can_retry(self, attempt: int) -> bool:
        """已执行 ``attempt`` 次后是否还允许重试。"""
        return attempt < self.max_attempts

    def delay_for(self, attempt: int, rng: random.Random | None = None) -> float:
        """第 ``attempt`` 次失败后的退避秒数（attempt 从 1 开始）。"""
        if attempt < 1:
            raise ValueError("attempt 必须从 1 开始")
        delay = min(
            self.max_delay,
            self.base_delay * (self.multiplier ** (attempt - 1)),
        )
        if self.jitter:
            rng = rng or random
            delay *= 1 + rng.uniform(-self.jitter, self.jitter)
        return max(0.0, delay)


class TaskFailure(Exception):
    """任务处理器可主动抛出的业务失败。

    retryable=False 表示不可重试错误（如参数语义非法、确定性的业务拒绝），
    将直接进入死信，不会被反复重试。
    """

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.message = message
        self.retryable = retryable


def is_retryable(exc: BaseException) -> bool:
    """判断异常是否可重试。

    - :class:`TaskFailure`：以其 ``retryable`` 标记为准；
    - 其他异常（如未预期的运行时错误、超时）：默认按瞬时错误处理，允许重试。
    """
    if isinstance(exc, TaskFailure):
        return exc.retryable
    return True
