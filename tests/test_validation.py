"""场景二：非法输入与幂等冲突 —— 不静默接受，原因可区分。"""
import logging

import pytest

from app.models import IdempotencyConflictError, InvalidPayloadError

logger = logging.getLogger("test.validation")


async def test_空幂等键被拒绝(env):
    service, _, _ = env
    with pytest.raises(InvalidPayloadError, match="非空字符串"):
        await service.submit("", {})
    logger.info("空幂等键 -> InvalidPayloadError ✓")


async def test_非法载荷被拒绝(env):
    service, _, _ = env
    with pytest.raises(InvalidPayloadError, match="JSON 对象"):
        await service.submit("k", ["not", "a", "dict"])
    with pytest.raises(InvalidPayloadError, match="可 JSON 序列化"):
        await service.submit("k", {"bad": object()})
    logger.info("非对象/不可序列化载荷 -> InvalidPayloadError ✓")


async def test_非法重试次数被拒绝(env):
    service, _, _ = env
    with pytest.raises(InvalidPayloadError, match="max_attempts"):
        await service.submit("k", {}, max_attempts=0)
    logger.info("max_attempts=0 -> InvalidPayloadError ✓")


async def test_同键不同载荷报冲突(env):
    service, _, _ = env
    await service.submit("key-conflict", {"v": 1})
    with pytest.raises(IdempotencyConflictError, match="不同的任务载荷"):
        await service.submit("key-conflict", {"v": 2})
    logger.info("同键不同载荷 -> IdempotencyConflictError，未被静默接受 ✓")
