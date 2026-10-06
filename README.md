# Idempotent Task Queue

基于 FastAPI 的异步任务队列：接口提交、后台异步执行，在重复提交、失败重试与并发执行下保持状态一致、结论可解释。

## 任务状态流转

```
                 ┌─────────────┐
                 │   queued    │ 已受理，等待执行
                 └──────┬──────┘
                        │ worker 获取租约（CAS，写入 run_token）
                        ▼
                 ┌─────────────┐
            ┌────│   running   │────┐
            │    └─────────────┘    │
   成功     │              可重试失败 │  不可重试失败 / 重试耗尽
            ▼                       ▼
     ┌────────────┐          ┌────────────┐         ┌────────────┐
     │ succeeded  │          │   failed   │────────▶│    dead    │
     └────────────┘          └─────┬──────┘ 耗尽    └────────────┘
                                   │ 退避到期
                                   ▼ 回到 queued
```

- 状态机定义在 `app/models.py` 的 `ALLOWED_TRANSITIONS`，所有流转经存储层 CAS（`version` 乐观锁）校验，非法流转直接拒绝，保证单调、无回退。
- `succeeded` 与 `dead` 是终态，不再变化。
- 执行前 worker 必须抢到租约（`queued → running` 并写入 `run_token`），只有持有当前 token 的执行者能写回结果；超时或中途中断的过期执行者写回会被拒绝，不会产生脏写或状态回退。

## 重试与死信规则

- 处理器抛出 `RetryableError`、抛出未预期异常、或执行超过 `attempt_timeout`：视为**可重试失败**，状态 `running → failed`，按指数退避 `base_backoff * 2^(attempts-1)` 后重新入队（`failed → queued`）。
- 第 `max_attempts` 次尝试仍失败：进入死信 `dead`，`last_error` 保留失败原因（含 `attempts exhausted` 标记）。
- 处理器抛出 `FatalError`：**不可重试**，首次失败直接进入死信，`last_error` 带 `fatal:` 前缀。
- `failed`（等待重试）与 `dead`（死信）是两个明确区分的独立状态。

## 幂等约定

- 提交时必须携带 `idempotency_key`；同一键只会创建一个任务、执行一次。
- 重复提交（键与载荷均相同）：返回已有任务，`created=false`（HTTP 200），不重复执行。
- 键相同但载荷不同：拒绝并返回 `409 idempotency_conflict`，不静默接受。
- 参数非法（空键、载荷非 JSON 对象/不可序列化/超限、`max_attempts < 1`）：返回 `422 invalid_payload`，原因可区分。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务。201=新建，200=幂等命中，409=键冲突，422=参数非法 |
| GET | `/tasks/{task_id}` | 查询任务状态、尝试次数、失败原因与结果 |

## 配置项

`TaskQueue(store, handler, ...)`（见 `app/worker.py`）：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `workers` | 4 | 并发执行单元数量 |
| `base_backoff` | 0.05s | 退避基数，实际延迟 = `base_backoff * 2^(attempts-1)` |
| `attempt_timeout` | 5.0s | 单次尝试超时，超时按可重试失败处理 |

`TaskService` 校验上限（`app/service.py`）：幂等键最长 128 字符，载荷最大 64KB。

## 本地验证

```bash
uv sync                  # 安装依赖
uv run python -m pytest  # 运行测试（日志会打印各场景用例与结果）
uv run python main.py    # 启动服务，访问 http://127.0.0.1:8000/docs
```

测试覆盖的关键场景（`tests/`）：

- `test_idempotency.py`：幂等命中、并发提交同键只建一个任务、不同键独立执行；
- `test_validation.py`：空键/非法载荷/非法重试次数/同键不同载荷冲突；
- `test_retry_deadletter.py`：退避重试后成功、重试耗尽进死信、FatalError 直接进死信、超时回收；
- `test_concurrency.py`：重复入队去重、批量任务恰好执行一次、状态流转单调；
- `test_api.py`：HTTP 状态码与错误码（201/200/409/422/404）。

手动验证示例：

```bash
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key": "demo-1", "payload": {"fail": true}, "max_attempts": 2}'
curl localhost:8000/tasks/<task_id>   # 观察 failed -> queued -> dead 的流转
```
