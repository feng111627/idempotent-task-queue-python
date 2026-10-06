# 幂等异步任务队列（Python / FastAPI）

任务通过 HTTP 接口提交，由后台执行单元异步处理。系统在以下三方面提供保证：

1. **幂等受理**：同一幂等键重复提交不会重复执行，受理结果一致，并区分
   "新建 / 命中已有 / 参数冲突 / 参数非法"。
2. **失败重试与死信**：可重试失败按可解释的指数退避自动重试，次数用尽后
   进入死信并保留失败原因；不可重试错误只执行一次，直接进入死信。
3. **并发状态一致**：单一锁 + CAS 状态流转 + 租约机制，保证多个执行单元
   并发处理同一任务时不重复生效、不漏做、不状态回退；执行者崩溃/超时后
   任务会在租约过期后被接管，不会卡死或重复入队。

## 目录结构

```
idempotent_queue/
  models.py   # 任务状态枚举、Task 实体、合法状态流转表、history 事件
  errors.py   # 可区分的错误类型（非法输入 / 幂等冲突 / 不存在 / 并发竞争）
  retry.py    # RetryPolicy 指数退避、TaskFailure 失败分类
  store.py    # 线程安全内存存储：幂等插入、CAS 流转、原子领取/接管
  worker.py   # 后台执行单元：执行、续租、超时、重试/死信写回
  queue.py    # 门面：参数校验、幂等提交、处理器注册、worker 生命周期
  api.py      # FastAPI 接口与错误码 -> HTTP 状态码映射
tests/        # 五类关键场景测试（45 个用例）
main.py       # 本地启动入口（含 echo 示例处理器）
```

## 任务状态流转

```
                 ┌──────────────────── 重试退避到期 ───────────────────┐
                 │                                                     │
  提交受理 ─► QUEUED ──领取──► RUNNING ──成功──────────────► SUCCEEDED（终态）
                              │     ▲
                              │     └─────────────────────────────────┘
                              │
                 可重试失败 ──┴──► FAILED（退避中，available_at 到期后可再次被领取）
                 不可重试失败
                 或重试耗尽 ─────► DEAD（终态，保留 last_error 与 history）
```

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `queued` | 已受理排队中（首次提交或重新入队） | 否 |
| `running` | 已被某个执行单元凭租约锁定，正在执行 | 否 |
| `failed` | 可重试失败，按 `available_at` 退避等待中 | 否 |
| `succeeded` | 执行成功 | 是 |
| `dead` | 死信：重试耗尽，或命中不可重试错误 | 是 |

合法流转只有：

- `queued -> running`
- `running -> succeeded | failed | dead`
- `failed -> running`（退避到期重新领取）

任何其它跳转（包括终态回退、期望状态不符、租约持有者不符）都会被存储层
CAS 拒绝并记录冲突，状态只能沿上表单调前进。每个任务带 `history` 列表，
逐条记录 `from/to`、发生时间、尝试序号与说明（如"可重试失败，0.020s 后
第 2 次重试"、"进入死信：重试次数已耗尽"），结论可解释、可追溯。

## 重试与死信规则

**退避策略（指数退避 + 抖动）：**

```
delay(n) = min(max_delay, base_delay * multiplier ** (n - 1)) * (1 + uniform(-jitter, +jitter))
```

- `n` 为第几次执行失败（从 1 开始）；
- `jitter=0` 时完全确定（测试默认），默认配置带 ±10% 抖动防惊群；
- 每次失败后任务进入 `failed`，并设置 `available_at = now + delay`，
  worker 只领取到期任务，因此重试节奏可见、可解释。

**何时进入死信（`dead`）：**

1. 可重试错误（处理器抛普通异常，或 `TaskFailure(retryable=True)`），
   且 `attempt == max_attempts`：原因记为"重试次数已耗尽"；
2. 不可重试错误（处理器抛 `TaskFailure(retryable=False)`）：只执行一次，
   原因记为"命中不可重试错误"，**不会被反复重试**；
3. 任务类型没有注册处理器：确定性配置错误，直接死信。

死信任务保留 `last_error`（异常类型与消息）与完整 `history`，
不会被再次领取（终态过滤）。

`failed`（非终态、还会被调度）与 `dead`（终态、不再调度）在状态字段、
`is_terminal` 属性与 history 上都严格可区分。

## 幂等约定

- 每次提交必须携带非空、长度 ≤ 128 的 `idempotency_key`；
- `(idempotency_key) -> task` 唯一：存储以 insert-if-absent + 锁保证
  并发提交下同一键只创建一个任务；
- 重复提交（同键、同 `task_type`、同 `payload`）返回**同一个任务**，
  响应中 `created=false` 表示命中已有任务，首次为 `created=true`；
  无论任务处于 queued/running/failed/succeeded/dead，结论一致；
- 同键但 `task_type` 或 `payload` 不一致：返回 `409
  idempotency_conflict`，不会覆盖、不会静默接受；
- 参数非法（空键、空类型、payload 非对象、`max_attempts` 非正整数等）：
  返回 `422 invalid_task`，且不落库。

| HTTP | error.code | 触发条件 |
| --- | --- | --- |
| 202 | — | 受理成功（`created` 区分新建/命中） |
| 404 | `task_not_found` | 查询的任务不存在 |
| 409 | `idempotency_conflict` | 幂等键已被不同类型/参数的任务占用 |
| 409 | `concurrent_state` | 并发 CAS 竞争失败（一般由内部重试/接管消化） |
| 422 | `invalid_task` | 参数非法或非法状态流转 |

## 并发一致性机制

- **单锁串行化**：内存存储的所有读写在同一把 `RLock` 内完成；
- **原子领取**：`claim_due` 在锁内挑选"到期且无有效租约"的任务并置为
  `running`、写入 `lease_owner/lease_expires_at`、`attempt+1`，
  多个 worker 抢同一任务时只有一个成功；
- **租约与心跳**：执行期间每 `lease_seconds/2` 续租一次。若执行者卡顿
  导致租约过期，任务可被其它执行单元接管（history 标注"接管过期租约"），
  原执行者的心跳发现租约易主后会取消正在执行的处理器，其最终写回也会被
  CAS 拒绝，过期结果不会重复生效；
- **崩溃/超时恢复**：执行者崩溃后租约到期任务自动可被重新领取；
  单次执行超过 `execution_timeout` 按可重试失败处理，不会无限占用；
- **不重复入队**：退避只改变 `failed` 任务的 `available_at`，不产生新
  任务、不改变任务 id，幂等键始终指向同一任务。

## 配置项

构造 `TaskQueue(retry_policy=RetryPolicy(...), **kwargs)`：

`RetryPolicy`：

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `max_attempts` | `3` | 最大执行次数（含首次），用尽即死信；提交时可按任务覆盖 |
| `base_delay` | `0.05` (s) | 首次失败的基础退避 |
| `max_delay` | `5.0` (s) | 退避上限 |
| `multiplier` | `2.0` | 退避倍数（≥1） |
| `jitter` | `0.1` | 抖动比例，0 表示确定退避 |

`TaskQueue` / `Worker`：

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `worker_count` | `1` | 后台执行单元数量 |
| `poll_interval` | `0.02` (s) | 无任务可领取时的轮询间隔 |
| `lease_seconds` | `5.0` (s) | 租约时长（崩溃恢复时间上限） |
| `execution_timeout` | `30.0` (s) | 单次执行超时 |

## 接口

### 提交任务

```bash
curl -s -X POST http://127.0.0.1:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"order-1001","task_type":"echo","payload":{"x":1}}'
```

返回 `202`：

```json
{
  "accepted": true,
  "created": true,
  "task": {"id": "...", "status": "queued", "attempt": 0, "history": [...]}
}
```

重复提交同样请求时 `created=false` 且 `task.id` 不变。

### 查询任务

```bash
curl -s http://127.0.0.1:8000/tasks/<task_id> | python -m json.tool
```

返回当前 `status`、`attempt`、`max_attempts`、`last_error`、`result`、
`available_at`、租约信息与完整 `history`。

## 本地验证

需要 Python 3.14+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
# 1. 安装依赖
uv sync

# 2. 跑全部测试（日志开头打印"场景 -> 用例"清单，每个用例打印 ✅/❌ 结果）
uv run pytest -s

# 只跑某一类场景
uv run pytest tests/test_retry_deadletter.py -s

# 3. 启动服务（含 echo 示例处理器，两个 worker）
uv run python main.py

# 4. 另开终端手动验证
curl -s -X POST http://127.0.0.1:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"demo-1","task_type":"echo","payload":{"hello":"world"}}'
curl -s http://127.0.0.1:8000/tasks/<task_id> | python -m json.tool

# 演示不可重试失败 -> 直接死信
curl -s -X POST http://127.0.0.1:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"demo-fatal","task_type":"echo","payload":{"fail":"fatal"}}'

# 演示可重试失败 -> 退避重试 -> 耗尽死信
curl -s -X POST http://127.0.0.1:8000/tasks -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"demo-retry","task_type":"echo","payload":{"fail":true},"max_attempts":3}'
```

### 测试覆盖的关键场景

| 测试文件 | 场景 |
| --- | --- |
| `tests/test_idempotency.py` | 幂等命中只执行一次、并发提交唯一、类型/参数冲突、终态后结论一致 |
| `tests/test_retry_deadletter.py` | 指数退避、重试耗尽入死信、不可重试不重试、重试后恢复、FAILED/DEAD 区分 |
| `tests/test_concurrency.py` | 并发领取唯一、租约过期接管、旧结果写回被拒、心跳取消、执行超时、状态单调、多 worker 集成 |
| `tests/test_invalid_input.py` | 空键/空类型/非法 payload/非法 max_attempts、错误码可区分、拒绝不落库 |
| `tests/test_api.py` | 202/409/422/404 契约、受理结论一致、死信原因可追溯 |

## 说明与边界

- 当前实现为**进程内内存存储**，适用于单进程多 worker 的场景；多副本部署
  需要把 `TaskStore` 换成带事务/CAS 能力的外部存储（接口已按该职责切分）。
- 处理器可以是同步函数或 `async def`；同步函数在线程中执行。
