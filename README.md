# OneGate — 高并发多厂商大模型统一网关与计费平台

[![Java](https://img.shields.io/badge/Java-17%2B%20(Verified%20JDK%2023)-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.16-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![MySQL](https://img.shields.io/badge/MySQL-8.0%20%2F%209.0-blue.svg)](https://www.mysql.com/)
[![Redis](https://img.shields.io/badge/Redis-Lua%20%2B%20Redisson%20%2B%20Pub%2FSub-red.svg)](https://redis.io/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-Confirm%20%2B%20Delay%20%2B%20DLX-ff6600.svg)](https://www.rabbitmq.com/)

OneGate 是面向企业多租户场景的高并发大模型统一网关（LLM API Gateway），对外兼容 OpenAI Chat Completions 协议（`/v1/chat/completions`、`/v1/models`、`text/event-stream` SSE 流式与非流式响应），对内解决高并发鉴权限流、上游慢渠道舱壁隔离与 TTFT 故障秒级切换、流式断连异常估算计费与防重核销，以及 200 万级流水表的索引覆盖聚合与复合游标深分页问题。

---

## 1. 核心架构与工程特性

```text
Client (OpenAI SDK / curl / SSE)
  │
  ▼  Authorization: Bearer <API Key> | X-OneGate-Request-Id
┌───────────────────────────────────────────────────────────────────────────┐
│  1. L1/L2 多级鉴权缓存 & 管理面防线                                        │
│     • Caffeine + Redis 多级缓存 (非法 Key 写 60s 短空值防穿透)             │
│     • Admin 更新/轮换密钥通过 Redisson 锁同步清 Redis + Pub/Sub 广播失效  │
│     • /admin/** 强制经 AdminAuthInterceptor + AdminTokenVerifier 常量校验 │
├───────────────────────────────────────────────────────────────────────────┤
│  2. Redis + Lua 原子双维限流 ({tenantId} HashTag 同槽)                   │
│     • 单次 Lua 原子校验并扣减：RPM (ZSet + 唯一 Nonce) + TPM (令牌桶)     │
│     • 严格复用 StringRedisTemplate 避开默认序列化引号导致 Lua tonumber nil│
├───────────────────────────────────────────────────────────────────────────┤
│  3. Redis 额度预冻 (Prompt + max_tokens) + 防重放核销 + MQ 延迟兜底      │
│     • 请求前冻结：按 Prompt + max_tokens 预占余额，并发受 Redisson 初始化锁│
│     • 核销与重放防护：seenKey 拦截重复核销；实际超额消耗时 Redis 安全钳位归零│
│     • 异步记账：RabbitMQ Confirm + 指数退避重试 (3 次) + 死信表补偿与重放 │
├───────────────────────────────────────────────────────────────────────────┤
│  4. 舱壁线程池 + TTFT 首包监控 + 429/5xx 熔断 failover + SSRF 防护         │
│     • 按厂商划分独立 JUC ThreadPoolExecutor (DEEPSEEK / QWEN / OPENAI 等)  │
│     • OkHttp 禁用重定向 (followRedirects=false)，仅 429/5xx 触发熔断切换   │
│     • 仅在“尚未向客户端写出首分片前”超时或 429/5xx 时 Cancel 原连接切备道 │
│     • SSE 客户端断开联动取消上游流，按已推送分片估算 (含 tool_calls) 扣费 │
└───────────────────────────────────────────────────────────────────────────┘
  │
  ▼  MySQL (8.0 / 9.0) + 覆盖索引 & 复合游标分页
  • uk_tenant_request (tenant_id, request_id) —— 租户隔离幂等唯一键
  • idx_tenant_model_time_tokens / idx_tenant_time_tokens —— 聚合零回表 (Using index)
  • idx_tenant_model_time / idx_tenant_create_time —— (create_time, id) 复合游标深分页
```

### 核心模块代码导航

| 模块 | 核心类 / 脚本 | 关键职责 |
| :--- | :--- | :--- |
| 网关与管理入口 | [ChatGatewayController.java](src/main/java/com/onegate/controller/ChatGatewayController.java)、[AdminTenantController.java](src/main/java/com/onegate/controller/AdminTenantController.java)、[UsageAnalyticsController.java](src/main/java/com/onegate/controller/UsageAnalyticsController.java)、[WebMvcConfig.java](src/main/java/com/onegate/common/config/WebMvcConfig.java) | `/v1/chat/completions`、`/v1/models`、`/v1/usage/**`、`/admin/tenants/**`、`/admin/benchmark/**` 路由与拦截器编排；CORS 白名单与请求头正则校验 |
| 鉴权与原子限流 | [ApiKeyAuthInterceptor.java](src/main/java/com/onegate/auth/ApiKeyAuthInterceptor.java)、[TenantAuthCacheService.java](src/main/java/com/onegate/auth/TenantAuthCacheService.java)、[RateLimiterLuaService.java](src/main/java/com/onegate/auth/RateLimiterLuaService.java)、[dual_rate_limit.lua](src/main/resources/lua/dual_rate_limit.lua) | L1 Caffeine + L2 Redis 认证缓存（分布式锁 + Pub/Sub 失效）、Admin 密钥脱敏响应、`dual_rate_limit.lua` RPM/TPM 原子扣减 |
| 多厂商路由与流式代理 | [LlmChannelRouter.java](src/main/java/com/onegate/router/LlmChannelRouter.java)、[ProviderPoolManager.java](src/main/java/com/onegate/router/ProviderPoolManager.java)、[ChannelCircuitBreaker.java](src/main/java/com/onegate/router/ChannelCircuitBreaker.java)、[LlmStreamingProxyService.java](src/main/java/com/onegate/router/LlmStreamingProxyService.java) | 10s Caffeine 渠道路由缓存、按厂商 JUC 舱壁隔离、Lua 固定窗口熔断器、TTFT 超时与首包前 `429/5xx` 自动故障转移、SSE 断连联动中断上游 |
| 额度预占与分布式计费 | [QuotaFreezeLuaService.java](src/main/java/com/onegate/billing/QuotaFreezeLuaService.java)、[freeze_quota.lua](src/main/resources/lua/freeze_quota.lua)、[settle_or_rollback_quota.lua](src/main/resources/lua/settle_or_rollback_quota.lua)、[RabbitMqConfig.java](src/main/java/com/onegate/billing/RabbitMqConfig.java)、[BillingSettlementTxService.java](src/main/java/com/onegate/billing/BillingSettlementTxService.java)、[BillingDlqConsumer.java](src/main/java/com/onegate/billing/BillingDlqConsumer.java) | `{tenantId}` 预占冻结 (`freeze-ttl-seconds=600`)、TTL+DLX 延迟回滚 (`freeze-delay-ms=180000`)、`seenKey` 防重放核销、单事务扣费落库、手动 ACK 退避重试与 DLQ 落库兜底 |
| 数据库 Schema 与 200W 压测 | [schema.sql](sql/schema.sql)、[ApiUsageLogMapper.xml](src/main/resources/mapper/ApiUsageLogMapper.xml)、[UsageDataBenchmarkRunner.java](src/main/java/com/onegate/benchmark/UsageDataBenchmarkRunner.java) | 租户/渠道/流水/死信四表定义、覆盖索引统计与复合游标深分页 SQL、200 万条数据批处理写入与 `EXPLAIN` 对比引擎 |

---

## 2. 安全防线与工程规范设计

1. **生产默认收紧配置**（详见 [application.yml](src/main/resources/application.yml) 与 [application-dev.yml](src/main/resources/application-dev.yml)）：
   - `onegate.mock-enabled` 默认为 `false`（仅 `--spring.profiles.active=dev` 时开启内置 `/mock-llm/**` 与模拟测试 Header 透传），防止生产环境暴露伪造上游节点。
   - `onegate.admin-token` 默认留空；[AdminTokenVerifier.java](src/main/java/com/onegate/auth/AdminTokenVerifier.java) 拒绝空口令并通过常量时间比较防止空串绕过。
   - `management.endpoints.web.exposure.include` 收敛为 `health`。
2. **输入校验、并发安全与 SSRF 防护**：
   - [OkHttpConfig.java](src/main/java/com/onegate/common/config/OkHttpConfig.java) 关闭 HTTP/HTTPS 自动重定向（`followRedirects=false`），阻断上游通道成为内网探测跳板。
   - [ChatCompletionRequest.java](src/main/java/com/onegate/domain/dto/ChatCompletionRequest.java) 限制 `maxTokens` 上限（`[1, 65536]`），[ChatGatewayController.java](src/main/java/com/onegate/controller/ChatGatewayController.java) 与拦截器校验 `X-OneGate-Request-Id` 格式（`^[a-zA-Z0-9._:-]{1,64}$`），防止超大预占或非法键注入。
   - 管理端局部更新（[AdminTenantController.java](src/main/java/com/onegate/controller/AdminTenantController.java)）使用 `LambdaUpdateWrapper` 显式更新指定列，避免覆盖并发记账字段 `balance`；充值操作在 `quotaFreezeLuaService.executeUnderInitLock(...)` 锁内同步 DB 与 Redis。

---

## 3. 快速启动（Quick Start）

### 3.1 环境准备
- **JDK**：17+（已在 OpenJDK 23.0.2 下完整编译通过）
- **Maven**：3.9+
- **依赖中间件**：MySQL 8.0 / 9.0、Redis 7.x、RabbitMQ 3.x（可通过项目根目录 [docker-compose.yml](docker-compose.yml) 一键拉起，端口默认仅绑定 `127.0.0.1`）

### 3.2 一键启动基础设施与本地开发调试
```bash
# 1. 复制环境变量模板并按需修改 ONEGATE_ADMIN_TOKEN / 数据库密码等
cp .env.example .env

# 2. 启动 MySQL、Redis 与 RabbitMQ（自动初始化 sql/schema.sql）
docker compose up -d

# 若本机已安装原生 MySQL (3306) 或另有 Redis 占用 6379，也可仅按需拉起单个容器（如 RabbitMQ）：
# docker compose up -d rabbitmq
```

**方式 A：使用引导脚本（Windows PowerShell）**
```powershell
# 自动检查本机环境、初始化 sql/schema.sql，并以 dev profile 启动应用
.\scripts\start-dev.ps1 -StartDocker -StartApp
```

**方式 B：标准 Maven 启动（跨平台）**
```bash
# 本地联调（开启 /mock-llm/** 仿真上游，dev 默认 admin-token 为 onegate-dev-secret-change-me）
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=dev"

# 生产 / 默认模式启动（必须显式注入高强度 ONEGATE_ADMIN_TOKEN）
export ONEGATE_ADMIN_TOKEN="replace-with-strong-secret"
mvn spring-boot:run
```

### 3.3 常用 API 调用示例（`dev` 模式）
> 种子数据（[schema.sql](sql/schema.sql)）：演示租户 `t_1001` 的 API Key 为 `sk-onegate-demo-1001`（余额 `1,000,000` Tokens）。

```bash
# 1. 非流式对话转发（走模拟主渠道）
curl -X POST http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer sk-onegate-demo-1001" \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"Hello OneGate"}],"stream":false,"max_tokens":128}'

# 2. SSE 流式转发 + 主渠道 500 故障自动转移至备用渠道（仅 dev mock-enabled=true 生效）
curl -N -X POST http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer sk-onegate-demo-1001" \
  -H "X-OneGate-Mock-Primary-Mode: http500" \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"Stream failover test"}],"stream":true,"max_tokens":128}'

# 3. 租户覆盖索引聚合统计与复合游标分页查询
curl "http://127.0.0.1:8080/v1/usage/stats?modelName=deepseek-chat" \
  -H "Authorization: Bearer sk-onegate-demo-1001"
curl "http://127.0.0.1:8080/v1/usage/logs?pageSize=20" \
  -H "Authorization: Bearer sk-onegate-demo-1001"

# 4. 管理端查询租户脱敏快照（默认/生产与 dev 均校验 X-OneGate-Admin-Token）
curl "http://127.0.0.1:8080/admin/tenants/t_1001" \
  -H "X-OneGate-Admin-Token: onegate-dev-secret-change-me"
```

---

## 4. 自动化测试与真实基准数据

### 4.1 分层测试执行命令

> **测试边界说明**：
> - 隔离式并发回归套件（`AuthAndRateLimitIntegrationTest`、`QuotaFreezeAndBillingTest` 等）直连真实本地 **MySQL 9.0.1** 与 **Redis 7.x**，在各自上下文通过 `exclude = RabbitAutoConfiguration` + `@MockitoBean RabbitTemplate` 聚焦测并发锁、Lua 与单事务；
> - 端到端消息代理集成测试（[RabbitMqLiveIntegrationTest](src/test/java/com/onegate/billing/RabbitMqLiveIntegrationTest.java)）**不排除 RabbitMQ 自动装配、不使用 `@MockitoBean`**，直连真实 **RabbitMQ 3.12 容器（`127.0.0.1:5672`）** 验证完整 AMQP 闭环；未启动 `5672` 端口时通过 `@EnabledIf` 自动跳过，启动后随 `mvn test` 一并运行；
> - `@Tag("benchmark")` 200 万行压测默认跳过；真实外部大模型测试在注入 `DASHSCOPE_API_KEY` 时触发。

```bash
# 1. 运行默认回归与并发测试套件（若本地 5672 RabbitMQ 在线则一并实测 RabbitMqLiveIntegrationTest；未设 DASHSCOPE_API_KEY 时跳过在线大模型用例）
mvn test

# 2. 单独运行真实 RabbitMQ 3.12 端到端集成测试（需先运行 docker compose up -d rabbitmq）
mvn -Dtest=RabbitMqLiveIntegrationTest test

# 3. 运行 200 万级 MySQL 慢 SQL 治理与复合游标对比基准（约 45~55s）
mvn "-Donegate.excludedGroups=" -Dtest=UsageAnalyticsAndBenchmarkTest test

# 4. 运行阿里云百炼（DashScope）真实大模型端到端集成测试
export DASHSCOPE_API_KEY="your-dashscope-api-key"
mvn -Dtest=DashScopeLiveIntegrationTest test
```

### 4.2 实测验证结果汇总（最新代码基线实测）

#### A. 默认全量测试套件（含真实 RabbitMQ：**37 个用例发现 / 32 个执行通过 / 5 个在线大模型用例按预期跳过 / 0 失败**）
- **鉴权缓存与 Lua 原子限流基准（[AuthAndRateLimitIntegrationTest](src/test/java/com/onegate/auth/AuthAndRateLimitIntegrationTest.java)）**：
  - 1,000 次顺序请求（L1 Caffeine + Redis Lua 双限流）：`avg = 583.7 us`、`P50 = 518 us`、`P95 = 1,027 us`、`P99 = 1,491 us (1.491 ms)`。
  - 20 线程并发 × 50 次请求（共 1,000 次进程内并发压测）：`wallTime = 153.47 ms`、吞吐 **`6,516.1 req/s`**、`P99 = 19.295 ms`。
- **额度预占、并发核销防重与故障转移验证（[QuotaFreezeAndBillingTest](src/test/java/com/onegate/billing/QuotaFreezeAndBillingTest.java)、[LlmRouterAndStreamingTest](src/test/java/com/onegate/router/LlmRouterAndStreamingTest.java)）**：
  - 50 线程并发额度冻结：10 个成功、40 个触发余额不足拦截（HTTP 402），`finalAvail = 0`（0 超卖）。
  - 50 线程核销 vs 超时回滚并发竞争：`settleApplied = 1`、`rollbackApplied = 0`，Redis 可用余额精确锁定 `1680`。
  - 50 线程单事务 DB 计费并发：10 个落库扣减成功、40 个异常回滚，`finalBalance = 0`。
  - SSE 中途断连 (`client_abort`)：`15` 分片中仅拉取并转发 `4` 片即触发上游 `Call.cancel()`，按已推分片结算 `20` Tokens，剩余预占退还后 `availAfter = 9980`。
  - JUC 舱壁隔离：注入 `DEEPSEEK` 池饱和返回 HTTP `503` 时，`QWEN` 独立线程池端到端耗时仅 `38 ms`，零级联阻塞。
- **真实 RabbitMQ 3.12 端到端全链路验证（[RabbitMqLiveIntegrationTest](src/test/java/com/onegate/billing/RabbitMqLiveIntegrationTest.java)，5/5 通过）**：
  - `[MQ-1]` **真实拓扑声明与 Confirm/Returns**：4 个交换机与 4 个队列一次性声明成功无属性冲突，真实发布消息触发 `confirmAckCount` 递增与不可路由消息 `returnedMessageCount` 递增。
  - `[MQ-2]` **HTTP 网关 → 真实 RabbitMQ → 单事务落库 + 重复消费幂等**：`POST /v1/chat/completions` 经真实 MQ 结算 `30` Tokens（`redisAvail = mysqlBalance = 9970`）；手动重放完全相同 `BillingMessage` 经 `uk_tenant_request` 幂等跳过，流水仍为 `1` 条且死信 `0`。
  - `[MQ-3]` **确定性异常直转死信持久化**：余额不足场景下 `BillingSettleConsumer` 直接 `basicNack(requeue=false)` 经 `BILLING_DLX -> BILLING_DLQ` 由 `BillingDlqConsumer` 落库 `billing_dead_letter_record`（`status = 'PENDING'`）。
  - `[MQ-4]` **瞬时故障指数退避重试与耗尽兜底**：模拟前 2 次瞬时数据库异常在第 3 次（耗时 `1,567 ms`）重试恢复落库；连续 3 次瞬时失败（耗时 `1,566 ms`）耗尽重试后经 `oneGateRejectAndDontRequeueRecoverer` 拒绝入 `BILLING_DLQ` 并持久化死信记录。（通过真机测试发现并修复了 `DefaultJackson2JavaTypeMapper` DLQ 反序列化信任包与手动 ACK 下默认 `Recoverer` 未设 `rejectManual=true` 导致消息滞留 Unacked 的两处真缺陷，见 [RabbitMqConfig.java](src/main/java/com/onegate/billing/RabbitMqConfig.java)）。
  - `[MQ-5]` **原生 TTL+DLX 预占冻结补偿**：孤儿冻结 `300` Tokens 在 `1,556 ms` 后经 TTL+DLX 到期自动回滚恢复 `1,000`；已结算请求对应延迟消息到期时 `FreezeTimeoutConsumer` 检测到预冻 Key 已核销，幂等跳过且余额维持 `880`。
- **P0/P1 安全与并发修复回归套件（[SecurityAndConcurrencyFixesTest](src/test/java/com/onegate/SecurityAndConcurrencyFixesTest.java)，7/7 通过）**：
  - 验证空/缺失 Admin Token 拦截及敏感 API Key 脱敏响应；默认关闭 Mock 上游与模拟 Header 透传；非法 `X-OneGate-Request-Id` 与超限 `max_tokens` 在限流之前被拦截为 `400`；同一 `request_id` 的 RPM Nonce 计数不互相覆盖及跨租户 `seenKey`/唯一约束隔离；超额消耗触发 Redis 余额钳位归零不负扣；管理端并发局部更新不覆盖 `balance` + 锁内充值同步；上游 `HTTP 400` 客户端错误不触发熔断或故障切换。

#### B. 200 万级 MySQL 账单流水慢 SQL 治理实测（**2/2 通过**）
> 基于真实 MySQL 9.0.1 `one_gate.api_usage_log` 表 `2,000,000` 行压测数据，均采用**同构公平索引条件**验证（见 [ApiUsageLogMapper.xml](src/main/resources/mapper/ApiUsageLogMapper.xml)）：

| 评测场景 | 对比方案与命中索引 | `EXPLAIN Extra` 关键特征 | 实测耗时（中位数 / 单轮） | 结论 |
| :--- | :--- | :--- | :--- | :--- |
| 特定模型区间统计 (`modelName=deepseek-chat`) | `idx_tenant_model_time_tokens` 联合覆盖索引 (`type=range`) | `Using where; Using index`（零回表、无 filesort） | `520 ~ 536 ms`（统计 666,667 条） | 全程覆盖索引完成 `COUNT(*)` + `SUM(total_tokens)` |
| 全租户全模型统计 (`ALL_MODELS`) | `idx_tenant_time_tokens` 联合覆盖索引 (`type=range`) | `Using where; Using index`（零回表） | `1,633 ms`（统计 2,000,000 条） | 消除跳过 `model_name` 前缀引发的范围扫描退化 |
| 全模型百万深分页 (`offset=1,000,000`) | `USE INDEX (idx_tenant_create_time)` 传统 `LIMIT` (`type=ref`) vs 复合游标 (`type=range`) | 无 `Using filesort`；复合游标走索引范围截断 `Using index condition; Backward index scan` | 传统 `2,511 ~ 2,645 ms`（3 轮中位 `2,584 ms`）<br/>复合游标 **`2 ~ 5 ms`** | 结果完全一致，加速比 **`516.8x ~ 1,085.3x`** |
| 指定模型 30 万深分页 (`offset=300,000`) | `USE INDEX (idx_tenant_model_time)` 传统 `LIMIT` vs 复合游标 | `Backward index scan` vs `Using index condition` | 传统 `1,070 ~ 1,086 ms`<br/>复合游标 **`2 ~ 3 ms`** | 结果完全一致，加速比 **`357.8x ~ 487.6x`** |
| 同毫秒时间戳边界验证 | `CompositeCursor` `(create_time, id) < (lastTime, lastId)` | 主键有序联合过滤 | 25 条含 15 条同毫秒记录跨 7 页精确切分 | **0 丢失、0 重复** |

#### C. 阿里云百炼（DashScope）真实大模型实测（[DashScopeLiveIntegrationTest](src/test/java/com/onegate/router/DashScopeLiveIntegrationTest.java)，**5/5 通过**）
- **Qwen 非流式 (`qwen3.8-flash` / `qwen3.8-max`)**：精确返回模型问答与官方 `usage`，分别耗时 `2,163 ms`（`157` Tokens）与 `9,371 ms`（`362` Tokens），Redis 预占与 MySQL 单事务扣费完全一致。
- **Qwen SSE 流式转发 (`qwen3.8-flash` / `qwen3.8-max`)**：实时逐帧透传分片并提取尾包精准 `usage`，分别稳定完成 `46 chunks / 199 Tokens / 2,454 ms` 与 `44 chunks / 199 Tokens / 3,735 ms`。
- **主故障自动转移真实大模型**：故障主渠道触发异常后，`OneGate` 自动完成熔断计次与备用 DashScope `qwen3.8-flash` 切换，耗时 `2,938 ms` 完整返回响应并准确扣费 `148` Tokens。
- **真实 SSE 半途断开联动中止上游 (`client_abort`)**：客户端接收 `3` 个有效分片后物理断开 TCP 连接，网关立即中断上游百炼流并依据已推送文本估算落账 `146` Tokens（原预冻 `622` Tokens），Redis 与 MySQL 余额精确锁定 `99,854`。
