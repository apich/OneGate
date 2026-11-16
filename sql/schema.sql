-- ============================================================================
-- OneGate — 高并发大模型 API 统一网关与多租户计费限流系统
-- 数据库初始化脚本 (兼容 MySQL 8.0 / 9.0.1, 幂等可重复执行)
-- ============================================================================

CREATE DATABASE IF NOT EXISTS one_gate
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE one_gate;
SET NAMES utf8mb4;

-- ----------------------------------------------------------------------------
-- 1. 租户账户与配额表 (tenant_account)
-- 设计说明：
--   - uk_tenant_id: 支撑网关内部按 tenant_id 进行原子配额扣减与充值。
--   - uk_api_key:   支撑认证链路 Cache-Aside 回源按 Bearer API Key 精确查找。
--   - balance:      可用 Token 额度余额，采用 SQL 条件原子扣减
--                   (WHERE tenant_id = ? AND balance >= actual_cost) 防止高并发超扣。
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_account (
    id            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    tenant_id     VARCHAR(64)  NOT NULL COMMENT '租户唯一标识',
    tenant_name   VARCHAR(128) NOT NULL COMMENT '租户名称',
    api_key       VARCHAR(128) NOT NULL COMMENT '网关访问 API Key',
    balance       BIGINT       NOT NULL DEFAULT 0 COMMENT '可用 Token 额度余额',
    frozen_tokens BIGINT       NOT NULL DEFAULT 0 COMMENT '预占冻结中的 Token 额度(对账参考)',
    rpm_limit     INT          NOT NULL DEFAULT 60 COMMENT '每分钟最大请求数 (RPM)',
    tpm_limit     INT          NOT NULL DEFAULT 100000 COMMENT '每分钟最大 Token 数 (TPM)',
    status        TINYINT      NOT NULL DEFAULT 1 COMMENT '状态: 1-正常, 0-禁用',
    version       INT          NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    create_time   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    update_time   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_id (tenant_id),
    UNIQUE KEY uk_api_key (api_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='多租户账户与配额表';

-- ----------------------------------------------------------------------------
-- 2. 上游模型渠道池表 (llm_channel_key)
-- 设计说明：
--   - idx_model_status_priority (model_name, status, priority):
--     直接覆盖网关路由层高频查询 "按逻辑模型筛选启用渠道并按主备优先级排序"，
--     避免全表扫描与 filesort。
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS llm_channel_key (
    id           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '渠道主键ID',
    channel_name VARCHAR(128) NOT NULL COMMENT '渠道唯一名称',
    provider     VARCHAR(32)  NOT NULL COMMENT '上游模型厂商: DEEPSEEK/QWEN/OPENAI/MOCK',
    model_name   VARCHAR(64)  NOT NULL COMMENT '逻辑模型名称, 如 deepseek-chat/qwen-plus/gpt-4o-mini',
    base_url     VARCHAR(255) NOT NULL COMMENT '上游接口 Base URL',
    secret_key   VARCHAR(255) NOT NULL COMMENT '上游厂商 API 密钥',
    weight       INT          NOT NULL DEFAULT 100 COMMENT '加权轮询权重',
    priority     TINYINT      NOT NULL DEFAULT 1 COMMENT '优先级: 1-主渠道, 2-备用渠道',
    status       TINYINT      NOT NULL DEFAULT 1 COMMENT '渠道状态: 1-启用, 0-停用',
    create_time  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    update_time  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_channel_name (channel_name),
    KEY idx_model_status_priority (model_name, status, priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='上游大模型渠道池配置表';

-- ----------------------------------------------------------------------------
-- 3. API 调用计费流水表 (api_usage_log)
-- 设计说明：
--   - uk_tenant_request (tenant_id, request_id):
--     租户维度幂等键，保证 RabbitMQ 计费消费者幂等落库，同时阻断跨租户 request_id 碰撞/白嫖。
--   - idx_tenant_model_time_tokens (tenant_id, model_name, create_time, total_tokens):
--     按模型筛选时的联合覆盖索引，聚合 COUNT(*) 与 SUM(total_tokens) 全程走 Using index，零回表。
--   - idx_tenant_time_tokens (tenant_id, create_time, total_tokens):
--     不区分模型全租户统计时的联合覆盖索引，消除跳过 model_name 前缀导致的范围退化。
--   - idx_tenant_model_time (tenant_id, model_name, create_time):
--     带 modelName 过滤时的游标深分页索引 (隐式附带主键 id)，消除 total_tokens 列对排序的阻断。
--   - idx_tenant_create_time (tenant_id, create_time):
--     InnoDB 二级索引叶子节点隐式附带主键 id，等效于 (tenant_id, create_time, id)，
--     完美支持全租户无 modelName 过滤的 (create_time, id) < (last_time, last_id) 游标深分页。
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_usage_log (
    id                BIGINT      NOT NULL AUTO_INCREMENT COMMENT '流水主键ID',
    request_id        VARCHAR(64) NOT NULL COMMENT '网关全局唯一请求ID(租户内幂等键)',
    tenant_id         VARCHAR(64) NOT NULL COMMENT '租户唯一标识',
    model_name        VARCHAR(64) NOT NULL COMMENT '调用逻辑模型名称',
    provider          VARCHAR(32) NOT NULL COMMENT '实际命中的上游厂商',
    channel_id        BIGINT      NOT NULL COMMENT '实际命中的上游渠道ID',
    prompt_tokens     INT         NOT NULL DEFAULT 0 COMMENT '输入 Prompt Token 数',
    completion_tokens INT         NOT NULL DEFAULT 0 COMMENT '输出 Completion Token 数',
    total_tokens      INT         NOT NULL DEFAULT 0 COMMENT '总计费 Token 数',
    cost_ms           INT         NOT NULL DEFAULT 0 COMMENT '全链路请求耗时(毫秒)',
    finish_reason     VARCHAR(32) DEFAULT NULL COMMENT '结束原因: stop/length/fallback/error',
    estimated         TINYINT     NOT NULL DEFAULT 0 COMMENT '是否兜底估算 Token: 0-精确解析, 1-本地估算',
    create_time       DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录创建时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_request (tenant_id, request_id),
    KEY idx_tenant_model_time_tokens (tenant_id, model_name, create_time, total_tokens),
    KEY idx_tenant_time_tokens (tenant_id, create_time, total_tokens),
    KEY idx_tenant_model_time (tenant_id, model_name, create_time),
    KEY idx_tenant_create_time (tenant_id, create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='API 调用计量与计费流水表';

-- ----------------------------------------------------------------------------
-- 4. 计费死信补偿记录表 (billing_dead_letter_record)
-- 设计说明：
--   - 承载 RabbitMQ 计费主队列重试耗尽后路由至 DLQ 的失败消息，支持人工介入或定时重放补偿。
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS billing_dead_letter_record (
    id           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '死信记录主键ID',
    request_id   VARCHAR(64)  NOT NULL COMMENT '关联网关请求ID',
    tenant_id    VARCHAR(64)  NOT NULL COMMENT '租户唯一标识',
    payload      TEXT         NOT NULL COMMENT '原始计费 MQ 消息 JSON 载荷',
    error_reason VARCHAR(512) NOT NULL COMMENT '进入死信队列的异常原因',
    status       VARCHAR(32)  NOT NULL DEFAULT 'PENDING' COMMENT '处理状态: PENDING-待补偿, RESOLVED-已解决',
    create_time  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '死信入库时间',
    update_time  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '处理更新时间',
    PRIMARY KEY (id),
    KEY idx_dlq_request_id (request_id),
    KEY idx_dlq_status_time (status, create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='计费死信队列落库补偿表';

-- ----------------------------------------------------------------------------
-- 5. 初始化演示/压测种子数据 (幂等 INSERT IGNORE)
-- ----------------------------------------------------------------------------
INSERT IGNORE INTO tenant_account
    (tenant_id, tenant_name, api_key, balance, frozen_tokens, rpm_limit, tpm_limit, status, version)
VALUES
    ('t_1001',     'Demo Tenant 1001', 'sk-onegate-demo-1001', 1000000, 0, 1200,  500000, 1, 0),
    ('t_1002',     'VIP Tenant 1002',  'sk-onegate-vip-1002',  5000000, 0, 3000, 2000000, 1, 0),
    ('t_disabled', 'Disabled Tenant',  'sk-onegate-disabled',   100000, 0,   60,  100000, 0, 0);

INSERT IGNORE INTO llm_channel_key
    (channel_name, provider, model_name, base_url, secret_key, weight, priority, status)
VALUES
    ('deepseek-chat-primary', 'DEEPSEEK', 'deepseek-chat', 'http://127.0.0.1:8080/mock-llm/primary', 'mock-deepseek-primary-key', 100, 1, 1),
    ('deepseek-chat-backup',  'DEEPSEEK', 'deepseek-chat', 'http://127.0.0.1:8080/mock-llm/backup',  'mock-deepseek-backup-key',   50, 2, 1),
    ('qwen-plus-primary',     'QWEN',     'qwen-plus',     'http://127.0.0.1:8080/mock-llm/primary', 'mock-qwen-primary-key',     100, 1, 1),
    ('qwen-plus-backup',      'QWEN',     'qwen-plus',     'http://127.0.0.1:8080/mock-llm/backup',  'mock-qwen-backup-key',       50, 2, 1),
    ('gpt-4o-mini-primary',   'OPENAI',   'gpt-4o-mini',   'http://127.0.0.1:8080/mock-llm/primary', 'mock-openai-primary-key',   100, 1, 1),
    ('gpt-4o-mini-backup',    'OPENAI',   'gpt-4o-mini',   'http://127.0.0.1:8080/mock-llm/backup',  'mock-openai-backup-key',     50, 2, 1);
