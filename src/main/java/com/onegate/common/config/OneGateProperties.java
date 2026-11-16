package com.onegate.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * OneGate 网关全局自定义配置属性 (对应 application.yml 中 onegate.*)。
 */
@Data
@ConfigurationProperties(prefix = "onegate")
public class OneGateProperties {

    /** L1/L2 正常租户认证缓存 TTL (秒) */
    private long authCacheTtlSeconds = 300;

    /** 无效 API Key 空值缓存 TTL (秒), 防止缓存穿透 */
    private long nullCacheTtlSeconds = 60;

    /** L1 Caffeine 本地缓存最大条目数 */
    private long caffeineMaxSize = 10_000;

    /** 上游 TCP 连接超时 (毫秒) */
    private long connectTimeoutMs = 2000;

    /** 首字延迟超时阈值 TTFT (毫秒), 超时触发主备快速故障转移 */
    private long ttftTimeoutMs = 3000;

    /** 上游完整读取超时 (毫秒) */
    private long readTimeoutMs = 60_000;

    /** 非流式请求整包最大等待超时 (毫秒) */
    private long nonStreamTimeoutMs = 30_000;

    /** SSE 流式请求整链路最大超时 (毫秒) */
    private long streamTimeoutMs = 180_000;

    /** 单次请求允许的 max_tokens 上限 */
    private int maxTokensLimit = 65_536;

    /** 渠道连续失败熔断阈值 */
    private int failureThreshold = 3;

    /** 渠道熔断冷却期 (秒) */
    private long cooldownSeconds = 30;

    /** 单次请求最大故障转移重试次数 */
    private int maxFailoverAttempts = 2;

    /** Redis 预占冻结 Key TTL (秒), 严格大于 RabbitMQ freezeDelayMs */
    private long freezeTtlSeconds = 600;

    /** RabbitMQ 冻结超时延迟释放队列 TTL (毫秒) */
    private long freezeDelayMs = 180_000;

    /** 请求级配额幂等标记 TTL (秒) */
    private long quotaSeenTtlSeconds = 86_400;

    /** 是否启用内置 /mock-llm 模拟上游 (默认关闭, 仅 dev/测试开启) */
    private boolean mockEnabled = false;

    /** 管理后台鉴权 Token (默认空串强制外部注入, 避免硬编码弱口令) */
    private String adminToken = "";

    /** 允许跨域的 Origin Pattern 列表 (逗号分隔) */
    private String corsAllowedOrigins = "http://localhost:*,http://127.0.0.1:*";

    /** 各上游模型厂商独立舱壁隔离线程池配置 */
    private Map<String, ThreadPoolSpec> pools = new LinkedHashMap<>();

    /**
     * 按厂商标识获取对应隔离线程池规格，未显式配置时回退至 default 规格。
     */
    public ThreadPoolSpec getPoolSpec(String provider) {
        if (provider != null && !provider.isBlank()) {
            ThreadPoolSpec spec = pools.get(provider.toLowerCase(Locale.ROOT));
            if (spec != null) {
                return spec;
            }
        }
        return pools.getOrDefault("default", new ThreadPoolSpec());
    }

    @Data
    public static class ThreadPoolSpec {
        private int coreSize = 16;
        private int maxSize = 64;
        private int queueCapacity = 256;
        private long keepAliveSeconds = 60;
    }
}
