package com.onegate.common.constant;

/**
 * Redis Key 与 Pub/Sub 通道命名规范。
 * <p>
 * 限流与配额 Key 统一采用 Redis Cluster HashTag {@code {tenant_id}}，
 * 确保同一租户的 RPM/TPM 计数、可用余额与预占冻结 Key 落在同一分片 Slot，
 * 支持单节点 Lua 脚本原子执行。
 */
public final class RedisKeys {

    private RedisKeys() {
    }

    /** L2 认证信息缓存 Key 前缀：onegate:auth:key:{apiKey} */
    public static final String AUTH_KEY_PREFIX = "onegate:auth:key:";

    /** L1 Caffeine 本地认证缓存集群失效广播频道 */
    public static final String INVALIDATE_CHANNEL = "onegate:apikey:invalidate";

    /** 租户 RPM 滑动窗口限流 Key 模板 (含 {tenant_id} HashTag) */
    public static final String RATE_LIMIT_RPM_FMT = "onegate:rl:rpm:{%s}";

    /** 租户 TPM 令牌桶限流 Key 模板 (含 {tenant_id} HashTag) */
    public static final String RATE_LIMIT_TPM_FMT = "onegate:rl:tpm:{%s}";

    /** 租户可用 Token 配额 Key 模板 (含 {tenant_id} HashTag) */
    public static final String QUOTA_AVAIL_FMT = "onegate:quota:avail:{%s}";

    /** 请求级预占冻结额度 Key 模板 (与 QUOTA_AVAIL_FMT 共享 {tenant_id} Slot) */
    public static final String QUOTA_FREEZE_FMT = "onegate:quota:freeze:{%s}:%s";

    /** 请求级配额幂等去重标记 Key 模板 (与 QUOTA_AVAIL_FMT 共享 {tenant_id} Slot) */
    public static final String QUOTA_SEEN_FMT = "onegate:quota:seen:{%s}:%s";

    /** 上游渠道连续失败次数计数 Key 模板 (含 {channelId} HashTag) */
    public static final String CHANNEL_FAIL_FMT = "onegate:cb:fail:{%d}";

    /** 上游渠道熔断冷却标记 Key 模板 (含 {channelId} HashTag) */
    public static final String CHANNEL_COOLDOWN_FMT = "onegate:cb:cooldown:{%d}";

    /** 认证缓存防击穿分布式锁前缀 */
    public static final String AUTH_LOCK_PREFIX = "onegate:lock:auth:";

    /** 配额缓存冷启动初始化分布式锁前缀 */
    public static final String QUOTA_INIT_LOCK_PREFIX = "onegate:lock:quota:init:";

    public static String authKey(String apiKey) {
        return AUTH_KEY_PREFIX + apiKey;
    }

    public static String rateLimitRpmKey(String tenantId) {
        return String.format(RATE_LIMIT_RPM_FMT, tenantId);
    }

    public static String rateLimitTpmKey(String tenantId) {
        return String.format(RATE_LIMIT_TPM_FMT, tenantId);
    }

    public static String quotaAvailKey(String tenantId) {
        return String.format(QUOTA_AVAIL_FMT, tenantId);
    }

    public static String quotaFreezeKey(String tenantId, String requestId) {
        return String.format(QUOTA_FREEZE_FMT, tenantId, requestId);
    }

    public static String quotaSeenKey(String tenantId, String requestId) {
        return String.format(QUOTA_SEEN_FMT, tenantId, requestId);
    }

    public static String channelFailKey(Long channelId) {
        return String.format(CHANNEL_FAIL_FMT, channelId);
    }

    public static String channelCooldownKey(Long channelId) {
        return String.format(CHANNEL_COOLDOWN_FMT, channelId);
    }
}
