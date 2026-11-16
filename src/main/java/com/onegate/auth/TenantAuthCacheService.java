package com.onegate.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.onegate.common.config.OneGateProperties;
import com.onegate.common.constant.RedisKeys;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.dto.TenantAuthInfo;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.TenantAccountMapper;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多级缓存租户认证服务: L1 Caffeine 本地缓存 + L2 Redis 分布式缓存 + Redisson 防击穿 + 空值缓存防穿透 + Pub/Sub 集群失效广播。
 * <p>
 * L2 Redis 显式采用 {@link StringRedisTemplate} + {@link ObjectMapper} 序列化纯 JSON 字符串
 * 与哨兵空值 {@code "__NULL__"}，避免引入通用 {@code RedisTemplate} 默认类型信息耦合。
 */
@Slf4j
@Service
public class TenantAuthCacheService {

    /** 非法 API Key 在 Redis L2 中的哨兵空值标记，防止恶意 Key 穿透打垮 MySQL */
    public static final String NULL_CACHE_VALUE = "__NULL__";

    private static final long LOCK_WAIT_SECONDS = 2L;
    /** 锁租约需大于 MyBatis default-statement-timeout (10s)，防止慢 SQL 期间锁提前过期 */
    private static final long LOCK_LEASE_SECONDS = 15L;

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final TenantAccountMapper tenantAccountMapper;
    private final OneGateProperties properties;
    private final ObjectMapper objectMapper;
    private final Cache<String, TenantAuthInfo> localCache;

    /** 实际穿透回源 MySQL 的次数统计 (用于压测与防击穿可观测性验证) */
    private final AtomicLong dbFallbackCount = new AtomicLong(0L);

    public TenantAuthCacheService(
            StringRedisTemplate stringRedisTemplate,
            RedissonClient redissonClient,
            TenantAccountMapper tenantAccountMapper,
            OneGateProperties properties,
            ObjectMapper objectMapper) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
        this.tenantAccountMapper = tenantAccountMapper;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.localCache = buildLocalCache(properties);
    }

    private Cache<String, TenantAuthInfo> buildLocalCache(OneGateProperties props) {
        long maxSize = Math.max(100L, props.getCaffeineMaxSize());
        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfter(new Expiry<String, TenantAuthInfo>() {
                    @Override
                    public long expireAfterCreate(String key, TenantAuthInfo value, long currentTime) {
                        long ttlSeconds = (value != null && value.isExists())
                                ? props.getAuthCacheTtlSeconds()
                                : props.getNullCacheTtlSeconds();
                        return TimeUnit.SECONDS.toNanos(Math.max(1L, ttlSeconds));
                    }

                    @Override
                    public long expireAfterUpdate(String key, TenantAuthInfo value, long currentTime, long currentDuration) {
                        return expireAfterCreate(key, value, currentTime);
                    }

                    @Override
                    public long expireAfterRead(String key, TenantAuthInfo value, long currentTime, long currentDuration) {
                        return currentDuration;
                    }
                })
                .recordStats()
                .build();
    }

    /**
     * 按 API Key 执行多级缓存鉴权查询：
     * L1 Caffeine -> L2 Redis -> Redisson 分布式互斥锁 + DCL 二次检查 -> MySQL 回源。
     */
    public TenantAuthInfo authenticate(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            return TenantAuthInfo.notFound(apiKey);
        }

        // 1. L1 Caffeine 本地缓存查询
        TenantAuthInfo l1Hit = localCache.getIfPresent(apiKey);
        if (l1Hit != null) {
            return l1Hit;
        }

        // 2. L2 Redis 分布式缓存查询
        TenantAuthInfo l2Hit = readFromRedis(apiKey);
        if (l2Hit != null) {
            localCache.put(apiKey, l2Hit);
            return l2Hit;
        }

        // 3. 回源 MySQL 前通过 Redisson 分布式锁 + DCL 双重检查防止热点 Key 击穿数据库
        return loadFromDbWithLock(apiKey);
    }

    private TenantAuthInfo loadFromDbWithLock(String apiKey) {
        RLock lock = redissonClient.getLock(RedisKeys.AUTH_LOCK_PREFIX + apiKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(LOCK_WAIT_SECONDS, LOCK_LEASE_SECONDS, TimeUnit.SECONDS);
            if (!locked) {
                // 获取锁超时: 先重试检查 L1/L2 (可能前置持锁线程刚写完缓存), 仍未命中则快速失败保护 DB
                TenantAuthInfo retryL1 = localCache.getIfPresent(apiKey);
                if (retryL1 != null) {
                    return retryL1;
                }
                TenantAuthInfo retryL2 = readFromRedis(apiKey);
                if (retryL2 != null) {
                    localCache.put(apiKey, retryL2);
                    return retryL2;
                }
                throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                        "Authentication cache reload busy, please retry shortly.");
            }

            // DCL 二次检查 L1 Caffeine 与 L2 Redis
            TenantAuthInfo dclL1 = localCache.getIfPresent(apiKey);
            if (dclL1 != null) {
                return dclL1;
            }
            TenantAuthInfo dclL2 = readFromRedis(apiKey);
            if (dclL2 != null) {
                localCache.put(apiKey, dclL2);
                return dclL2;
            }

            // 确认为首批回源请求: 查询 MySQL 并回写 L2 Redis 与 L1 Caffeine
            dbFallbackCount.incrementAndGet();
            TenantAccount account = tenantAccountMapper.selectByApiKey(apiKey);
            TenantAuthInfo loaded = (account != null)
                    ? TenantAuthInfo.fromEntity(account)
                    : TenantAuthInfo.notFound(apiKey);

            writeToRedis(apiKey, loaded);
            localCache.put(apiKey, loaded);
            return loaded;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                    "Interrupted while waiting for authentication cache lock.", ex);
        } finally {
            safeUnlock(lock, locked, apiKey);
        }
    }

    /**
     * 失效指定 API Key 的多级缓存:
     * 获取与回源相同的 Redisson 分布式锁后，再删除 Redis L2 缓存 -> 清除本节点 Caffeine -> Pub/Sub 广播失效全集群，
     * 防止并发慢回源在管理员禁用/轮换后把旧数据回填进缓存；若短暂未抢到锁或中断也执行兜底淘汰与广播。
     * <p>
     * 安全约定：严禁在日志与异常消息中打印明文 API Key，必须经过 {@link #maskApiKey(String)} 脱敏。
     */
    public void invalidateApiKey(String... apiKeys) {
        if (apiKeys == null || apiKeys.length == 0) {
            return;
        }
        Set<String> uniqueKeys = new LinkedHashSet<>();
        for (String key : apiKeys) {
            if (key != null && !key.isBlank()) {
                uniqueKeys.add(key);
            }
        }
        for (String apiKey : uniqueKeys) {
            RLock lock = redissonClient.getLock(RedisKeys.AUTH_LOCK_PREFIX + apiKey);
            boolean locked = false;
            try {
                locked = lock.tryLock(LOCK_WAIT_SECONDS, LOCK_LEASE_SECONDS, TimeUnit.SECONDS);
                if (!locked) {
                    log.warn("Timeout acquiring auth lock during cache invalidation, executing fallback eviction for key suffix={}",
                            maskApiKey(apiKey));
                }
                evictRedisLocalAndBroadcast(apiKey);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted acquiring auth lock during cache invalidation, executing fallback eviction for key suffix={}",
                        maskApiKey(apiKey));
                evictRedisLocalAndBroadcast(apiKey);
            } finally {
                safeUnlock(lock, locked, apiKey);
            }
        }
    }

    private void evictRedisLocalAndBroadcast(String apiKey) {
        try {
            stringRedisTemplate.delete(RedisKeys.authKey(apiKey));
        } catch (Exception ex) {
            log.warn("Failed to delete L2 auth cache for key suffix={}: {}", maskApiKey(apiKey), ex.getMessage());
        }
        localCache.invalidate(apiKey);
        try {
            stringRedisTemplate.convertAndSend(RedisKeys.INVALIDATE_CHANNEL, apiKey);
        } catch (Exception ex) {
            log.warn("Failed to publish auth invalidation Pub/Sub event for key suffix={}: {}",
                    maskApiKey(apiKey), ex.getMessage());
        }
    }

    private void safeUnlock(RLock lock, boolean locked, String apiKey) {
        try {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        } catch (Exception ex) {
            log.warn("Failed to unlock auth cache Redisson lock for key suffix={}: {}",
                    maskApiKey(apiKey), ex.getMessage());
        }
    }

    /**
     * 仅清除本节点 L1 Caffeine 缓存 (由 Redis Pub/Sub 订阅器调用，或用于单元测试模拟 L1 失效)。
     */
    public void evictLocalCache(String apiKey) {
        if (apiKey != null && !apiKey.isBlank()) {
            localCache.invalidate(apiKey);
        }
    }

    public boolean isPresentInLocalCache(String apiKey) {
        return apiKey != null && localCache.getIfPresent(apiKey) != null;
    }

    public CacheStats getLocalCacheStats() {
        return localCache.stats();
    }

    public long getDbFallbackCount() {
        return dbFallbackCount.get();
    }

    private TenantAuthInfo readFromRedis(String apiKey) {
        String redisKey = RedisKeys.authKey(apiKey);
        String raw = stringRedisTemplate.opsForValue().get(redisKey);
        if (raw == null) {
            return null;
        }
        if (NULL_CACHE_VALUE.equals(raw)) {
            return TenantAuthInfo.notFound(apiKey);
        }
        try {
            return objectMapper.readValue(raw, TenantAuthInfo.class);
        } catch (JsonProcessingException ex) {
            log.warn("Failed to deserialize L2 auth cache for key suffix={}, deleting corrupted entry",
                    maskApiKey(apiKey), ex);
            stringRedisTemplate.delete(redisKey);
            return null;
        }
    }

    private void writeToRedis(String apiKey, TenantAuthInfo authInfo) {
        String redisKey = RedisKeys.authKey(apiKey);
        if (authInfo == null || !authInfo.isExists()) {
            long nullTtl = Math.max(1L, properties.getNullCacheTtlSeconds());
            stringRedisTemplate.opsForValue().set(redisKey, NULL_CACHE_VALUE, nullTtl, TimeUnit.SECONDS);
            return;
        }
        try {
            String json = objectMapper.writeValueAsString(authInfo);
            long authTtl = Math.max(1L, properties.getAuthCacheTtlSeconds());
            stringRedisTemplate.opsForValue().set(redisKey, json, authTtl, TimeUnit.SECONDS);
        } catch (JsonProcessingException ex) {
            log.error("Failed to serialize TenantAuthInfo for tenantId={}", authInfo.getTenantId(), ex);
        }
    }

    private String maskApiKey(String apiKey) {
        if (apiKey == null || apiKey.length() <= 6) {
            return "***";
        }
        return "***" + apiKey.substring(apiKey.length() - 6);
    }
}
