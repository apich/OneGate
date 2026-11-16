package com.onegate.billing;

import com.onegate.common.config.OneGateProperties;
import com.onegate.common.constant.RedisKeys;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.TenantAccountMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis + Lua 脚本的请求级额度预占冻结、原子核销与超时回滚服务。
 * <p>
 * 强制使用 {@link StringRedisTemplate} 与 {@code {tenant_id}} HashTag 同槽 Key，
 * 避免 Jackson JSON 序列化引起 Lua {@code tonumber()} 解析失败并支持 Redis Cluster 单节点原子执行。
 */
@Slf4j
@Service
public class QuotaFreezeLuaService {

    private static final long LOCK_WAIT_SECONDS = 2L;
    /** 锁租约需大于 MyBatis default-statement-timeout (10s)，防止慢 SQL 期间锁提前过期 */
    private static final long LOCK_LEASE_SECONDS = 15L;

    private static final long STATUS_OK = 1L;
    private static final long STATUS_INSUFFICIENT = 0L;
    private static final long STATUS_MISSING_AVAIL = -1L;
    private static final long STATUS_DUPLICATE_FREEZE = -2L;

    private static final DefaultRedisScript<Long> INCREMENT_IF_EXISTS_SCRIPT = new DefaultRedisScript<>(
            "if (redis.call('EXISTS', KEYS[1]) == 1) then return redis.call('INCRBY', KEYS[1], tonumber(ARGV[1])) else return nil end",
            Long.class
    );

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final TenantAccountMapper tenantAccountMapper;
    private final OneGateProperties properties;

    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> freezeQuotaScript;

    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> settleOrRollbackScript;

    public QuotaFreezeLuaService(
            StringRedisTemplate stringRedisTemplate,
            RedissonClient redissonClient,
            TenantAccountMapper tenantAccountMapper,
            OneGateProperties properties) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
        this.tenantAccountMapper = tenantAccountMapper;
        this.properties = properties;

        this.freezeQuotaScript = new DefaultRedisScript<>();
        this.freezeQuotaScript.setLocation(new ClassPathResource("lua/freeze_quota.lua"));
        this.freezeQuotaScript.setResultType(List.class);

        this.settleOrRollbackScript = new DefaultRedisScript<>();
        this.settleOrRollbackScript.setLocation(new ClassPathResource("lua/settle_or_rollback_quota.lua"));
        this.settleOrRollbackScript.setResultType(List.class);
    }

    /**
     * 按预估 Token 数原子冻结可用额度；若 Redis 尚无余额缓存则加 Redisson 锁按需从 MySQL 冷启动 NX 加载。
     *
     * @return 冻结后 Redis 中的剩余可用余额
     */
    public long freezeQuotaOrThrow(String tenantId, String requestId, int estimatedTokens) {
        if (tenantId == null || tenantId.isBlank() || requestId == null || requestId.isBlank()) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "tenantId and requestId must not be blank.");
        }
        if (estimatedTokens < 0) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "estimatedTokens must not be negative.");
        }
        long ttlSeconds = Math.max(1L, properties.getFreezeTtlSeconds());
        long seenTtlSeconds = Math.max(1L, properties.getQuotaSeenTtlSeconds());

        List<String> keys = Arrays.asList(
                RedisKeys.quotaAvailKey(tenantId),
                RedisKeys.quotaFreezeKey(tenantId, requestId),
                RedisKeys.quotaSeenKey(tenantId, requestId)
        );

        for (int attempt = 0; attempt < 2; attempt++) {
            List<?> raw = stringRedisTemplate.execute(
                    freezeQuotaScript,
                    keys,
                    String.valueOf(estimatedTokens),
                    String.valueOf(ttlSeconds),
                    String.valueOf(seenTtlSeconds)
            );
            if (raw == null || raw.size() < 2) {
                throw new GatewayException(ErrorCode.INTERNAL_ERROR, "Quota freeze Lua script returned invalid response.");
            }
            long status = toLong(raw.get(0));
            long value = toLong(raw.get(1));

            if (status == STATUS_OK) {
                return value;
            }
            if (status == STATUS_MISSING_AVAIL) {
                initAvailBalanceIfAbsent(tenantId);
                continue;
            }
            if (status == STATUS_INSUFFICIENT) {
                throw new GatewayException(ErrorCode.INSUFFICIENT_QUOTA,
                        String.format("Insufficient token quota for tenant %s (available=%d, required=%d).",
                                tenantId, value, estimatedTokens));
            }
            if (status == STATUS_DUPLICATE_FREEZE) {
                throw new GatewayException(ErrorCode.INVALID_REQUEST,
                        "Duplicate or replayed request_id is not allowed: " + requestId);
            }
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "Invalid quota freeze parameters.");
        }

        throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                "Failed to initialize quota balance cache for tenant: " + tenantId);
    }

    /**
     * 正常结束或客户端断连后原子核销预占额度，并将 {@code (frozen - actualCost)} 差额幂等退还至可用余额。
     * <p>
     * 核销仅删除 {@code freezeKey}，保持 {@code seenKey} 在有效期内持续拦截同 {@code request_id} 重放请求；
     * 若发生超额核销且余额不足扣减，Lua 脚本会将 Redis 可用余额钳制在 0，绝不出现负余额。
     */
    public SettleResult settleQuota(String tenantId, String requestId, int actualTokens) {
        if (tenantId == null || tenantId.isBlank() || requestId == null || requestId.isBlank()) {
            return new SettleResult(false, 0L, 0L, false);
        }
        int actualCost = Math.max(0, actualTokens);
        List<String> keys = Arrays.asList(
                RedisKeys.quotaAvailKey(tenantId),
                RedisKeys.quotaFreezeKey(tenantId, requestId)
        );
        List<?> raw = stringRedisTemplate.execute(
                settleOrRollbackScript,
                keys,
                String.valueOf(actualCost)
        );
        if (raw == null || raw.isEmpty()) {
            return new SettleResult(false, 0L, 0L, false);
        }
        boolean applied = toLong(raw.get(0)) == 1L;
        if (!applied) {
            return new SettleResult(false, 0L, 0L, false);
        }
        long delta = raw.size() > 1 ? toLong(raw.get(1)) : 0L;
        long frozen = raw.size() > 2 ? toLong(raw.get(2)) : 0L;
        boolean clamped = raw.size() > 3 && toLong(raw.get(3)) == 1L;
        if (clamped) {
            log.warn("Quota overdraft clamped to zero in Redis: tenantId={}, requestId={}, frozen={}, actualCost={}, appliedDelta={}",
                    tenantId, requestId, frozen, actualCost, delta);
        }
        return new SettleResult(true, delta, frozen, clamped);
    }

    /**
     * 超时未核销或上游异常未消耗任何 Token 时全额回滚预占额度 (与 {@link #settleQuota} 互斥幂等)。
     */
    public SettleResult rollbackFrozen(String tenantId, String requestId) {
        return settleQuota(tenantId, requestId, 0);
    }

    /**
     * 查询当前 Redis 中的可用额度快照（未加载则返回 null）。
     */
    public Long getAvailableQuota(String tenantId) {
        String val = stringRedisTemplate.opsForValue().get(RedisKeys.quotaAvailKey(tenantId));
        return val != null ? Long.parseLong(val) : null;
    }

    /**
     * 在与配额冷启动初始化同一把 Redisson 分布式锁 ({@link RedisKeys#QUOTA_INIT_LOCK_PREFIX}) 内执行管理端原子充值等操作，
     * 防止管理端充值与网关冷启动回源并发执行导致额度丢失或重复累加。
     */
    public void executeUnderInitLock(String tenantId, Runnable action) {
        if (tenantId == null || tenantId.isBlank() || action == null) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "tenantId and action must not be null.");
        }
        RLock lock = redissonClient.getLock(RedisKeys.QUOTA_INIT_LOCK_PREFIX + tenantId);
        boolean locked = false;
        try {
            locked = lock.tryLock(LOCK_WAIT_SECONDS, LOCK_LEASE_SECONDS, TimeUnit.SECONDS);
            if (!locked) {
                throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                        "Timeout acquiring quota init lock for tenant: " + tenantId);
            }
            action.run();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                    "Interrupted while acquiring quota init lock for tenant: " + tenantId, ex);
        } finally {
            safeUnlock(lock, locked, tenantId);
        }
    }

    /**
     * 若租户 Redis 可用配额缓存 Key ({@link RedisKeys#quotaAvailKey(String)}) 已存在，则原子递增指定额度；
     * 若尚未加载到 Redis，则返回 {@code null}，由后续首次请求走 DB 冷启动加载最新余额。
     */
    public Long incrementRedisBalanceIfExists(String tenantId, long amount) {
        if (tenantId == null || tenantId.isBlank()) {
            return null;
        }
        String availKey = RedisKeys.quotaAvailKey(tenantId);
        return stringRedisTemplate.execute(
                INCREMENT_IF_EXISTS_SCRIPT,
                Collections.singletonList(availKey),
                String.valueOf(amount)
        );
    }

    private void initAvailBalanceIfAbsent(String tenantId) {
        String availKey = RedisKeys.quotaAvailKey(tenantId);
        if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(availKey))) {
            return;
        }
        RLock lock = redissonClient.getLock(RedisKeys.QUOTA_INIT_LOCK_PREFIX + tenantId);
        boolean locked = false;
        try {
            locked = lock.tryLock(LOCK_WAIT_SECONDS, LOCK_LEASE_SECONDS, TimeUnit.SECONDS);
            if (!locked) {
                throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                        "Timeout acquiring quota initialization lock for tenant: " + tenantId);
            }
            if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(availKey))) {
                return;
            }
            TenantAccount account = tenantAccountMapper.selectByTenantId(tenantId);
            if (account == null) {
                throw new GatewayException(ErrorCode.INVALID_API_KEY, "Tenant account not found: " + tenantId);
            }
            long initialBalance = account.getBalance() != null ? Math.max(0L, account.getBalance()) : 0L;
            Boolean created = stringRedisTemplate.opsForValue().setIfAbsent(availKey, String.valueOf(initialBalance));
            log.info("Initialized Redis quota avail balance from MySQL: tenantId={}, balance={}, nxCreated={}",
                    tenantId, initialBalance, created);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                    "Interrupted while initializing quota cache for tenant: " + tenantId, ex);
        } finally {
            safeUnlock(lock, locked, tenantId);
        }
    }

    private void safeUnlock(RLock lock, boolean locked, String tenantId) {
        try {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        } catch (Exception ex) {
            log.warn("Failed to unlock quota Redisson lock for tenantId={}: {}", tenantId, ex.getMessage());
        }
    }

    private long toLong(Object val) {
        if (val instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(val));
    }

    @Getter
    @AllArgsConstructor
    public static class SettleResult {
        private final boolean applied;
        private final long refundedDelta;
        private final long frozenTokens;
        private final boolean overdraftClamped;

        public SettleResult(boolean applied, long refundedDelta, long frozenTokens) {
            this(applied, refundedDelta, frozenTokens, false);
        }
    }
}
