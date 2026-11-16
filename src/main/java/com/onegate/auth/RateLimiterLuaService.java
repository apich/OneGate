package com.onegate.auth;

import com.onegate.common.constant.RedisKeys;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.common.model.RateLimitResult;
import com.onegate.common.model.RateLimitService;
import com.onegate.domain.dto.TenantAuthInfo;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * 基于 Redis + Lua 脚本的租户级双指标 (RPM 滑动窗口 + TPM 令牌桶) 原子限流服务。
 * <p>
 * 关键序列化约定：必须显式注入并使用 {@link StringRedisTemplate}（纯 UTF-8 字符串序列化器），
 * 严禁使用带 Jackson JSON 序列化的 {@code RedisTemplate<String, Object>}，
 * 否则数字与字符串参数在传输或存储时会被附加双引号（如 {@code "100"}），导致 Lua 内 {@code tonumber()} 返回 {@code nil}。
 */
@Service
public class RateLimiterLuaService implements RateLimitService {

    private static final long WINDOW_MS = 60_000L;
    private static final long KEY_EXPIRE_MS = 65_000L;
    private static final int DEFAULT_RPM_LIMIT = 60;
    private static final int DEFAULT_TPM_LIMIT = 100_000;

    private static final long REJECT_CODE_RPM = 1L;
    private static final long REJECT_CODE_TPM = 2L;

    private final StringRedisTemplate stringRedisTemplate;

    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> dualRateLimitScript;

    public RateLimiterLuaService(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.dualRateLimitScript = new DefaultRedisScript<>();
        this.dualRateLimitScript.setLocation(new ClassPathResource("lua/dual_rate_limit.lua"));
        this.dualRateLimitScript.setResultType(List.class);
    }

    @Override
    public RateLimitResult acquireOrThrow(TenantAuthInfo tenant, String requestId, int estimatedTokens) {
        RateLimitResult result = tryAcquire(tenant, requestId, estimatedTokens);
        if (!result.isAllowed()) {
            String message;
            if ("RPM".equals(result.getRejectedBy())) {
                message = String.format("RPM rate limit exceeded for tenant %s (currentRpm=%d, rpmLimit=%d).",
                        tenant.getTenantId(), result.getCurrentRpm(), resolveRpmLimit(tenant));
            } else {
                message = String.format("TPM rate limit exceeded for tenant %s (remainingTpm=%d, requestedTokens=%d, tpmLimit=%d).",
                        tenant.getTenantId(), result.getRemainingTpm(), estimatedTokens, resolveTpmLimit(tenant));
            }
            throw new GatewayException(ErrorCode.RATE_LIMIT_EXCEEDED, message);
        }
        return result;
    }

    /**
     * 执行单次原子 RPM + TPM 检查并返还详细限流快照，不主动抛出业务限流异常。
     */
    public RateLimitResult tryAcquire(TenantAuthInfo tenant, String requestId, int estimatedTokens) {
        if (tenant == null || tenant.getTenantId() == null || tenant.getTenantId().isBlank()) {
            throw new GatewayException(ErrorCode.INVALID_API_KEY, "Missing tenant context for rate limiting.");
        }
        if (estimatedTokens < 0) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "estimatedTokens must not be negative.");
        }

        String tenantId = tenant.getTenantId();
        int rpmLimit = resolveRpmLimit(tenant);
        int tpmLimit = resolveTpmLimit(tenant);
        String baseRequestId = (requestId == null || requestId.isBlank())
                ? "req-" + UUID.randomUUID().toString().replace("-", "")
                : requestId;
        // ZSet member 采用 requestId + 随机 nonce 避免相同 ID 覆盖滑动窗口计数
        String nonce = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String zsetMember = baseRequestId + ":" + nonce;

        List<String> keys = Arrays.asList(
                RedisKeys.rateLimitRpmKey(tenantId),
                RedisKeys.rateLimitTpmKey(tenantId)
        );

        long nowMs = System.currentTimeMillis();
        long startNanos = System.nanoTime();
        List<?> rawResult = stringRedisTemplate.execute(
                dualRateLimitScript,
                keys,
                String.valueOf(nowMs),
                String.valueOf(rpmLimit),
                String.valueOf(tpmLimit),
                String.valueOf(estimatedTokens),
                zsetMember,
                String.valueOf(WINDOW_MS),
                String.valueOf(KEY_EXPIRE_MS)
        );
        long latencyMicros = Math.max(1L, (System.nanoTime() - startNanos) / 1_000L);

        if (rawResult == null || rawResult.size() < 4) {
            throw new GatewayException(ErrorCode.INTERNAL_ERROR, "Rate limit Lua script returned invalid response.");
        }

        boolean allowed = toLong(rawResult.get(0)) == 1L;
        long rejectCode = toLong(rawResult.get(1));
        long currentRpm = toLong(rawResult.get(2));
        long remainingTpm = toLong(rawResult.get(3));

        String rejectedBy = "NONE";
        if (!allowed) {
            rejectedBy = (rejectCode == REJECT_CODE_RPM) ? "RPM"
                    : (rejectCode == REJECT_CODE_TPM ? "TPM" : "UNKNOWN");
        }

        return RateLimitResult.builder()
                .allowed(allowed)
                .rejectedBy(rejectedBy)
                .currentRpm(currentRpm)
                .remainingTpm(remainingTpm)
                .latencyMicros(latencyMicros)
                .build();
    }

    private int resolveRpmLimit(TenantAuthInfo tenant) {
        return tenant.getRpmLimit() != null ? tenant.getRpmLimit() : DEFAULT_RPM_LIMIT;
    }

    private int resolveTpmLimit(TenantAuthInfo tenant) {
        return tenant.getTpmLimit() != null ? tenant.getTpmLimit() : DEFAULT_TPM_LIMIT;
    }

    private long toLong(Object val) {
        if (val instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(val));
    }
}
