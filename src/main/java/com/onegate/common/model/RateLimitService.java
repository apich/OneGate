package com.onegate.common.model;

import com.onegate.domain.dto.TenantAuthInfo;

/**
 * 鉴权与双指标原子限流契约接口。
 * <p>
 * 用于解耦认证限流模块与网关转发控制层，确保单次原子 Lua 调用完成 RPM + TPM 双维校验。
 */
public interface RateLimitService {

    /**
     * 对指定租户执行单次原子 RPM（滑动窗口）+ TPM（令牌桶）校验与扣减；
     * 若超出阈值则直接抛出 {@link com.onegate.common.exception.GatewayException} (HTTP 429)。
     *
     * @param tenant          已通过 L1/L2 认证的租户信息
     * @param requestId       全局唯一请求 ID
     * @param estimatedTokens 预估消耗 Token 数 (Prompt + max_tokens)
     * @return 限流通过后的配额快照与耗时指标
     */
    RateLimitResult acquireOrThrow(TenantAuthInfo tenant, String requestId, int estimatedTokens);
}
