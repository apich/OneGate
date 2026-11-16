package com.onegate.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Redis + Lua 双指标原子限流返回结果 (RPM 滑动窗口 + TPM 令牌桶)。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitResult {

    /** 是否允许放行 */
    private boolean allowed;

    /** 触发拦截的维度: NONE / RPM / TPM */
    private String rejectedBy;

    /** 当前窗口内 RPM 计数 */
    private long currentRpm;

    /** 当前剩余 TPM 令牌数 */
    private long remainingTpm;

    /** 本次 Lua 原子脚本执行耗时 (微秒) */
    private long latencyMicros;
}
