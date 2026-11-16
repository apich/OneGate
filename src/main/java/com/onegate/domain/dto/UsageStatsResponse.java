package com.onegate.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 租户调用量统计响应 (由覆盖索引 idx_tenant_model_time_tokens 直接聚合，零回表)。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UsageStatsResponse {

    private String tenantId;

    private String modelName;

    private Long requestCount;

    private Long totalTokens;

    /** SQL 执行耗时 (毫秒) */
    private Long queryElapsedMs;
}
