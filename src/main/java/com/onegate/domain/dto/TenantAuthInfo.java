package com.onegate.domain.dto;

import com.onegate.domain.entity.TenantAccount;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 租户认证多级缓存快照 (L1 Caffeine + L2 Redis)。
 * <p>
 * 通过 {@code exists} 标记支持空值缓存 (Negative Cache)，防止非法 API Key 穿透查询 MySQL。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TenantAuthInfo {

    /** 是否存在有效租户记录: false 表示空值缓存占位对象 */
    private boolean exists;

    private String tenantId;

    private String tenantName;

    private String apiKey;

    /** 状态: 1-正常, 0-禁用 */
    private Integer status;

    private Integer rpmLimit;

    private Integer tpmLimit;

    private Long balance;

    public static TenantAuthInfo notFound(String apiKey) {
        return TenantAuthInfo.builder()
                .exists(false)
                .apiKey(apiKey)
                .build();
    }

    public static TenantAuthInfo fromEntity(TenantAccount account) {
        if (account == null) {
            return notFound(null);
        }
        return TenantAuthInfo.builder()
                .exists(true)
                .tenantId(account.getTenantId())
                .tenantName(account.getTenantName())
                .apiKey(account.getApiKey())
                .status(account.getStatus())
                .rpmLimit(account.getRpmLimit())
                .tpmLimit(account.getTpmLimit())
                .balance(account.getBalance())
                .build();
    }
}
