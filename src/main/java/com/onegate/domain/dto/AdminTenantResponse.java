package com.onegate.domain.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.onegate.domain.entity.TenantAccount;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 管理端租户查询/更新/充值脱敏响应 DTO。
 * <p>
 * {@code apiKey} 仅在创建、密钥轮换或显式更新密钥时返回一次完整明文；
 * 查询与充值接口一律保持为 {@code null}，仅返回脱敏后的 {@code apiKeyMasked} (如 {@code sk-****1001})。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdminTenantResponse {

    private String tenantId;

    private String tenantName;

    private Long balance;

    private Integer rpmLimit;

    private Integer tpmLimit;

    private Integer status;

    private String apiKeyMasked;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String apiKey;

    public static AdminTenantResponse fromEntityMasked(TenantAccount account) {
        return buildBase(account).apiKey(null).build();
    }

    public static AdminTenantResponse fromEntityWithPlaintextKey(TenantAccount account) {
        return buildBase(account).apiKey(account.getApiKey()).build();
    }

    private static AdminTenantResponseBuilder buildBase(TenantAccount account) {
        return AdminTenantResponse.builder()
                .tenantId(account.getTenantId())
                .tenantName(account.getTenantName())
                .balance(account.getBalance())
                .rpmLimit(account.getRpmLimit())
                .tpmLimit(account.getTpmLimit())
                .status(account.getStatus())
                .apiKeyMasked(maskApiKey(account.getApiKey()));
    }

    public static String maskApiKey(String rawKey) {
        if (rawKey == null || rawKey.isBlank()) {
            return "";
        }
        String trimmed = rawKey.trim();
        if (trimmed.length() <= 7) {
            return "****";
        }
        return trimmed.substring(0, 3) + "****" + trimmed.substring(trimmed.length() - 4);
    }
}
