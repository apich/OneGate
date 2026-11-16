package com.onegate.domain.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 管理端租户 Token 额度充值请求。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdminRechargeRequest {

    @NotNull(message = "amount must not be null")
    @Min(value = 1, message = "amount must be at least 1")
    private Long amount;

    private String remark;
}
