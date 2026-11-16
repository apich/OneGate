package com.onegate.domain.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 管理端动态调整租户配额/状态/API Key 请求。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdminKeyUpdateRequest {

    private String tenantName;

    @Min(value = 0, message = "status must be 0 or 1")
    @Max(value = 1, message = "status must be 0 or 1")
    private Integer status;

    @Min(value = 1, message = "rpmLimit must be positive")
    private Integer rpmLimit;

    @Min(value = 1, message = "tpmLimit must be positive")
    private Integer tpmLimit;

    /** 可选显式指定新 API Key */
    @Size(min = 12, max = 128, message = "apiKey length must be between 12 and 128")
    private String apiKey;

    /** 为 true 时自动轮换生成新的随机 API Key */
    private Boolean rotateApiKey;
}
