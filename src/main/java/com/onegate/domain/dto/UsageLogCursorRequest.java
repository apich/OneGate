package com.onegate.domain.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 账单明细复合游标深分页查询请求。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UsageLogCursorRequest {

    @NotBlank(message = "tenantId must not be blank")
    private String tenantId;

    private String modelName;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private LocalDateTime lastCreateTime;

    private Long lastId;

    @Builder.Default
    @Min(value = 1, message = "pageSize must be at least 1")
    @Max(value = 200, message = "pageSize must not exceed 200")
    private Integer pageSize = 20;
}
