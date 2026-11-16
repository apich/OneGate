package com.onegate.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * API 调用计量与计费流水实体，映射表 {@code api_usage_log}。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("api_usage_log")
public class ApiUsageLog {

    @TableId(type = IdType.AUTO)
    private Long id;

    /** 网关全局唯一请求ID (对应 uk_request_id 幂等键) */
    private String requestId;

    private String tenantId;

    private String modelName;

    private String provider;

    private Long channelId;

    private Integer promptTokens;

    private Integer completionTokens;

    private Integer totalTokens;

    /** 全链路请求耗时 (毫秒) */
    private Integer costMs;

    private String finishReason;

    /** 是否兜底估算: 0-精确解析, 1-本地估算 */
    private Integer estimated;

    private LocalDateTime createTime;
}
