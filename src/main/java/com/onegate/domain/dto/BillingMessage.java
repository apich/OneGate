package com.onegate.domain.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * RabbitMQ 异步计费与预占冻结超时释放统一消息体。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BillingMessage {

    private String requestId;

    private String tenantId;

    private String modelName;

    private String provider;

    private Long channelId;

    /** 请求入站时估算并预占冻结的 Token 数 */
    private Integer estimatedTokens;

    private Integer promptTokens;

    private Integer completionTokens;

    private Integer totalTokens;

    private Integer costMs;

    private String finishReason;

    /** 是否为客户端断连/异常触发的本地估算兜底 */
    private Boolean estimated;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private LocalDateTime requestTime;
}
