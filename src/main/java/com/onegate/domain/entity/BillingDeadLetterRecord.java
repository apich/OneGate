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
 * 计费死信队列落库补偿实体，映射表 {@code billing_dead_letter_record}。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("billing_dead_letter_record")
public class BillingDeadLetterRecord {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String requestId;

    private String tenantId;

    private String payload;

    private String errorReason;

    /** 状态: PENDING / RESOLVED */
    private String status;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;
}
