package com.onegate.domain.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 多租户账户与配额实体，映射表 {@code tenant_account}。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("tenant_account")
public class TenantAccount {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String tenantId;

    private String tenantName;

    private String apiKey;

    /** 可用 Token 额度余额 (与 SQL 原子扣减条件 balance >= actual_cost 对应) */
    private Long balance;

    /** 预占冻结中的 Token 额度 (对账参考) */
    private Long frozenTokens;

    /** 每分钟最大请求数 (RPM) */
    private Integer rpmLimit;

    /** 每分钟最大 Token 数 (TPM) */
    private Integer tpmLimit;

    /** 状态: 1-正常, 0-禁用 */
    private Integer status;

    /** MyBatis-Plus 乐观锁版本号 */
    @Version
    private Integer version;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;
}
