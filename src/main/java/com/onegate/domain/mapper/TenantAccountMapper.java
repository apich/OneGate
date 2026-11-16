package com.onegate.domain.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.onegate.domain.entity.TenantAccount;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 租户账户与配额 Mapper。
 */
@Mapper
public interface TenantAccountMapper extends BaseMapper<TenantAccount> {

    /**
     * 按 API Key 精确查找租户记录 (走 uk_api_key 唯一索引)。
     */
    TenantAccount selectByApiKey(@Param("apiKey") String apiKey);

    /**
     * 按租户标识精确查找租户记录 (走 uk_tenant_id 唯一索引)。
     */
    TenantAccount selectByTenantId(@Param("tenantId") String tenantId);

    /**
     * 基于余额防超扣条件执行原子扣减:
     * {@code UPDATE tenant_account SET balance = balance - actualCost WHERE tenant_id = ? AND balance >= actualCost}
     *
     * @return 受影响行数 (1 表示扣减成功, 0 表示租户不存在或余额不足)
     */
    int deductBalanceAtomically(@Param("tenantId") String tenantId, @Param("actualCost") long actualCost);

    /**
     * 原子增加租户 Token 额度余额。
     */
    int rechargeBalance(@Param("tenantId") String tenantId, @Param("amount") long amount);
}
