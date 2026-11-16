package com.onegate.domain.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.onegate.domain.entity.BillingDeadLetterRecord;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 计费死信补偿记录 Mapper。
 */
@Mapper
public interface BillingDeadLetterMapper extends BaseMapper<BillingDeadLetterRecord> {

    /**
     * 查询待补偿处理的死信记录 (状态为 PENDING)。
     */
    List<BillingDeadLetterRecord> selectPending(@Param("limit") int limit);

    /**
     * 将死信补偿记录状态更新为 RESOLVED。
     */
    int markResolved(@Param("id") Long id);
}
