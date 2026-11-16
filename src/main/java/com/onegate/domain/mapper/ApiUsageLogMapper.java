package com.onegate.domain.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.onegate.domain.dto.UsageStatsResponse;
import com.onegate.domain.entity.ApiUsageLog;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.List;

/**
 * API 调用计费流水 Mapper (覆盖索引聚合 + 复合游标深分页 + 传统 Offset 对比)。
 */
@Mapper
public interface ApiUsageLogMapper extends BaseMapper<ApiUsageLog> {

    /**
     * 基于 uk_request_id 唯一键执行幂等插入 (INSERT IGNORE)，防止 MQ 重复投递导致重复计费流水。
     *
     * @return 插入行数 (1-新写入, 0-幂等去重忽略)
     */
    int insertIgnore(@Param("log") ApiUsageLog log);

    /**
     * 基于联合覆盖索引 idx_tenant_model_time_tokens (tenant_id, model_name, create_time, total_tokens)
     * 直接聚合请求数与总 Token 消耗，全程 Using index 零回表。
     */
    UsageStatsResponse selectCoveringStats(
            @Param("tenantId") String tenantId,
            @Param("modelName") String modelName,
            @Param("startTime") LocalDateTime startTime,
            @Param("endTime") LocalDateTime endTime
    );

    /**
     * 基于复合游标 (create_time, id) &lt; (#{lastCreateTime}, #{lastId}) 的高性能深分页查询。
     * 命中索引 idx_tenant_create_time (tenant_id, create_time) 及其隐式聚簇主键后缀 id。
     */
    List<ApiUsageLog> selectByCursor(
            @Param("tenantId") String tenantId,
            @Param("modelName") String modelName,
            @Param("lastCreateTime") LocalDateTime lastCreateTime,
            @Param("lastId") Long lastId,
            @Param("pageSize") int pageSize
    );

    /**
     * 传统 LIMIT offset, pageSize 慢查询对照方法，用于深分页基准压测对比。
     */
    List<ApiUsageLog> selectByTraditionalOffset(
            @Param("tenantId") String tenantId,
            @Param("modelName") String modelName,
            @Param("offset") long offset,
            @Param("pageSize") int pageSize
    );

    /**
     * 统计指定租户的调用流水总数。
     */
    long countByTenant(@Param("tenantId") String tenantId);
}
