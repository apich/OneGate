package com.onegate.domain.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.onegate.domain.entity.LlmChannelKey;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 上游大模型渠道池 Mapper。
 */
@Mapper
public interface LlmChannelKeyMapper extends BaseMapper<LlmChannelKey> {

    /**
     * 按逻辑模型名称查询所有已启用渠道，按优先级(主优先)与权重降序排列。
     * 命中联合索引 idx_model_status_priority (model_name, status, priority)。
     */
    List<LlmChannelKey> selectActiveByModel(@Param("modelName") String modelName);

    /**
     * 查询所有已启用渠道列表 (用于渠道健康度巡检/展示)。
     */
    List<LlmChannelKey> selectAllActive();
}
