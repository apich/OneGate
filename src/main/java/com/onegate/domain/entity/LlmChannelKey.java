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
 * 上游大模型渠道池实体，映射表 {@code llm_channel_key}。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("llm_channel_key")
public class LlmChannelKey {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String channelName;

    /** 上游模型厂商: DEEPSEEK / QWEN / OPENAI / MOCK */
    private String provider;

    /** 逻辑模型名称: 如 deepseek-chat / qwen-plus / gpt-4o-mini */
    private String modelName;

    private String baseUrl;

    private String secretKey;

    /** 加权轮询权重 */
    private Integer weight;

    /** 优先级: 1-主渠道, 2-备用渠道 */
    private Integer priority;

    /** 渠道状态: 1-启用, 0-停用 */
    private Integer status;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;
}
