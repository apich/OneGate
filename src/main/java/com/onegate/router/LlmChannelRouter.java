package com.onegate.router;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.entity.LlmChannelKey;
import com.onegate.domain.mapper.LlmChannelKeyMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 上游大模型渠道智能路由器：
 * 按逻辑模型查询启用渠道 -> 过滤 Redis 熔断冷却中渠道 -> 主备优先级分层 + 同优先级加权排序。
 */
@Slf4j
@Service
public class LlmChannelRouter {

    private final LlmChannelKeyMapper llmChannelKeyMapper;
    private final ChannelCircuitBreaker circuitBreaker;
    private final Cache<String, List<LlmChannelKey>> modelChannelCache;

    public LlmChannelRouter(LlmChannelKeyMapper llmChannelKeyMapper, ChannelCircuitBreaker circuitBreaker) {
        this.llmChannelKeyMapper = llmChannelKeyMapper;
        this.circuitBreaker = circuitBreaker;
        this.modelChannelCache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(10))
                .maximumSize(1_000)
                .build();
    }

    /**
     * 为指定逻辑模型选出按优先级与权重排列的可用候选渠道列表（用于首通道及故障转移备用链）。
     */
    public List<LlmChannelKey> selectAvailableChannels(String modelName) {
        if (modelName == null || modelName.isBlank()) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "model must not be blank.");
        }

        String normalizedModel = modelName.trim();
        List<LlmChannelKey> activeChannels = getActiveChannelsCached(normalizedModel);
        if (activeChannels == null || activeChannels.isEmpty()) {
            throw new GatewayException(ErrorCode.MODEL_NOT_FOUND,
                    "No active upstream channels configured for model: " + modelName);
        }

        List<LlmChannelKey> healthyChannels = new ArrayList<>(activeChannels.size());
        for (LlmChannelKey channel : activeChannels) {
            if (!circuitBreaker.isInCooldown(channel.getId())) {
                healthyChannels.add(channel);
            } else {
                log.debug("Skipping channel in cooldown: channelId={}, channelName={}, model={}",
                        channel.getId(), channel.getChannelName(), modelName);
            }
        }

        if (healthyChannels.isEmpty()) {
            throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                    "All upstream channels for model " + modelName + " are currently in circuit-breaker cooldown.");
        }

        // 按 priority 升序分层；同一优先级内按 weight 做无放回随机加权排序，兼顾主备顺序与负载均衡
        Map<Integer, List<LlmChannelKey>> byPriority = new TreeMap<>();
        for (LlmChannelKey channel : healthyChannels) {
            int priority = channel.getPriority() != null ? channel.getPriority() : Integer.MAX_VALUE;
            byPriority.computeIfAbsent(priority, k -> new ArrayList<>()).add(channel);
        }

        List<LlmChannelKey> ordered = new ArrayList<>(healthyChannels.size());
        for (List<LlmChannelKey> tier : byPriority.values()) {
            if (tier.size() <= 1) {
                ordered.addAll(tier);
            } else {
                ordered.addAll(weightedShuffle(tier));
            }
        }
        return ordered;
    }

    public List<LlmChannelKey> listAllActiveChannels() {
        return llmChannelKeyMapper.selectAllActive();
    }

    /**
     * 驱逐指定模型的本地渠道路由缓存（渠道管理变更后可主动调用）。
     */
    public void evictModelCache(String modelName) {
        if (modelName != null && !modelName.isBlank()) {
            modelChannelCache.invalidate(modelName.trim());
        }
    }

    /**
     * 清空全部本地渠道路由缓存。
     */
    public void evictAll() {
        modelChannelCache.invalidateAll();
    }

    private List<LlmChannelKey> getActiveChannelsCached(String normalizedModel) {
        List<LlmChannelKey> cached = modelChannelCache.get(normalizedModel, key -> {
            List<LlmChannelKey> queried = llmChannelKeyMapper.selectActiveByModel(key);
            if (queried == null || queried.isEmpty()) {
                return java.util.Collections.emptyList();
            }
            return java.util.Collections.unmodifiableList(new java.util.ArrayList<>(queried));
        });
        return (cached == null || cached.isEmpty())
                ? java.util.Collections.emptyList()
                : new java.util.ArrayList<>(cached);
    }

    private List<LlmChannelKey> weightedShuffle(List<LlmChannelKey> tier) {
        List<LlmChannelKey> pool = new ArrayList<>(tier);
        List<LlmChannelKey> result = new ArrayList<>(pool.size());
        ThreadLocalRandom random = ThreadLocalRandom.current();

        while (!pool.isEmpty()) {
            long totalWeight = 0L;
            for (LlmChannelKey ch : pool) {
                totalWeight += Math.max(1, ch.getWeight() != null ? ch.getWeight() : 100);
            }
            long pick = random.nextLong(totalWeight);
            long cumulative = 0L;
            int chosenIndex = 0;
            for (int i = 0; i < pool.size(); i++) {
                cumulative += Math.max(1, pool.get(i).getWeight() != null ? pool.get(i).getWeight() : 100);
                if (pick < cumulative) {
                    chosenIndex = i;
                    break;
                }
            }
            result.add(pool.remove(chosenIndex));
        }
        return result;
    }
}
