package com.onegate.auth;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Redis Pub/Sub 集群失效广播订阅器:
 * 监听 {@link com.onegate.common.constant.RedisKeys#INVALIDATE_CHANNEL} 频道，
 * 收到管理端广播的失效 API Key 后立即清除当前网关实例的本地 L1 Caffeine 缓存。
 */
@Slf4j
@Component
public class KeyInvalidationSubscriber implements MessageListener {

    private final TenantAuthCacheService tenantAuthCacheService;

    public KeyInvalidationSubscriber(TenantAuthCacheService tenantAuthCacheService) {
        this.tenantAuthCacheService = tenantAuthCacheService;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        if (message == null || message.getBody() == null || message.getBody().length == 0) {
            return;
        }
        try {
            String apiKey = new String(message.getBody(), StandardCharsets.UTF_8).trim();
            // 防御性清理意外带引号的消息体
            if (apiKey.length() >= 2 && apiKey.startsWith("\"") && apiKey.endsWith("\"")) {
                apiKey = apiKey.substring(1, apiKey.length() - 1).trim();
            }
            if (!apiKey.isBlank()) {
                tenantAuthCacheService.evictLocalCache(apiKey);
                log.debug("Evicted local L1 auth cache via Redis Pub/Sub for key suffix={}", maskApiKey(apiKey));
            }
        } catch (Exception ex) {
            log.warn("Failed to process auth invalidation Pub/Sub message: {}", ex.getMessage());
        }
    }

    private String maskApiKey(String apiKey) {
        if (apiKey == null || apiKey.length() <= 6) {
            return "***";
        }
        return "***" + apiKey.substring(apiKey.length() - 6);
    }
}
