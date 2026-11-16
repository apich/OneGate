package com.onegate.auth;

import com.onegate.common.constant.RedisKeys;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * 认证缓存 Pub/Sub 订阅容器配置。
 */
@Configuration
public class AuthPubSubConfig {

    @Bean
    public RedisMessageListenerContainer authRedisMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            KeyInvalidationSubscriber keyInvalidationSubscriber) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(keyInvalidationSubscriber, new ChannelTopic(RedisKeys.INVALIDATE_CHANNEL));
        return container;
    }
}
