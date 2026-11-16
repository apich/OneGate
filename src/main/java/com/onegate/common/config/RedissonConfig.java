package com.onegate.common.config;

import org.redisson.spring.starter.RedissonAutoConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 客户端连接池与超时调优配置。
 * 复用 spring.data.redis.* 基础连接参数，避免在多处重复硬编码主机/端口。
 */
@Configuration
public class RedissonConfig {

    @Bean
    public RedissonAutoConfigurationCustomizer redissonAutoConfigurationCustomizer() {
        return config -> {
            if (!config.isClusterConfig()) {
                var singleConfig = config.useSingleServer()
                        .setConnectionPoolSize(64)
                        .setConnectionMinimumIdleSize(8)
                        .setConnectTimeout(3000)
                        .setTimeout(3000);
                // Spring ${REDIS_PASSWORD:} 未设置环境变量时会注入空串 ""，清理为 null 避免无密码 Redis 返回 ERR AUTH
                if (singleConfig.getPassword() != null && singleConfig.getPassword().isBlank()) {
                    singleConfig.setPassword(null);
                }
            }
        };
    }
}
