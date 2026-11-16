package com.onegate.router;

import com.onegate.common.config.OneGateProperties;
import com.onegate.common.constant.RedisKeys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;

/**
 * 基于 Redis 分布式状态的渠道连续失败熔断器。
 * <p>
 * 60 秒固定计数窗口内连续失败达到 {@code onegate.failure-threshold}（默认 3 次）后，
 * 立即写入短时冷却标记键 {@code onegate:cb:cooldown:{channelId}}（TTL = {@code cooldown-seconds}），
 * 并在成功后清理失败与冷却状态。
 */
@Slf4j
@Component
public class ChannelCircuitBreaker {

    private static final long FAIL_WINDOW_SECONDS = 60L;

    private static final DefaultRedisScript<Long> RECORD_FAILURE_SCRIPT = new DefaultRedisScript<>(
            "local c = redis.call('INCR', KEYS[1]); "
                    + "local threshold = tonumber(ARGV[2]); "
                    + "if c >= threshold then "
                    + "  redis.call('SET', KEYS[2], '1', 'EX', tonumber(ARGV[3])); "
                    + "  redis.call('DEL', KEYS[1]); "
                    + "elseif c == 1 or redis.call('TTL', KEYS[1]) < 0 then "
                    + "  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1])); "
                    + "end; "
                    + "return c;",
            Long.class
    );

    private final StringRedisTemplate stringRedisTemplate;
    private final OneGateProperties properties;

    public ChannelCircuitBreaker(StringRedisTemplate stringRedisTemplate, OneGateProperties properties) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.properties = properties;
    }

    /**
     * 判断渠道当前是否处于熔断冷却期。
     */
    public boolean isInCooldown(Long channelId) {
        if (channelId == null) {
            return false;
        }
        return Boolean.TRUE.equals(stringRedisTemplate.hasKey(RedisKeys.channelCooldownKey(channelId)));
    }

    /**
     * 原子记录一次渠道失败；若达到连续失败阈值则开启冷却期并重置失败计数。
     *
     * @return {@code true} 本次失败触发了渠道熔断进入冷却期
     */
    public boolean recordFailure(Long channelId) {
        if (channelId == null) {
            return false;
        }
        String failKey = RedisKeys.channelFailKey(channelId);
        String cooldownKey = RedisKeys.channelCooldownKey(channelId);
        int threshold = Math.max(1, properties.getFailureThreshold());
        long cooldownSeconds = Math.max(1L, properties.getCooldownSeconds());
        Long count = stringRedisTemplate.execute(
                RECORD_FAILURE_SCRIPT,
                Arrays.asList(failKey, cooldownKey),
                String.valueOf(FAIL_WINDOW_SECONDS),
                String.valueOf(threshold),
                String.valueOf(cooldownSeconds)
        );
        if (count != null && count >= threshold) {
            log.warn("Circuit breaker tripped for channelId={} after {} consecutive failures; cooldown={}s",
                    channelId, count, cooldownSeconds);
            return true;
        }
        log.debug("Recorded failure for channelId={}: count={}/{}", channelId, count, threshold);
        return false;
    }

    /**
     * 渠道成功响应后清零连续失败计数与冷却标记。
     */
    public void recordSuccess(Long channelId) {
        if (channelId == null) {
            return;
        }
        stringRedisTemplate.delete(Arrays.asList(
                RedisKeys.channelFailKey(channelId),
                RedisKeys.channelCooldownKey(channelId)
        ));
    }

    public long getFailureCount(Long channelId) {
        if (channelId == null) {
            return 0L;
        }
        String val = stringRedisTemplate.opsForValue().get(RedisKeys.channelFailKey(channelId));
        return val != null ? Long.parseLong(val) : 0L;
    }
}
