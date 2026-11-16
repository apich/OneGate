package com.onegate.router;

import com.onegate.common.config.OneGateProperties;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于厂商维度的 JUC 舱壁隔离线程池管理器。
 * <p>
 * 为不同上游模型厂商 ({@code DEEPSEEK} / {@code QWEN} / {@code OPENAI} / {@code MOCK} 等) 分配独立的
 * {@link ThreadPoolExecutor}（有界 {@link ArrayBlockingQueue} + 自定义命名线程工厂 + {@link ThreadPoolExecutor.AbortPolicy}），
 * 防止单一慢渠道或故障渠道耗尽全网关线程资源引发级联雪崩。
 */
@Slf4j
@Component
public class ProviderPoolManager {

    private final OneGateProperties properties;
    private final ConcurrentHashMap<String, ThreadPoolExecutor> pools = new ConcurrentHashMap<>();

    public ProviderPoolManager(OneGateProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void initConfiguredPools() {
        if (properties.getPools() != null) {
            for (String key : properties.getPools().keySet()) {
                String normalized = normalizeProvider(key);
                pools.computeIfAbsent(normalized, this::createExecutor);
            }
        }
        log.info("Initialized provider isolated thread pools: {}", pools.keySet());
    }

    /**
     * 在指定厂商的隔离线程池中提交执行任务；若队列与最大线程均已满载则快速失败抛出 503。
     */
    public void execute(String provider, Runnable task) {
        String normalized = normalizeProvider(provider);
        ThreadPoolExecutor executor = getExecutor(normalized);
        try {
            executor.execute(task);
        } catch (RejectedExecutionException ex) {
            log.warn("Provider thread pool saturated, rejecting task: provider={}, active={}/{}, queueSize={}",
                    normalized, executor.getActiveCount(), executor.getMaximumPoolSize(), executor.getQueue().size());
            throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                    "Upstream provider thread pool saturated for " + normalized, ex);
        }
    }

    public ThreadPoolExecutor getExecutor(String provider) {
        String normalized = normalizeProvider(provider);
        return pools.computeIfAbsent(normalized, this::createExecutor);
    }

    public Map<String, ThreadPoolExecutor> getPoolsSnapshot() {
        return Collections.unmodifiableMap(pools);
    }

    private ThreadPoolExecutor createExecutor(String normalizedProvider) {
        OneGateProperties.ThreadPoolSpec spec = properties.getPoolSpec(normalizedProvider);
        int coreSize = Math.max(1, spec.getCoreSize());
        int maxSize = Math.max(coreSize, spec.getMaxSize());
        int queueCapacity = Math.max(1, spec.getQueueCapacity());
        long keepAliveSeconds = Math.max(1L, spec.getKeepAliveSeconds());

        String threadPrefix = "onegate-pool-" + normalizedProvider.toLowerCase(Locale.ROOT) + "-";
        AtomicInteger seq = new AtomicInteger(1);
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, threadPrefix + seq.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                coreSize,
                maxSize,
                keepAliveSeconds,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy()
        );
        log.info("Created isolated ThreadPoolExecutor [provider={}, core={}, max={}, queueCapacity={}]",
                normalizedProvider, coreSize, maxSize, queueCapacity);
        return executor;
    }

    private String normalizeProvider(String provider) {
        if (provider == null || provider.isBlank()) {
            return "DEFAULT";
        }
        return provider.trim().toUpperCase(Locale.ROOT);
    }

    @PreDestroy
    public void shutdown() {
        pools.forEach((provider, executor) -> {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        });
    }
}
