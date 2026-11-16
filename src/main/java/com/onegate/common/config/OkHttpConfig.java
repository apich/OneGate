package com.onegate.common.config;

import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * 上游大模型代理专用 OkHttp4 全局连接池与调度器配置。
 * <p>
 * 调大 Dispatcher 单主机并发阈值 (默认仅 5)，防止高并发网关转发时在客户端队列发生隐式排队；
 * 关闭 OkHttp 自动重试，将主备切换控制权完全交给网关路由层与 TTFT 熔断机制。
 */
@Configuration
public class OkHttpConfig {

    @Bean
    public OkHttpClient okHttpClient(OneGateProperties properties) {
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(256);
        dispatcher.setMaxRequestsPerHost(64);

        return new OkHttpClient.Builder()
                .dispatcher(dispatcher)
                .connectionPool(new ConnectionPool(128, 5, TimeUnit.MINUTES))
                .connectTimeout(properties.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(properties.getReadTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(15, TimeUnit.SECONDS)
                .followRedirects(false)
                .followSslRedirects(false)
                .retryOnConnectionFailure(false)
                .build();
    }
}
