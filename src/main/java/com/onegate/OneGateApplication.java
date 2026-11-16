package com.onegate;

import com.onegate.common.config.OneGateProperties;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * OneGate 高并发大模型 API 统一网关启动入口。
 */
@SpringBootApplication
@MapperScan("com.onegate.domain.mapper")
@EnableConfigurationProperties(OneGateProperties.class)
public class OneGateApplication {

    public static void main(String[] args) {
        SpringApplication.run(OneGateApplication.class, args);
    }
}
