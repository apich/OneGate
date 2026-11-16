package com.onegate.common.config;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Arrays;

/**
 * Spring MVC 全局路由与拦截器配置。
 * <p>
 * 通过 {@link ObjectProvider} 可选注入 {@code adminAuthInterceptor} ({@code /admin/**})
 * 与 {@code apiKeyAuthInterceptor} ({@code /v1/**})，并基于 {@link OneGateProperties#getCorsAllowedOrigins()}
 * 收紧跨域允许源列表。
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    private final ObjectProvider<HandlerInterceptor> apiKeyAuthInterceptorProvider;
    private final ObjectProvider<HandlerInterceptor> adminAuthInterceptorProvider;
    private final OneGateProperties properties;

    public WebMvcConfig(
            @Qualifier("apiKeyAuthInterceptor") ObjectProvider<HandlerInterceptor> apiKeyAuthInterceptorProvider,
            @Qualifier("adminAuthInterceptor") ObjectProvider<HandlerInterceptor> adminAuthInterceptorProvider,
            OneGateProperties properties) {
        this.apiKeyAuthInterceptorProvider = apiKeyAuthInterceptorProvider;
        this.adminAuthInterceptorProvider = adminAuthInterceptorProvider;
        this.properties = properties;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        HandlerInterceptor adminInterceptor = adminAuthInterceptorProvider.getIfAvailable();
        if (adminInterceptor != null) {
            registry.addInterceptor(adminInterceptor)
                    .addPathPatterns("/admin/**")
                    .order(0);
        }

        HandlerInterceptor authInterceptor = apiKeyAuthInterceptorProvider.getIfAvailable();
        if (authInterceptor != null) {
            registry.addInterceptor(authInterceptor)
                    .addPathPatterns("/v1/**")
                    .excludePathPatterns("/mock-llm/**", "/admin/**", "/actuator/**", "/error")
                    .order(1);
        }
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        String rawOrigins = properties.getCorsAllowedOrigins();
        String[] allowedPatterns = (rawOrigins == null || rawOrigins.isBlank())
                ? new String[0]
                : Arrays.stream(rawOrigins.split(","))
                        .map(String::trim)
                        .filter(origin -> !origin.isEmpty())
                        .toArray(String[]::new);

        registry.addMapping("/**")
                .allowedOriginPatterns(allowedPatterns)
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .maxAge(3600);
    }
}
