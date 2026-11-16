package com.onegate.auth;

import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.dto.TenantAuthInfo;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 网关请求鉴权拦截器 (Bean 名称固定为 {@code apiKeyAuthInterceptor} 以自动接入 {@code WebMvcConfig})。
 * <p>
 * 负责解析 Bearer API Key、调用 L1/L2 多级缓存鉴权、校验租户状态、注入请求追踪 ID 与微秒耗时响应头，
 * 并在请求结束后强制清理 {@link TenantContext}。
 */
@Component("apiKeyAuthInterceptor")
public class ApiKeyAuthInterceptor implements HandlerInterceptor {

    private static final int MAX_REQUEST_ID_LENGTH = 64;
    private static final Pattern REQUEST_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9._:-]+$");

    private final TenantAuthCacheService tenantAuthCacheService;

    public ApiKeyAuthInterceptor(TenantAuthCacheService tenantAuthCacheService) {
        this.tenantAuthCacheService = tenantAuthCacheService;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        // 防止 SSE 流式响应的 ASYNC 异步派发阶段重入鉴权拦截器
        if (request.getDispatcherType() != DispatcherType.REQUEST) {
            return true;
        }

        long startNanos = System.nanoTime();
        String apiKey = extractBearerToken(request.getHeader(HeaderConstants.AUTHORIZATION));
        if (apiKey == null) {
            throw new GatewayException(ErrorCode.INVALID_API_KEY);
        }

        TenantAuthInfo authInfo = tenantAuthCacheService.authenticate(apiKey);
        if (authInfo == null || !authInfo.isExists()) {
            throw new GatewayException(ErrorCode.INVALID_API_KEY);
        }
        if (authInfo.getStatus() == null || authInfo.getStatus() != 1) {
            throw new GatewayException(ErrorCode.TENANT_DISABLED);
        }

        String requestId = resolveRequestId(request.getHeader(HeaderConstants.REQUEST_ID_HEADER));

        TenantContext.set(authInfo);
        request.setAttribute(HeaderConstants.TENANT_AUTH_ATTR, authInfo);
        request.setAttribute(HeaderConstants.TENANT_ID_ATTR, authInfo.getTenantId());
        request.setAttribute(HeaderConstants.REQUEST_ID_ATTR, requestId);

        long authMicros = Math.max(1L, (System.nanoTime() - startNanos) / 1_000L);
        response.setHeader(HeaderConstants.REQUEST_ID_HEADER, requestId);
        response.setHeader(HeaderConstants.AUTH_LATENCY_US_HEADER, String.valueOf(authMicros));
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        TenantContext.clear();
    }

    private String extractBearerToken(String authorizationHeader) {
        if (authorizationHeader == null || !authorizationHeader.startsWith(HeaderConstants.BEARER_PREFIX)) {
            return null;
        }
        String token = authorizationHeader.substring(HeaderConstants.BEARER_PREFIX.length()).trim();
        return token.isEmpty() ? null : token;
    }

    private String resolveRequestId(String incomingRequestId) {
        if (incomingRequestId != null) {
            String trimmed = incomingRequestId.trim();
            if (trimmed.isEmpty()
                    || trimmed.length() > MAX_REQUEST_ID_LENGTH
                    || !REQUEST_ID_PATTERN.matcher(trimmed).matches()) {
                throw new GatewayException(ErrorCode.INVALID_REQUEST,
                        "Invalid X-OneGate-Request-Id header: must be non-blank, <= 64 chars, and match ^[a-zA-Z0-9._:-]+$.");
            }
            return trimmed;
        }
        return "req-" + UUID.randomUUID().toString().replace("-", "");
    }
}
