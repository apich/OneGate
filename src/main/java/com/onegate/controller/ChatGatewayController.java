package com.onegate.controller;

import com.onegate.auth.TenantContext;
import com.onegate.common.config.OneGateProperties;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.dto.ChatCompletionRequest;
import com.onegate.domain.dto.TenantAuthInfo;
import com.onegate.domain.entity.LlmChannelKey;
import com.onegate.router.LlmChannelRouter;
import com.onegate.router.LlmStreamingProxyService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * OpenAI 兼容统一大模型网关入口控制器 ({@code /v1/chat/completions}, {@code /v1/models})。
 * <p>
 * 全量请求受 {@code ApiKeyAuthInterceptor} 鉴权保护，通过 {@link HttpServletRequest} 属性传递租户上下文与全局 {@code requestId}，
 * 避免异步线程池或 SSE 异步派发阶段丢失 {@code ThreadLocal}。
 */
@RestController
@RequestMapping("/v1")
public class ChatGatewayController {

    private static final Pattern REQUEST_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9._:-]{1,64}$");

    private static final List<String> PASSTHROUGH_MOCK_HEADERS = List.of(
            MockUpstreamController.HEADER_MOCK_MODE,
            MockUpstreamController.HEADER_MOCK_PRIMARY_MODE,
            MockUpstreamController.HEADER_MOCK_BACKUP_MODE,
            MockUpstreamController.HEADER_MOCK_DELAY_MS,
            MockUpstreamController.HEADER_MOCK_PRIMARY_DELAY_MS,
            MockUpstreamController.HEADER_MOCK_CHUNK_COUNT,
            MockUpstreamController.HEADER_MOCK_CHUNK_DELAY_MS
    );

    private final LlmStreamingProxyService proxyService;
    private final LlmChannelRouter channelRouter;
    private final OneGateProperties properties;

    public ChatGatewayController(
            LlmStreamingProxyService proxyService,
            LlmChannelRouter channelRouter,
            OneGateProperties properties) {
        this.proxyService = proxyService;
        this.channelRouter = channelRouter;
        this.properties = properties;
    }

    @GetMapping("/models")
    public ResponseEntity<Map<String, Object>> listModels() {
        List<LlmChannelKey> activeChannels = channelRouter.listAllActiveChannels();
        Map<String, Map<String, Object>> uniqueModels = new LinkedHashMap<>();
        for (LlmChannelKey channel : activeChannels) {
            uniqueModels.computeIfAbsent(channel.getModelName(), modelName -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("id", modelName);
                item.put("object", "model");
                item.put("owned_by", channel.getProvider());
                return item;
            });
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("object", "list");
        body.put("data", new ArrayList<>(uniqueModels.values()));
        return ResponseEntity.ok(body);
    }

    @PostMapping("/chat/completions")
    public ResponseEntity<?> chatCompletions(
            HttpServletRequest servletRequest,
            @Valid @RequestBody ChatCompletionRequest request) {
        TenantAuthInfo tenant = resolveTenant(servletRequest);
        String requestId = resolveRequestId(servletRequest);
        Map<String, String> passthroughHeaders = extractPassthroughHeaders(servletRequest);

        if (Boolean.TRUE.equals(request.getStream())) {
            LlmStreamingProxyService.StreamConnectResult result =
                    proxyService.proxyChatStream(request, tenant, requestId, passthroughHeaders);
            return ResponseEntity.ok()
                    .contentType(MediaType.TEXT_EVENT_STREAM)
                    .header(HttpHeaders.CACHE_CONTROL, "no-cache")
                    .header(HeaderConstants.REQUEST_ID_HEADER, requestId)
                    .header(HeaderConstants.PROVIDER_HEADER, result.getProvider())
                    .header(HeaderConstants.FAILOVER_HEADER, String.valueOf(result.isFailover()))
                    .body(result.getEmitter());
        }

        LlmStreamingProxyService.NonStreamResult result =
                proxyService.proxyChatCompletion(request, tenant, requestId, passthroughHeaders);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .header(HeaderConstants.REQUEST_ID_HEADER, requestId)
                .header(HeaderConstants.PROVIDER_HEADER, result.getProvider())
                .header(HeaderConstants.FAILOVER_HEADER, String.valueOf(result.isFailover()))
                .body(result.getResponse());
    }

    private TenantAuthInfo resolveTenant(HttpServletRequest request) {
        Object attr = request.getAttribute(HeaderConstants.TENANT_AUTH_ATTR);
        if (attr instanceof TenantAuthInfo authInfo && authInfo.isExists()) {
            return authInfo;
        }
        TenantAuthInfo contextTenant = TenantContext.get();
        if (contextTenant != null && contextTenant.isExists()) {
            return contextTenant;
        }
        throw new GatewayException(ErrorCode.INVALID_API_KEY, "Missing tenant authentication attribute.");
    }

    private String resolveRequestId(HttpServletRequest request) {
        String header = request.getHeader(HeaderConstants.REQUEST_ID_HEADER);
        if (header != null && !header.isBlank()) {
            String trimmed = header.trim();
            validateRequestId(trimmed);
            return trimmed;
        }
        Object attr = request.getAttribute(HeaderConstants.REQUEST_ID_ATTR);
        if (attr instanceof String id && !id.isBlank()) {
            String trimmed = id.trim();
            validateRequestId(trimmed);
            return trimmed;
        }
        return "req-" + UUID.randomUUID().toString().replace("-", "");
    }

    private void validateRequestId(String requestId) {
        if (!REQUEST_ID_PATTERN.matcher(requestId).matches()) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "Invalid X-OneGate-Request-Id: length must be 1..64 and contain only [a-zA-Z0-9._:-].");
        }
    }

    private Map<String, String> extractPassthroughHeaders(HttpServletRequest request) {
        if (!properties.isMockEnabled()) {
            return Collections.emptyMap();
        }
        Map<String, String> headers = new LinkedHashMap<>();
        for (String name : PASSTHROUGH_MOCK_HEADERS) {
            String value = request.getHeader(name);
            if (value != null && !value.isBlank()) {
                headers.put(name, value.trim());
            }
        }
        return headers;
    }
}
