package com.onegate.router;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.billing.BillingMessageProducer;
import com.onegate.billing.QuotaFreezeLuaService;
import com.onegate.common.config.OneGateProperties;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.common.model.RateLimitService;
import com.onegate.controller.MockUpstreamController;
import com.onegate.domain.dto.BillingMessage;
import com.onegate.domain.dto.ChatCompletionRequest;
import com.onegate.domain.dto.ChatCompletionResponse;
import com.onegate.domain.dto.TenantAuthInfo;
import com.onegate.domain.entity.LlmChannelKey;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 大模型统一流式/非流式网关转发与高可用容灾代理服务。
 * <p>
 * 核心保障：<br>
 * 1. 请求准入：本地 Prompt+max_tokens 估算 -> 单次 Lua 原子 RPM+TPM 限流 -> Lua 额度预占冻结 + RabbitMQ 原生 TTL 延迟兜底。<br>
 * 2. 舱壁隔离：按实际命中渠道的 {@code provider} 投递至独立有界 {@link ProviderPoolManager} 线程池执行 {@link OkHttpClient} 调用。<br>
 * 3. 首包 TTFT 容灾：基于 {@code AtomicBoolean firstChunkWritten} + 异步调度定时器，仅在尚未写出首包前发生首包超时或 429/5xx 时，
 *    调用 {@code Call.cancel()} 释放阻塞连接、记录 Redis 熔断失败并无缝切换备用渠道。<br>
 * 4. 断连中断与唯一核销：客户端主动断开 SSE（{@code onCompletion}/{@code onError}/{@code onTimeout} 或写回异常）时立即 {@code Call.cancel()}
 *    终止上游生成，按已推分片字符估算 Token，通过 {@code AtomicBoolean finalized} 保证全局有且仅有一次 Lua 核销与 MQ 账单投递。
 */
@Slf4j
@Service
public class LlmStreamingProxyService {

    private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");

    private final OkHttpClient okHttpClient;
    private final ObjectMapper objectMapper;
    private final OneGateProperties properties;
    private final RateLimitService rateLimitService;
    private final QuotaFreezeLuaService quotaFreezeLuaService;
    private final BillingMessageProducer billingMessageProducer;
    private final LlmChannelRouter llmChannelRouter;
    private final ChannelCircuitBreaker circuitBreaker;
    private final ProviderPoolManager providerPoolManager;
    private final TokenEstimator tokenEstimator;

    private final ScheduledExecutorService ttftScheduler;

    public LlmStreamingProxyService(
            OkHttpClient okHttpClient,
            ObjectMapper objectMapper,
            OneGateProperties properties,
            RateLimitService rateLimitService,
            QuotaFreezeLuaService quotaFreezeLuaService,
            BillingMessageProducer billingMessageProducer,
            LlmChannelRouter llmChannelRouter,
            ChannelCircuitBreaker circuitBreaker,
            ProviderPoolManager providerPoolManager,
            TokenEstimator tokenEstimator) {
        this.okHttpClient = okHttpClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.rateLimitService = rateLimitService;
        this.quotaFreezeLuaService = quotaFreezeLuaService;
        this.billingMessageProducer = billingMessageProducer;
        this.llmChannelRouter = llmChannelRouter;
        this.circuitBreaker = circuitBreaker;
        this.providerPoolManager = providerPoolManager;
        this.tokenEstimator = tokenEstimator;

        AtomicInteger seq = new AtomicInteger(1);
        this.ttftScheduler = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "onegate-ttft-timer-" + seq.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    public void shutdown() {
        ttftScheduler.shutdownNow();
    }

    /**
     * 非流式聊天补全代理转发 (含限流、预冻、主备切换与核销)。
     */
    public NonStreamResult proxyChatCompletion(
            ChatCompletionRequest request,
            TenantAuthInfo tenant,
            String requestId,
            Map<String, String> passthroughHeaders) {
        AdmissionContext ctx = admitAndFreeze(request, tenant, requestId);
        List<LlmChannelKey> candidates = selectChannelsOrRollback(request.getModel(), tenant.getTenantId(), requestId);

        int maxAttempts = Math.min(candidates.size(), 1 + Math.max(0, properties.getMaxFailoverAttempts()));
        String payloadJson = serializeUpstreamRequestOrRollback(request, false, tenant.getTenantId(), requestId);
        GatewayException lastGatewayException = null;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            LlmChannelKey channel = candidates.get(attempt);
            boolean failover = attempt > 0;

            CompletableFuture<ChatCompletionResponse> attemptFuture = new CompletableFuture<>();
            AtomicReference<Call> callRef = new AtomicReference<>();

            try {
                providerPoolManager.execute(channel.getProvider(), () -> {
                    try {
                        if (attemptFuture.isDone()) {
                            return;
                        }
                        Request okRequest = buildUpstreamRequest(channel, payloadJson, passthroughHeaders);
                        Call call = okHttpClient.newCall(okRequest);
                        callRef.set(call);
                        if (attemptFuture.isDone()) {
                            call.cancel();
                            return;
                        }
                        try (Response response = call.execute()) {
                            if (!response.isSuccessful()) {
                                String errBody = safeReadErrorBody(response.body());
                                attemptFuture.completeExceptionally(new UpstreamStatusException(
                                        response.code(),
                                        errBody,
                                        String.format("Upstream HTTP %d from channel %s: %s",
                                                response.code(), channel.getChannelName(), errBody)));
                                return;
                            }
                            ResponseBody body = response.body();
                            if (body == null) {
                                attemptFuture.completeExceptionally(new IOException("Empty upstream response body"));
                                return;
                            }
                            ChatCompletionResponse parsed = objectMapper.readValue(body.bytes(), ChatCompletionResponse.class);
                            attemptFuture.complete(parsed);
                        }
                    } catch (Throwable ex) {
                        attemptFuture.completeExceptionally(ex);
                    }
                });
            } catch (GatewayException poolRejectedEx) {
                lastGatewayException = poolRejectedEx;
                log.warn("Pool rejected non-stream attempt {}/{} for provider={}: {}",
                        attempt + 1, maxAttempts, channel.getProvider(), poolRejectedEx.getMessage());
                continue;
            }

            try {
                long nonStreamTimeoutMs = Math.max(100L, properties.getNonStreamTimeoutMs());
                ChatCompletionResponse response = attemptFuture
                        .orTimeout(nonStreamTimeoutMs, TimeUnit.MILLISECONDS)
                        .join();

                recordSuccessSafely(channel.getId());
                settleNonStreamBilling(ctx, request, channel, response);
                return new NonStreamResult(response, channel.getProvider(), failover);
            } catch (CompletionException compEx) {
                Throwable cause = compEx.getCause();
                if (cause instanceof UpstreamStatusException statusEx
                        && !isRetryableUpstreamStatus(statusEx.statusCode)) {
                    rollbackFrozenSafely(tenant.getTenantId(), requestId);
                    throw mapNonRetryableUpstreamException(statusEx.statusCode, channel, statusEx.responseBody);
                }
                if (cause instanceof TimeoutException) {
                    Call call = callRef.get();
                    if (call != null) {
                        call.cancel();
                    }
                    lastGatewayException = new GatewayException(ErrorCode.UPSTREAM_ERROR,
                            "Upstream timeout (" + properties.getNonStreamTimeoutMs() + "ms) on channel "
                                    + channel.getChannelName(), cause);
                } else {
                    lastGatewayException = new GatewayException(ErrorCode.UPSTREAM_ERROR,
                            "Upstream call failed on channel " + channel.getChannelName() + ": "
                                    + (cause != null ? cause.getMessage() : compEx.getMessage()), cause);
                }
                recordFailureSafely(channel.getId());
                log.warn("Non-streaming attempt {}/{} failed [channelId={}, provider={}]: {}",
                        attempt + 1, maxAttempts, channel.getId(), channel.getProvider(),
                        lastGatewayException.getMessage());
            }
        }

        rollbackFrozenSafely(tenant.getTenantId(), requestId);
        if (lastGatewayException != null) {
            throw lastGatewayException;
        }
        throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                "All upstream attempts failed for model: " + request.getModel());
    }

    /**
     * 流式 SSE 聊天补全代理转发 (支持首包 TTFT 超时切换、429/5xx 容灾、客户端断连联动中断上游)。
     */
    public StreamConnectResult proxyChatStream(
            ChatCompletionRequest request,
            TenantAuthInfo tenant,
            String requestId,
            Map<String, String> passthroughHeaders) {
        AdmissionContext ctx = admitAndFreeze(request, tenant, requestId);
        List<LlmChannelKey> candidates = selectChannelsOrRollback(request.getModel(), tenant.getTenantId(), requestId);

        int maxAttempts = Math.min(candidates.size(), 1 + Math.max(0, properties.getMaxFailoverAttempts()));
        String payloadJson = serializeUpstreamRequestOrRollback(request, true, tenant.getTenantId(), requestId);

        StreamSession session = new StreamSession(
                ctx,
                request,
                candidates,
                maxAttempts,
                payloadJson,
                passthroughHeaders,
                new SseEmitter(Math.max(5_000L, properties.getStreamTimeoutMs()))
        );

        session.registerEmitterCallbacks();
        session.launchAttempt(0, null);

        long maxWaitMs = Math.max(2_000L,
                (properties.getTtftTimeoutMs() + properties.getConnectTimeoutMs()) * maxAttempts + 2_000L);
        try {
            return session.firstDecisionFuture.get(maxWaitMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof GatewayException ge) {
                throw ge;
            }
            throw new GatewayException(ErrorCode.UPSTREAM_ERROR,
                    "Streaming upstream failed before first chunk", cause);
        } catch (TimeoutException ex) {
            StreamConnectResult lateWinner = session.abortOnFirstDecisionTimeout();
            if (lateWinner != null) {
                return lateWinner;
            }
            throw new GatewayException(ErrorCode.UPSTREAM_ERROR,
                    "Timed out waiting for first streaming chunk", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            session.abortFromClient("gateway_thread_interrupted");
            throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                    "Interrupted while waiting for upstream stream", ex);
        }
    }

    private AdmissionContext admitAndFreeze(ChatCompletionRequest request, TenantAuthInfo tenant, String requestId) {
        if (tenant == null || tenant.getTenantId() == null || tenant.getTenantId().isBlank()) {
            throw new GatewayException(ErrorCode.INVALID_API_KEY, "Missing authenticated tenant context.");
        }
        long startNanos = System.nanoTime();
        int promptTokens = Math.max(1, tokenEstimator.estimatePromptTokens(request));
        int estimatedTokens = tokenEstimator.estimateTotalFrozenTokens(request);
        if (estimatedTokens < promptTokens) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "Estimated token count overflow.");
        }

        rateLimitService.acquireOrThrow(tenant, requestId, estimatedTokens);
        quotaFreezeLuaService.freezeQuotaOrThrow(tenant.getTenantId(), requestId, estimatedTokens);

        try {
            BillingMessage delayMsg = BillingMessage.builder()
                    .requestId(requestId)
                    .tenantId(tenant.getTenantId())
                    .modelName(request.getModel())
                    .estimatedTokens(estimatedTokens)
                    .promptTokens(promptTokens)
                    .requestTime(LocalDateTime.now())
                    .build();
            billingMessageProducer.sendFreezeDelayMessage(delayMsg);
        } catch (RuntimeException ex) {
            quotaFreezeLuaService.rollbackFrozen(tenant.getTenantId(), requestId);
            throw ex;
        }

        return new AdmissionContext(tenant.getTenantId(), requestId, promptTokens, estimatedTokens, startNanos);
    }

    private List<LlmChannelKey> selectChannelsOrRollback(String modelName, String tenantId, String requestId) {
        try {
            return llmChannelRouter.selectAvailableChannels(modelName);
        } catch (RuntimeException ex) {
            quotaFreezeLuaService.rollbackFrozen(tenantId, requestId);
            throw ex;
        }
    }

    private void settleNonStreamBilling(
            AdmissionContext ctx,
            ChatCompletionRequest request,
            LlmChannelKey channel,
            ChatCompletionResponse response) {
        int actualPrompt = ctx.promptTokens;
        int actualCompletion;
        int actualTotal;
        boolean estimated = false;
        String finishReason = "stop";

        if (response.getChoices() != null && !response.getChoices().isEmpty()
                && response.getChoices().get(0).getFinishReason() != null) {
            finishReason = response.getChoices().get(0).getFinishReason();
        }

        ChatCompletionResponse.Usage usage = response.getUsage();
        if (usage != null && usage.getTotalTokens() != null && usage.getTotalTokens() > 0) {
            actualPrompt = usage.getPromptTokens() != null ? usage.getPromptTokens() : ctx.promptTokens;
            actualCompletion = usage.getCompletionTokens() != null
                    ? usage.getCompletionTokens()
                    : Math.max(1, usage.getTotalTokens() - actualPrompt);
            actualTotal = usage.getTotalTokens();
        } else {
            estimated = true;
            String content = (response.getChoices() != null && !response.getChoices().isEmpty()
                    && response.getChoices().get(0).getMessage() != null)
                    ? response.getChoices().get(0).getMessage().getContent()
                    : "";
            actualCompletion = tokenEstimator.estimateCompletionTokens(content);
            actualTotal = actualPrompt + actualCompletion;
        }

        int costMs = (int) Math.max(1L, (System.nanoTime() - ctx.startNanos) / 1_000_000L);
        quotaFreezeLuaService.settleQuota(ctx.tenantId, ctx.requestId, actualTotal);

        BillingMessage billingMsg = BillingMessage.builder()
                .requestId(ctx.requestId)
                .tenantId(ctx.tenantId)
                .modelName(request.getModel())
                .provider(channel.getProvider())
                .channelId(channel.getId())
                .estimatedTokens(ctx.estimatedTokens)
                .promptTokens(actualPrompt)
                .completionTokens(actualCompletion)
                .totalTokens(actualTotal)
                .costMs(costMs)
                .finishReason(finishReason)
                .estimated(estimated)
                .requestTime(LocalDateTime.now())
                .build();
        billingMessageProducer.sendBillingMessage(billingMsg);
    }

    private String serializeUpstreamRequestOrRollback(
            ChatCompletionRequest request,
            boolean forceStream,
            String tenantId,
            String requestId) {
        try {
            return serializeUpstreamRequest(request, forceStream);
        } catch (RuntimeException ex) {
            rollbackFrozenSafely(tenantId, requestId);
            throw ex;
        }
    }

    private String serializeUpstreamRequest(ChatCompletionRequest request, boolean forceStream) {
        try {
            ChatCompletionRequest.StreamOptions streamOptions = request.getStreamOptions();
            if (forceStream && streamOptions == null) {
                streamOptions = ChatCompletionRequest.StreamOptions.builder()
                        .includeUsage(Boolean.TRUE)
                        .build();
            } else if (!forceStream) {
                streamOptions = null;
            }
            ChatCompletionRequest outbound = ChatCompletionRequest.builder()
                    .model(request.getModel())
                    .messages(request.getMessages())
                    .stream(forceStream)
                    .maxTokens(request.getMaxTokens())
                    .temperature(request.getTemperature())
                    .topP(request.getTopP())
                    .user(request.getUser())
                    .streamOptions(streamOptions)
                    .build();
            return objectMapper.writeValueAsString(outbound);
        } catch (Exception ex) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "Failed to serialize request payload.", ex);
        }
    }

    private Request buildUpstreamRequest(
            LlmChannelKey channel,
            String payloadJson,
            Map<String, String> passthroughHeaders) {
        String targetUrl = buildUpstreamUrl(channel.getBaseUrl());
        Request.Builder builder = new Request.Builder()
                .url(targetUrl)
                .post(RequestBody.create(payloadJson, JSON_MEDIA_TYPE))
                .header("Authorization", "Bearer " + channel.getSecretKey())
                .header("Content-Type", "application/json");

        if (passthroughHeaders != null) {
            passthroughHeaders.forEach((k, v) -> {
                if (k != null && v != null && !v.isBlank() && !isProtectedHeader(k)) {
                    builder.header(k, v);
                }
            });
        }
        return builder.build();
    }

    private static boolean isProtectedHeader(String headerName) {
        return "Authorization".equalsIgnoreCase(headerName)
                || "Host".equalsIgnoreCase(headerName)
                || "Content-Length".equalsIgnoreCase(headerName)
                || "Content-Type".equalsIgnoreCase(headerName)
                || "Transfer-Encoding".equalsIgnoreCase(headerName);
    }

    private static boolean isRetryableUpstreamStatus(int statusCode) {
        return statusCode == 429 || statusCode >= 500;
    }

    private GatewayException mapNonRetryableUpstreamException(
            int statusCode,
            LlmChannelKey channel,
            String errBody) {
        ErrorCode errorCode;
        if (statusCode == 400 || statusCode == 422) {
            errorCode = ErrorCode.INVALID_REQUEST;
        } else if (statusCode == 404) {
            errorCode = ErrorCode.MODEL_NOT_FOUND;
        } else {
            errorCode = ErrorCode.UPSTREAM_ERROR;
        }
        String channelName = channel != null ? channel.getChannelName() : "unknown";
        String detail = (errBody != null && !errBody.isBlank()) ? ": " + errBody : "";
        return new GatewayException(errorCode,
                String.format("Upstream HTTP %d from channel %s%s", statusCode, channelName, detail));
    }

    private String safeReadErrorBody(ResponseBody body) {
        if (body == null) {
            return "";
        }
        try {
            String raw = body.string();
            return raw.length() > 512 ? raw.substring(0, 512) + "..." : raw;
        } catch (IOException ex) {
            return "";
        }
    }

    private void recordSuccessSafely(Long channelId) {
        try {
            circuitBreaker.recordSuccess(channelId);
        } catch (Exception ex) {
            log.warn("Failed to record circuit breaker success for channelId={}: {}", channelId, ex.getMessage());
        }
    }

    private void recordFailureSafely(Long channelId) {
        try {
            circuitBreaker.recordFailure(channelId);
        } catch (Exception ex) {
            log.warn("Failed to record circuit breaker failure for channelId={}: {}", channelId, ex.getMessage());
        }
    }

    private void rollbackFrozenSafely(String tenantId, String requestId) {
        try {
            quotaFreezeLuaService.rollbackFrozen(tenantId, requestId);
        } catch (Exception ex) {
            log.error("Failed to rollback frozen quota in Redis (freeze timeout DLX will compensate): tenantId={}, requestId={}, error={}",
                    tenantId, requestId, ex.getMessage(), ex);
        }
    }

    private String buildUpstreamUrl(String baseUrl) {
        String trimmed = baseUrl != null ? baseUrl.trim() : "";
        int queryIdx = trimmed.indexOf('?');
        String pathPart = queryIdx >= 0 ? trimmed.substring(0, queryIdx) : trimmed;
        String queryPart = queryIdx >= 0 ? trimmed.substring(queryIdx) : "";

        while (pathPart.endsWith("/")) {
            pathPart = pathPart.substring(0, pathPart.length() - 1);
        }
        if (!pathPart.endsWith("/chat/completions")) {
            if (pathPart.endsWith("/v1")) {
                pathPart = pathPart + "/chat/completions";
            } else {
                pathPart = pathPart + "/v1/chat/completions";
            }
        }
        return pathPart + queryPart;
    }

    private ParsedChunkInfo parseChunkInfo(String jsonPayload) {
        try {
            JsonNode root = objectMapper.readTree(jsonPayload);
            String deltaContent = null;
            String finishReason = null;
            JsonNode choices = root.get("choices");
            if (choices != null && choices.isArray() && !choices.isEmpty()) {
                JsonNode first = choices.get(0);
                JsonNode delta = first.get("delta");
                if (delta != null) {
                    StringBuilder piece = new StringBuilder();
                    if (delta.hasNonNull("reasoning_content")) {
                        piece.append(delta.get("reasoning_content").asText());
                    }
                    if (delta.hasNonNull("content")) {
                        piece.append(delta.get("content").asText());
                    }
                    JsonNode toolCalls = delta.get("tool_calls");
                    if (toolCalls != null && toolCalls.isArray()) {
                        for (JsonNode toolCall : toolCalls) {
                            if (toolCall == null) {
                                continue;
                            }
                            JsonNode fn = toolCall.get("function");
                            if (fn != null) {
                                if (fn.hasNonNull("name")) {
                                    piece.append(fn.get("name").asText());
                                }
                                if (fn.hasNonNull("arguments")) {
                                    piece.append(fn.get("arguments").asText());
                                }
                            }
                        }
                    }
                    if (!piece.isEmpty()) {
                        deltaContent = piece.toString();
                    }
                }
                if (first.hasNonNull("finish_reason")) {
                    finishReason = first.get("finish_reason").asText();
                }
            }
            ChatCompletionResponse.Usage usage = null;
            JsonNode usageNode = root.get("usage");
            if (usageNode != null && !usageNode.isNull()) {
                usage = ChatCompletionResponse.Usage.builder()
                        .promptTokens(usageNode.hasNonNull("prompt_tokens") ? usageNode.get("prompt_tokens").asInt() : null)
                        .completionTokens(usageNode.hasNonNull("completion_tokens") ? usageNode.get("completion_tokens").asInt() : null)
                        .totalTokens(usageNode.hasNonNull("total_tokens") ? usageNode.get("total_tokens").asInt() : null)
                        .build();
            }
            return new ParsedChunkInfo(deltaContent, finishReason, usage);
        } catch (Exception ignored) {
            return new ParsedChunkInfo(null, null, null);
        }
    }

    private class StreamSession {
        private final AdmissionContext ctx;
        private final ChatCompletionRequest request;
        private final List<LlmChannelKey> candidates;
        private final int maxAttempts;
        private final String payloadJson;
        private final Map<String, String> passthroughHeaders;
        private final SseEmitter emitter;

        private final AtomicBoolean firstChunkWritten = new AtomicBoolean(false);
        private final AtomicBoolean finalized = new AtomicBoolean(false);
        private final AtomicBoolean clientAborted = new AtomicBoolean(false);
        private final Object decisionLock = new Object();
        private final AtomicReference<Call> currentCallRef = new AtomicReference<>();
        private final AtomicReference<ScheduledFuture<?>> currentTimerRef = new AtomicReference<>();
        private final AtomicReference<LlmChannelKey> activeChannelRef = new AtomicReference<>();
        private final AtomicReference<ChatCompletionResponse.Usage> upstreamUsageRef = new AtomicReference<>();
        private final AtomicReference<String> finishReasonRef = new AtomicReference<>("stop");
        private final StringBuilder pushedContent = new StringBuilder();
        private final CompletableFuture<StreamConnectResult> firstDecisionFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> preFirstChunkRollbackFuture = new CompletableFuture<>();

        private StreamSession(
                AdmissionContext ctx,
                ChatCompletionRequest request,
                List<LlmChannelKey> candidates,
                int maxAttempts,
                String payloadJson,
                Map<String, String> passthroughHeaders,
                SseEmitter emitter) {
            this.ctx = ctx;
            this.request = request;
            this.candidates = candidates;
            this.maxAttempts = maxAttempts;
            this.payloadJson = payloadJson;
            this.passthroughHeaders = passthroughHeaders;
            this.emitter = emitter;
        }

        private void registerEmitterCallbacks() {
            emitter.onCompletion(() -> {
                if (!finalized.get()) {
                    abortFromClient("sse_on_completion");
                }
            });
            emitter.onTimeout(() -> {
                abortFromClient("sse_on_timeout");
                emitter.complete();
            });
            emitter.onError(ex -> abortFromClient("sse_on_error: " + (ex != null ? ex.getMessage() : "unknown")));
        }

        private void launchAttempt(int attemptIndex, GatewayException lastError) {
            if (firstChunkWritten.get() || clientAborted.get() || finalized.get()) {
                return;
            }
            if (attemptIndex >= maxAttempts) {
                GatewayException finalErr = lastError != null ? lastError
                        : new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                        "All upstream channels failed for model: " + request.getModel());
                failBeforeFirstChunk(finalErr);
                return;
            }

            LlmChannelKey channel = candidates.get(attemptIndex);
            AtomicBoolean attemptFailed = new AtomicBoolean(false);

            try {
                providerPoolManager.execute(channel.getProvider(), () -> {
                    ScheduledFuture<?> ttftTimer = null;
                    try {
                        if (isStoppedOrAttemptFailed(attemptFailed)) {
                            return;
                        }

                        Request okRequest = buildUpstreamRequest(channel, payloadJson, passthroughHeaders);
                        Call call = okHttpClient.newCall(okRequest);
                        currentCallRef.set(call);
                        if (isStoppedOrAttemptFailed(attemptFailed)) {
                            call.cancel();
                            return;
                        }

                        ttftTimer = ttftScheduler.schedule(() -> {
                            if (tryClaimAttemptFailover(attemptFailed)) {
                                log.warn("TTFT timeout ({}ms) on stream channelId={} provider={}, canceling Call and failing over",
                                        properties.getTtftTimeoutMs(), channel.getId(), channel.getProvider());
                                call.cancel();
                                CompletableFuture.runAsync(() -> {
                                    try {
                                        recordFailureSafely(channel.getId());
                                        GatewayException timeoutErr = new GatewayException(ErrorCode.UPSTREAM_ERROR,
                                                "Upstream TTFT timeout (" + properties.getTtftTimeoutMs() + "ms) on channel "
                                                        + channel.getChannelName());
                                        launchAttempt(attemptIndex + 1, timeoutErr);
                                    } catch (Throwable asyncErr) {
                                        log.error("Unexpected error in TTFT failover async task [channelId={}]: {}",
                                                channel.getId(), asyncErr.getMessage(), asyncErr);
                                        failBeforeFirstChunk(new GatewayException(ErrorCode.UPSTREAM_ERROR,
                                                "Unexpected TTFT failover failure", asyncErr));
                                    }
                                });
                            }
                        }, Math.max(100L, properties.getTtftTimeoutMs()), TimeUnit.MILLISECONDS);
                        replaceCurrentTimer(ttftTimer);
                        if (isStoppedOrAttemptFailed(attemptFailed)) {
                            cancelAttemptTimer(ttftTimer);
                            call.cancel();
                            return;
                        }

                        try (Response response = call.execute()) {
                            if (attemptFailed.get() || clientAborted.get() || finalized.get()) {
                                cancelAttemptTimer(ttftTimer);
                                return;
                            }
                            if (!response.isSuccessful()) {
                                cancelAttemptTimer(ttftTimer);
                                int statusCode = response.code();
                                String errBody = safeReadErrorBody(response.body());
                                if (!isRetryableUpstreamStatus(statusCode)) {
                                    boolean claimedReject = false;
                                    synchronized (decisionLock) {
                                        if (!firstChunkWritten.get() && !clientAborted.get()
                                                && attemptFailed.compareAndSet(false, true)
                                                && finalized.compareAndSet(false, true)) {
                                            cancelCurrentTimer();
                                            claimedReject = true;
                                        }
                                    }
                                    if (claimedReject) {
                                        try {
                                            rollbackFrozenSafely(ctx.tenantId, ctx.requestId);
                                        } finally {
                                            preFirstChunkRollbackFuture.complete(null);
                                        }
                                        GatewayException clientErr = mapNonRetryableUpstreamException(
                                                statusCode, channel, errBody);
                                        log.warn("Non-retryable upstream HTTP {} on streaming [channelId={}, provider={}], aborting without failover",
                                                statusCode, channel.getId(), channel.getProvider());
                                        synchronized (decisionLock) {
                                            if (!firstDecisionFuture.isDone()) {
                                                firstDecisionFuture.completeExceptionally(clientErr);
                                            }
                                        }
                                    }
                                    return;
                                }
                                if (tryClaimAttemptFailover(attemptFailed)) {
                                    recordFailureSafely(channel.getId());
                                    GatewayException statusErr = new GatewayException(ErrorCode.UPSTREAM_ERROR,
                                            "Upstream HTTP " + statusCode + " on channel " + channel.getChannelName());
                                    log.warn("Streaming attempt {}/{} HTTP {} [channelId={}, provider={}], failing over",
                                            attemptIndex + 1, maxAttempts, statusCode,
                                            channel.getId(), channel.getProvider());
                                    launchAttempt(attemptIndex + 1, statusErr);
                                }
                                return;
                            }

                            ResponseBody body = response.body();
                            if (body == null) {
                                cancelAttemptTimer(ttftTimer);
                                if (tryClaimAttemptFailover(attemptFailed)) {
                                    recordFailureSafely(channel.getId());
                                    launchAttempt(attemptIndex + 1, new GatewayException(
                                            ErrorCode.UPSTREAM_ERROR, "Empty response body from " + channel.getChannelName()));
                                }
                                return;
                            }

                            readUpstreamSse(body.source(), call, channel, attemptIndex, attemptFailed, ttftTimer);
                        }
                    } catch (Throwable ex) {
                        cancelAttemptTimer(ttftTimer);
                        if (clientAborted.get() || finalized.get() || attemptFailed.get()) {
                            return;
                        }
                        if (!firstChunkWritten.get()) {
                            if (tryClaimAttemptFailover(attemptFailed)) {
                                recordFailureSafely(channel.getId());
                                GatewayException ioErr = new GatewayException(ErrorCode.UPSTREAM_ERROR,
                                        "Upstream stream error on channel " + channel.getChannelName()
                                                + ": " + ex.getMessage(), ex);
                                log.warn("Streaming attempt {}/{} exception before first chunk [channelId={}]: {}",
                                        attemptIndex + 1, maxAttempts, channel.getId(), ex.getMessage());
                                launchAttempt(attemptIndex + 1, ioErr);
                            }
                        } else {
                            recordFailureSafely(channel.getId());
                            finalizeBillingOnce(true, "error");
                            emitter.completeWithError(ex);
                        }
                    }
                });
            } catch (GatewayException poolRejectedEx) {
                log.warn("Pool rejected streaming attempt {}/{} for provider={}: {}",
                        attemptIndex + 1, maxAttempts, channel.getProvider(), poolRejectedEx.getMessage());
                launchAttempt(attemptIndex + 1, poolRejectedEx);
            }
        }

        private void readUpstreamSse(
                BufferedSource source,
                Call call,
                LlmChannelKey channel,
                int attemptIndex,
                AtomicBoolean attemptFailed,
                ScheduledFuture<?> ttftTimer) throws IOException {
            while (!clientAborted.get() && !attemptFailed.get() && !finalized.get()) {
                String line = source.readUtf8Line();
                if (line == null) {
                    break;
                }
                if (line.isEmpty() || line.startsWith(":")) {
                    continue;
                }
                if (!line.startsWith("data:")) {
                    continue;
                }
                String payload = line.substring(5).trim();
                if (payload.isEmpty()) {
                    continue;
                }
                if ("[DONE]".equals(payload)) {
                    if (firstChunkWritten.get()) {
                        try {
                            emitter.send(SseEmitter.event().data("[DONE]"));
                        } catch (Exception sendEx) {
                            abortFromClient("client_disconnect_on_done");
                            return;
                        }
                    }
                    break;
                }

                ParsedChunkInfo chunkInfo = parseChunkInfo(payload);
                if (chunkInfo.usage != null) {
                    upstreamUsageRef.set(chunkInfo.usage);
                }
                if (chunkInfo.finishReason != null) {
                    finishReasonRef.set(chunkInfo.finishReason);
                }

                if (!firstChunkWritten.get()) {
                    FirstChunkClaimOutcome outcome = tryClaimFirstChunkAndEmit(
                            attemptFailed, channel, attemptIndex, payload, chunkInfo.deltaContent);
                    if (outcome == FirstChunkClaimOutcome.CLAIMED) {
                        cancelAttemptTimer(ttftTimer);
                        recordSuccessSafely(channel.getId());
                        continue;
                    }
                    call.cancel();
                    if (outcome == FirstChunkClaimOutcome.EMITTER_FAILED_ROLLBACK) {
                        try {
                            rollbackFrozenSafely(ctx.tenantId, ctx.requestId);
                        } finally {
                            preFirstChunkRollbackFuture.complete(null);
                        }
                        synchronized (decisionLock) {
                            if (!firstDecisionFuture.isDone()) {
                                firstDecisionFuture.completeExceptionally(new GatewayException(
                                        ErrorCode.UPSTREAM_ERROR, "Client SSE emitter closed before first chunk"));
                            }
                        }
                    }
                    return;
                }

                try {
                    emitter.send(SseEmitter.event().data(payload));
                    appendPushedContent(chunkInfo.deltaContent);
                } catch (Exception sendEx) {
                    abortFromClient("client_disconnect_mid_stream: " + sendEx.getMessage());
                    return;
                }
            }

            cancelAttemptTimer(ttftTimer);
            if (clientAborted.get() || attemptFailed.get()) {
                return;
            }
            if (!firstChunkWritten.get()) {
                if (tryClaimAttemptFailover(attemptFailed)) {
                    recordFailureSafely(channel.getId());
                    launchAttempt(attemptIndex + 1, new GatewayException(
                            ErrorCode.UPSTREAM_ERROR, "Upstream closed SSE without emitting any chunk"));
                }
                return;
            }

            ChatCompletionResponse.Usage usage = upstreamUsageRef.get();
            boolean hasExactUsage = usage != null && usage.getTotalTokens() != null && usage.getTotalTokens() > 0;
            finalizeBillingOnce(!hasExactUsage, finishReasonRef.get());
            emitter.complete();
        }

        private void appendPushedContent(String deltaContent) {
            if (deltaContent != null && !deltaContent.isEmpty()) {
                synchronized (pushedContent) {
                    pushedContent.append(deltaContent);
                }
            }
        }

        private boolean isStoppedOrAttemptFailed(AtomicBoolean attemptFailed) {
            return firstChunkWritten.get() || clientAborted.get() || finalized.get() || attemptFailed.get();
        }

        private void failBeforeFirstChunk(GatewayException error) {
            boolean shouldRollback = false;
            synchronized (decisionLock) {
                if (!firstChunkWritten.get() && finalized.compareAndSet(false, true)) {
                    cancelCurrentTimer();
                    shouldRollback = true;
                }
            }
            if (shouldRollback) {
                try {
                    rollbackFrozenSafely(ctx.tenantId, ctx.requestId);
                } finally {
                    preFirstChunkRollbackFuture.complete(null);
                }
                synchronized (decisionLock) {
                    if (!firstDecisionFuture.isDone()) {
                        firstDecisionFuture.completeExceptionally(error);
                    }
                }
            }
        }

        private StreamConnectResult abortOnFirstDecisionTimeout() {
            Call call;
            boolean claimedFirst;
            boolean shouldRollback = false;
            synchronized (decisionLock) {
                if (firstDecisionFuture.isDone() && !firstDecisionFuture.isCompletedExceptionally()) {
                    return firstDecisionFuture.getNow(null);
                }
                clientAborted.set(true);
                call = currentCallRef.get();
                cancelCurrentTimer();
                claimedFirst = firstChunkWritten.get();
                if (!claimedFirst && finalized.compareAndSet(false, true)) {
                    shouldRollback = true;
                }
                if (!firstDecisionFuture.isDone()) {
                    firstDecisionFuture.completeExceptionally(new GatewayException(
                            ErrorCode.UPSTREAM_ERROR, "Timed out waiting for first streaming chunk"));
                }
            }
            if (call != null) {
                call.cancel();
            }
            if (shouldRollback) {
                try {
                    rollbackFrozenSafely(ctx.tenantId, ctx.requestId);
                } finally {
                    preFirstChunkRollbackFuture.complete(null);
                }
            } else if (!claimedFirst) {
                try {
                    preFirstChunkRollbackFuture.get(2, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // Best-effort wait so quota rollback completes before returning 502
                }
            }
            return null;
        }

        private void abortFromClient(String reason) {
            Call call;
            boolean claimedFirst;
            boolean shouldRollback = false;
            synchronized (decisionLock) {
                clientAborted.set(true);
                call = currentCallRef.get();
                cancelCurrentTimer();
                claimedFirst = firstChunkWritten.get();
                if (!claimedFirst && finalized.compareAndSet(false, true)) {
                    shouldRollback = true;
                }
            }
            if (call != null) {
                call.cancel();
            }
            if (claimedFirst) {
                finalizeBillingOnce(true, "client_abort");
            } else if (shouldRollback) {
                try {
                    rollbackFrozenSafely(ctx.tenantId, ctx.requestId);
                } finally {
                    preFirstChunkRollbackFuture.complete(null);
                }
                synchronized (decisionLock) {
                    if (!firstDecisionFuture.isDone()) {
                        firstDecisionFuture.completeExceptionally(new GatewayException(
                                ErrorCode.UPSTREAM_ERROR, "Client disconnected before first token: " + reason));
                    }
                }
            }
        }

        private void finalizeBillingOnce(boolean forceEstimated, String finishReason) {
            if (!finalized.compareAndSet(false, true)) {
                return;
            }
            cancelCurrentTimer();
            LlmChannelKey channel = activeChannelRef.get();
            ChatCompletionResponse.Usage usage = upstreamUsageRef.get();

            int actualPrompt = ctx.promptTokens;
            int actualCompletion;
            int actualTotal;
            boolean isEstimated = forceEstimated;

            if (!forceEstimated && usage != null && usage.getTotalTokens() != null && usage.getTotalTokens() > 0) {
                actualPrompt = usage.getPromptTokens() != null ? usage.getPromptTokens() : ctx.promptTokens;
                actualCompletion = usage.getCompletionTokens() != null
                        ? usage.getCompletionTokens()
                        : Math.max(1, usage.getTotalTokens() - actualPrompt);
                actualTotal = usage.getTotalTokens();
            } else {
                isEstimated = true;
                String snapshot;
                synchronized (pushedContent) {
                    snapshot = pushedContent.toString();
                }
                actualCompletion = tokenEstimator.estimateCompletionTokens(snapshot);
                actualTotal = (int) Math.min(Integer.MAX_VALUE, (long) actualPrompt + actualCompletion);
            }

            int costMs = (int) Math.max(1L, (System.nanoTime() - ctx.startNanos) / 1_000_000L);
            quotaFreezeLuaService.settleQuota(ctx.tenantId, ctx.requestId, actualTotal);

            BillingMessage billingMsg = BillingMessage.builder()
                    .requestId(ctx.requestId)
                    .tenantId(ctx.tenantId)
                    .modelName(request.getModel())
                    .provider(channel != null ? channel.getProvider() : "UNKNOWN")
                    .channelId(channel != null ? channel.getId() : 0L)
                    .estimatedTokens(ctx.estimatedTokens)
                    .promptTokens(actualPrompt)
                    .completionTokens(actualCompletion)
                    .totalTokens(actualTotal)
                    .costMs(costMs)
                    .finishReason(finishReason)
                    .estimated(isEstimated)
                    .requestTime(LocalDateTime.now())
                    .build();
            billingMessageProducer.sendBillingMessage(billingMsg);
        }

        private boolean tryClaimAttemptFailover(AtomicBoolean attemptFailed) {
            synchronized (decisionLock) {
                return !firstChunkWritten.get()
                        && !clientAborted.get()
                        && !finalized.get()
                        && attemptFailed.compareAndSet(false, true);
            }
        }

        private FirstChunkClaimOutcome tryClaimFirstChunkAndEmit(
                AtomicBoolean attemptFailed,
                LlmChannelKey channel,
                int attemptIndex,
                String payload,
                String deltaContent) {
            synchronized (decisionLock) {
                if (firstChunkWritten.get() || attemptFailed.get()
                        || clientAborted.get() || finalized.get()
                        || firstDecisionFuture.isDone()) {
                    return FirstChunkClaimOutcome.REJECTED;
                }
                try {
                    emitter.send(SseEmitter.event().data(payload));
                } catch (Exception sendEx) {
                    clientAborted.set(true);
                    boolean wonFinalized = finalized.compareAndSet(false, true);
                    cancelCurrentTimer();
                    return wonFinalized
                            ? FirstChunkClaimOutcome.EMITTER_FAILED_ROLLBACK
                            : FirstChunkClaimOutcome.REJECTED;
                }
                firstChunkWritten.set(true);
                activeChannelRef.set(channel);
                appendPushedContent(deltaContent);
                boolean failover = attemptIndex > 0;
                firstDecisionFuture.complete(
                        new StreamConnectResult(emitter, channel.getProvider(), failover));
                return FirstChunkClaimOutcome.CLAIMED;
            }
        }

        private void replaceCurrentTimer(ScheduledFuture<?> next) {
            ScheduledFuture<?> prev = currentTimerRef.getAndSet(next);
            if (prev != null) {
                prev.cancel(false);
            }
        }

        private void cancelAttemptTimer(ScheduledFuture<?> attemptTimer) {
            if (attemptTimer != null) {
                attemptTimer.cancel(false);
                currentTimerRef.compareAndSet(attemptTimer, null);
            }
        }

        private void cancelCurrentTimer() {
            ScheduledFuture<?> prev = currentTimerRef.getAndSet(null);
            if (prev != null) {
                prev.cancel(false);
            }
        }
    }

    @Getter
    @AllArgsConstructor
    public static class NonStreamResult {
        private final ChatCompletionResponse response;
        private final String provider;
        private final boolean failover;
    }

    @Getter
    @AllArgsConstructor
    public static class StreamConnectResult {
        private final SseEmitter emitter;
        private final String provider;
        private final boolean failover;
    }

    @AllArgsConstructor
    private static class AdmissionContext {
        private final String tenantId;
        private final String requestId;
        private final int promptTokens;
        private final int estimatedTokens;
        private final long startNanos;
    }

    @AllArgsConstructor
    private static class ParsedChunkInfo {
        private final String deltaContent;
        private final String finishReason;
        private final ChatCompletionResponse.Usage usage;
    }

    private enum FirstChunkClaimOutcome {
        CLAIMED,
        REJECTED,
        EMITTER_FAILED_ROLLBACK
    }

    private static class UpstreamStatusException extends RuntimeException {
        private final int statusCode;
        private final String responseBody;

        private UpstreamStatusException(int statusCode, String responseBody, String message) {
            super(message);
            this.statusCode = statusCode;
            this.responseBody = responseBody;
        }
    }
}
