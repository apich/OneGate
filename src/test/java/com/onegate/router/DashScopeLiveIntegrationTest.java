package com.onegate.router;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.auth.TenantAuthCacheService;
import com.onegate.billing.BillingSettlementTxService;
import com.onegate.billing.QuotaFreezeLuaService;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.constant.MqConstants;
import com.onegate.common.constant.RedisKeys;
import com.onegate.controller.MockUpstreamController;
import com.onegate.domain.dto.BillingMessage;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.entity.BillingDeadLetterRecord;
import com.onegate.domain.entity.LlmChannelKey;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import com.onegate.domain.mapper.BillingDeadLetterMapper;
import com.onegate.domain.mapper.LlmChannelKeyMapper;
import com.onegate.domain.mapper.TenantAccountMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

/**
 * 阿里云百炼 (DashScope OpenAI 兼容模式) 真实大模型全链路端到端集成测试。
 * <p>
 * 密钥仅通过 {@code -Ddashscope.api.key=...} 或环境变量 {@code DASHSCOPE_API_KEY} 运行时注入，
 * 仓库内零硬编码；未配置密钥时由 JUnit 5 {@link EnabledIf} 自动跳过，不影响离线全量构建。
 */
@Tag("live-llm")
@EnabledIf("isDashScopeConfigured")
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration",
                "onegate.mock-enabled=true",
                "onegate.non-stream-timeout-ms=20000",
                "onegate.ttft-timeout-ms=20000",
                "onegate.failure-threshold=3",
                "onegate.cooldown-seconds=30",
                "onegate.max-failover-attempts=2"
        }
)
class DashScopeLiveIntegrationTest {

    private static final String DASHSCOPE_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1";
    private static final String PROVIDER_QWEN = "QWEN";
    private static final String MODEL_FLASH = "qwen3.8-flash";
    private static final String MODEL_MAX = "qwen3.8-max";
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    @LocalServerPort
    private int port;

    @MockitoBean
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private BillingSettlementTxService billingSettlementTxService;

    @Autowired
    private TenantAccountMapper tenantAccountMapper;

    @Autowired
    private LlmChannelKeyMapper channelKeyMapper;

    @Autowired
    private ApiUsageLogMapper apiUsageLogMapper;

    @Autowired
    private BillingDeadLetterMapper billingDeadLetterMapper;

    @Autowired
    private TenantAuthCacheService tenantAuthCacheService;

    @Autowired
    private QuotaFreezeLuaService quotaFreezeLuaService;

    @Autowired
    private ChannelCircuitBreaker circuitBreaker;

    @Autowired
    private LlmChannelRouter llmChannelRouter;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .build();

    private final List<String> createdTenantIds = new ArrayList<>();
    private final List<String> createdApiKeys = new ArrayList<>();
    private final List<Long> createdChannelIds = new ArrayList<>();
    private final List<String> createdRequestIds = new ArrayList<>();

    static boolean isDashScopeConfigured() {
        return !resolveDashScopeApiKey().isBlank();
    }

    private static String resolveDashScopeApiKey() {
        String sysProp = System.getProperty("dashscope.api.key");
        if (sysProp != null && !sysProp.isBlank()) {
            return sysProp.trim();
        }
        String envVal = System.getenv("DASHSCOPE_API_KEY");
        return envVal != null ? envVal.trim() : "";
    }

    @AfterEach
    void cleanup() {
        if (!createdRequestIds.isEmpty()) {
            billingDeadLetterMapper.delete(new LambdaQueryWrapper<BillingDeadLetterRecord>()
                    .in(BillingDeadLetterRecord::getRequestId, createdRequestIds));
        }
        for (Long channelId : createdChannelIds) {
            stringRedisTemplate.delete(RedisKeys.channelFailKey(channelId));
            stringRedisTemplate.delete(RedisKeys.channelCooldownKey(channelId));
            channelKeyMapper.deleteById(channelId);
        }
        for (String apiKey : createdApiKeys) {
            tenantAuthCacheService.evictLocalCache(apiKey);
            stringRedisTemplate.delete(RedisKeys.authKey(apiKey));
            stringRedisTemplate.delete("onegate:lock:auth:" + apiKey);
        }
        for (String tenantId : createdTenantIds) {
            apiUsageLogMapper.delete(new LambdaQueryWrapper<ApiUsageLog>()
                    .eq(ApiUsageLog::getTenantId, tenantId));
            billingDeadLetterMapper.delete(new LambdaQueryWrapper<BillingDeadLetterRecord>()
                    .eq(BillingDeadLetterRecord::getTenantId, tenantId));
            for (String reqId : createdRequestIds) {
                stringRedisTemplate.delete(RedisKeys.quotaFreezeKey(tenantId, reqId));
                stringRedisTemplate.delete(RedisKeys.quotaSeenKey(tenantId, reqId));
            }
            stringRedisTemplate.delete(RedisKeys.quotaAvailKey(tenantId));
            stringRedisTemplate.delete(RedisKeys.rateLimitRpmKey(tenantId));
            stringRedisTemplate.delete(RedisKeys.rateLimitTpmKey(tenantId));
            stringRedisTemplate.delete("onegate:lock:quota:init:" + tenantId);
            TenantAccount account = tenantAccountMapper.selectByTenantId(tenantId);
            if (account != null) {
                tenantAccountMapper.deleteById(account.getId());
            }
        }
        createdChannelIds.clear();
        createdApiKeys.clear();
        createdTenantIds.clear();
        createdRequestIds.clear();
        llmChannelRouter.evictAll();
    }

    @Test
    void shouldInvokeQwen38FlashNonStreamAndReconcileBilling() throws Exception {
        String suffix = shortId();
        String tenantId = "t_live_f_non_" + suffix;
        String apiKey = "sk-live-f-non-" + suffix;
        long initialBalance = 100_000L;
        insertTenant(tenantId, apiKey, initialBalance);
        LlmChannelKey channel = insertDashScopeChannel(MODEL_FLASH, "ds-live-flash-non-" + suffix, 1);

        String reqId = "req-live-flash-non-" + suffix;
        createdRequestIds.add(reqId);

        long startNanos = System.nanoTime();
        try (Response response = executeChat(
                apiKey, reqId, MODEL_FLASH,
                "请用一句简洁的中文解释什么是API网关。",
                false, 512)) {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.header(HeaderConstants.REQUEST_ID_HEADER)).isEqualTo(reqId);
            assertThat(response.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo(PROVIDER_QWEN);
            assertThat(response.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("false");

            JsonNode root = objectMapper.readTree(response.body().string());
            String content = root.at("/choices/0/message/content").asText("");
            assertThat(content).isNotBlank();

            int promptTokens = root.at("/usage/prompt_tokens").asInt();
            int completionTokens = root.at("/usage/completion_tokens").asInt();
            int totalTokens = root.at("/usage/total_tokens").asInt();
            assertThat(promptTokens).isGreaterThan(0);
            assertThat(completionTokens).isGreaterThan(0);
            assertThat(totalTokens).isEqualTo(promptTokens + completionTokens);

            long expectedRemaining = initialBalance - totalTokens;
            assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, reqId))).isFalse();
            Long redisAvail = quotaFreezeLuaService.getAvailableQuota(tenantId);
            assertThat(redisAvail).isEqualTo(expectedRemaining);

            BillingMessage msg = awaitAndCaptureBillingMessage(reqId);
            assertThat(msg.getProvider()).isEqualTo(PROVIDER_QWEN);
            assertThat(msg.getChannelId()).isEqualTo(channel.getId());
            assertThat(msg.getModelName()).isEqualTo(MODEL_FLASH);
            assertThat(msg.getEstimated()).isFalse();
            assertThat(msg.getTotalTokens()).isEqualTo(totalTokens);

            settleAndAssertMysqlReconciled(msg, tenantId, expectedRemaining, 0, totalTokens, channel.getId());

            System.out.printf("[LIVE-1][qwen3.8-flash non-stream] elapsedMs=%d, prompt=%d, completion=%d, total=%d, "
                            + "redisAvail=%d, mysqlBalance=%d, answer=%s%n",
                    elapsedMs, promptTokens, completionTokens, totalTokens, redisAvail, expectedRemaining,
                    abbreviate(content, 100));
        }
    }

    @Test
    void shouldInvokeQwen38FlashStreamAndReconcileBilling() throws Exception {
        String suffix = shortId();
        String tenantId = "t_live_f_sse_" + suffix;
        String apiKey = "sk-live-f-sse-" + suffix;
        long initialBalance = 100_000L;
        insertTenant(tenantId, apiKey, initialBalance);
        LlmChannelKey channel = insertDashScopeChannel(MODEL_FLASH, "ds-live-flash-sse-" + suffix, 1);

        String reqId = "req-live-flash-sse-" + suffix;
        createdRequestIds.add(reqId);

        long startNanos = System.nanoTime();
        try (Response response = executeChat(
                apiKey, reqId, MODEL_FLASH,
                "请用简短的三点列出API网关在微服务中的核心作用。",
                true, 512)) {
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.header("Content-Type")).contains("text/event-stream");
            assertThat(response.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo(PROVIDER_QWEN);
            assertThat(response.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("false");

            StreamReadResult streamResult = readFullSseStream(response.body().source());
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

            assertThat(streamResult.doneReceived).isTrue();
            assertThat(streamResult.dataChunkCount).isGreaterThanOrEqualTo(2);
            assertThat(streamResult.content.toString()).isNotBlank();
            assertThat(streamResult.totalTokens).isGreaterThan(0);

            BillingMessage msg = awaitAndCaptureBillingMessage(reqId);
            long expectedRemaining = initialBalance - streamResult.totalTokens;
            Long redisAvail = quotaFreezeLuaService.getAvailableQuota(tenantId);
            assertThat(redisAvail).isEqualTo(expectedRemaining);
            assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, reqId))).isFalse();

            assertThat(msg.getEstimated()).isFalse();
            assertThat(msg.getProvider()).isEqualTo(PROVIDER_QWEN);
            assertThat(msg.getChannelId()).isEqualTo(channel.getId());
            assertThat(msg.getTotalTokens()).isEqualTo(streamResult.totalTokens);

            settleAndAssertMysqlReconciled(
                    msg, tenantId, expectedRemaining, 0, streamResult.totalTokens, channel.getId());

            System.out.printf("[LIVE-2][qwen3.8-flash stream] elapsedMs=%d, chunks=%d, prompt=%d, completion=%d, "
                            + "total=%d, redisAvail=%d, mysqlBalance=%d, answer=%s%n",
                    elapsedMs, streamResult.dataChunkCount, streamResult.promptTokens,
                    streamResult.completionTokens, streamResult.totalTokens, redisAvail, expectedRemaining,
                    abbreviate(streamResult.content.toString(), 100));
        }
    }

    @Test
    void shouldInvokeQwen38MaxNonStreamAndStreamAndReconcileBilling() throws Exception {
        String suffix = shortId();
        String tenantId = "t_live_max_" + suffix;
        String apiKey = "sk-live-max-" + suffix;
        long initialBalance = 200_000L;
        insertTenant(tenantId, apiKey, initialBalance);
        LlmChannelKey channel = insertDashScopeChannel(MODEL_MAX, "ds-live-max-" + suffix, 1);

        // 1. qwen3.8-max 非流式调用
        String nonStreamReqId = "req-live-max-non-" + suffix;
        createdRequestIds.add(nonStreamReqId);
        int nonStreamTokens;
        long nonStart = System.nanoTime();
        try (Response resp1 = executeChat(
                apiKey, nonStreamReqId, MODEL_MAX,
                "请用一句简洁中文说明为什么大模型网关需要TTL延迟释放冻结额度。",
                false, 512)) {
            long nonElapsedMs = (System.nanoTime() - nonStart) / 1_000_000L;
            assertThat(resp1.code()).isEqualTo(200);
            assertThat(resp1.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo(PROVIDER_QWEN);
            assertThat(resp1.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("false");

            JsonNode root = objectMapper.readTree(resp1.body().string());
            String content = root.at("/choices/0/message/content").asText("");
            assertThat(content).isNotBlank();
            nonStreamTokens = root.at("/usage/total_tokens").asInt();
            assertThat(nonStreamTokens).isGreaterThan(0);

            BillingMessage msg1 = awaitAndCaptureBillingMessage(nonStreamReqId);
            assertThat(msg1.getEstimated()).isFalse();
            assertThat(msg1.getModelName()).isEqualTo(MODEL_MAX);
            long balanceAfter1 = initialBalance - nonStreamTokens;
            assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isEqualTo(balanceAfter1);

            settleAndAssertMysqlReconciled(msg1, tenantId, balanceAfter1, 0, nonStreamTokens, channel.getId());

            System.out.printf("[LIVE-3A][qwen3.8-max non-stream] elapsedMs=%d, totalTokens=%d, balanceAfter=%d, answer=%s%n",
                    nonElapsedMs, nonStreamTokens, balanceAfter1, abbreviate(content, 100));
        }

        // 2. qwen3.8-max SSE 流式调用
        String streamReqId = "req-live-max-sse-" + suffix;
        createdRequestIds.add(streamReqId);
        long sseStart = System.nanoTime();
        try (Response resp2 = executeChat(
                apiKey, streamReqId, MODEL_MAX,
                "请用两句话说明覆盖索引在MySQL深分页治理中的作用。",
                true, 512)) {
            assertThat(resp2.code()).isEqualTo(200);
            assertThat(resp2.header("Content-Type")).contains("text/event-stream");
            assertThat(resp2.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo(PROVIDER_QWEN);

            StreamReadResult sseResult = readFullSseStream(resp2.body().source());
            long sseElapsedMs = (System.nanoTime() - sseStart) / 1_000_000L;
            assertThat(sseResult.doneReceived).isTrue();
            assertThat(sseResult.content.toString()).isNotBlank();
            assertThat(sseResult.totalTokens).isGreaterThan(0);

            BillingMessage msg2 = awaitAndCaptureBillingMessage(streamReqId);
            assertThat(msg2.getEstimated()).isFalse();
            assertThat(msg2.getModelName()).isEqualTo(MODEL_MAX);

            long finalExpectedBalance = initialBalance - nonStreamTokens - sseResult.totalTokens;
            Long redisAvail = quotaFreezeLuaService.getAvailableQuota(tenantId);
            assertThat(redisAvail).isEqualTo(finalExpectedBalance);

            settleAndAssertMysqlReconciled(
                    msg2, tenantId, finalExpectedBalance, 0, sseResult.totalTokens, channel.getId());

            System.out.printf("[LIVE-3B][qwen3.8-max stream] elapsedMs=%d, chunks=%d, totalTokens=%d, finalBalance=%d, answer=%s%n",
                    sseElapsedMs, sseResult.dataChunkCount, sseResult.totalTokens, finalExpectedBalance,
                    abbreviate(sseResult.content.toString(), 100));
        }
    }

    @Test
    void shouldFailoverFromBrokenPrimaryToLiveDashScopeQwen38Flash() throws Exception {
        String suffix = shortId();
        String tenantId = "t_live_fo_" + suffix;
        String apiKey = "sk-live-fo-" + suffix;
        long initialBalance = 100_000L;
        insertTenant(tenantId, apiKey, initialBalance);

        LlmChannelKey mockPrimary = insertChannel(
                "mock-primary-failover-" + suffix,
                "DEEPSEEK",
                MODEL_FLASH,
                "http://127.0.0.1:" + port + "/mock-llm/primary",
                "mock-primary-key",
                100,
                1
        );
        LlmChannelKey dashScopeBackup = insertDashScopeChannel(
                MODEL_FLASH, "ds-live-flash-backup-" + suffix, 2);

        String reqId = "req-live-failover-" + suffix;
        createdRequestIds.add(reqId);

        long startNanos = System.nanoTime();
        try (Response response = executeChat(
                apiKey, reqId, MODEL_FLASH,
                "请用一句中文说明什么是主备故障转移。",
                false, 512,
                MockUpstreamController.HEADER_MOCK_PRIMARY_MODE, "error-500")) {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("true");
            assertThat(response.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo(PROVIDER_QWEN);

            JsonNode root = objectMapper.readTree(response.body().string());
            String content = root.at("/choices/0/message/content").asText("");
            assertThat(content).isNotBlank();
            assertThat(content).doesNotContain("[primary]");

            int totalTokens = root.at("/usage/total_tokens").asInt();
            assertThat(totalTokens).isGreaterThan(0);

            assertThat(circuitBreaker.getFailureCount(mockPrimary.getId())).isEqualTo(1L);
            assertThat(circuitBreaker.getFailureCount(dashScopeBackup.getId())).isEqualTo(0L);

            List<BillingMessage> messagesForReq = awaitAndCaptureAllBillingMessages(reqId);
            assertThat(messagesForReq).hasSize(1);
            BillingMessage settledMsg = messagesForReq.get(0);
            assertThat(settledMsg.getChannelId()).isEqualTo(dashScopeBackup.getId());
            assertThat(settledMsg.getProvider()).isEqualTo(PROVIDER_QWEN);
            assertThat(settledMsg.getEstimated()).isFalse();
            assertThat(settledMsg.getTotalTokens()).isEqualTo(totalTokens);

            long expectedRemaining = initialBalance - totalTokens;
            assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isEqualTo(expectedRemaining);
            assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, reqId))).isFalse();

            settleAndAssertMysqlReconciled(
                    settledMsg, tenantId, expectedRemaining, 0, totalTokens, dashScopeBackup.getId());

            System.out.printf("[LIVE-4][Failover->DashScope qwen3.8-flash] elapsedMs=%d, primaryFailCount=%d, "
                            + "backupChannelId=%d, totalTokens=%d, balanceAfter=%d, answer=%s%n",
                    elapsedMs, circuitBreaker.getFailureCount(mockPrimary.getId()), dashScopeBackup.getId(),
                    totalTokens, expectedRemaining, abbreviate(content, 100));
        }
    }

    @Test
    void shouldCancelLiveDashScopeStreamOnClientAbortAndSettleEstimatedTokens() throws Exception {
        String suffix = shortId();
        String tenantId = "t_live_abort_" + suffix;
        String apiKey = "sk-live-abort-" + suffix;
        long initialBalance = 100_000L;
        insertTenant(tenantId, apiKey, initialBalance);
        LlmChannelKey channel = insertDashScopeChannel(MODEL_FLASH, "ds-live-flash-abort-" + suffix, 1);

        String reqId = "req-live-abort-" + suffix;
        createdRequestIds.add(reqId);

        int maxTokens = 512;
        String longPrompt = "请撰写一篇非常详尽的技术分析文章，全面阐述分布式AI网关的架构演进、"
                + "L1/L2缓存防穿透与击穿设计、Lua双指标令牌桶限流、TTFT快速故障转移、"
                + "SSE流式断连实时结算以及千万级MySQL慢查询与游标深分页治理，要求分8个大章节展开论述。";

        int receivedEffectiveChunks = 0;
        StringBuilder clientObservedText = new StringBuilder();
        try (Response response = executeChat(apiKey, reqId, MODEL_FLASH, longPrompt, true, maxTokens)) {
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.header("Content-Type")).contains("text/event-stream");
            assertThat(response.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo(PROVIDER_QWEN);

            BufferedSource source = response.body().source();
            while (receivedEffectiveChunks < 3) {
                String line = source.readUtf8Line();
                if (line == null) {
                    break;
                }
                if (!line.startsWith("data:")) {
                    continue;
                }
                String payload = line.substring(5).trim();
                if (payload.isEmpty() || "[DONE]".equals(payload)) {
                    continue;
                }
                String piece = extractDeltaText(payload);
                if (!piece.isEmpty()) {
                    receivedEffectiveChunks++;
                    clientObservedText.append(piece);
                }
            }
            assertThat(receivedEffectiveChunks).isGreaterThanOrEqualTo(2);
            // 客户端在收到 2~3 个有效 delta 分片后主动关闭 HTTP 流
            response.close();
        }

        BillingMessage abortMsg = awaitAndCaptureBillingMessage(reqId, 10_000L);
        assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, reqId))).isFalse();

        assertThat(abortMsg.getEstimated()).isTrue();
        assertThat(abortMsg.getFinishReason()).isEqualTo("client_abort");
        assertThat(abortMsg.getProvider()).isEqualTo(PROVIDER_QWEN);
        assertThat(abortMsg.getChannelId()).isEqualTo(channel.getId());
        assertThat(abortMsg.getCompletionTokens()).isGreaterThanOrEqualTo(1);
        assertThat(abortMsg.getTotalTokens())
                .isEqualTo(abortMsg.getPromptTokens() + abortMsg.getCompletionTokens());
        assertThat(abortMsg.getTotalTokens()).isLessThan(abortMsg.getEstimatedTokens());

        long expectedRemaining = initialBalance - abortMsg.getTotalTokens();
        Long redisAvail = quotaFreezeLuaService.getAvailableQuota(tenantId);
        assertThat(redisAvail).isEqualTo(expectedRemaining);

        settleAndAssertMysqlReconciled(
                abortMsg, tenantId, expectedRemaining, 1, abortMsg.getTotalTokens(), channel.getId());

        ApiUsageLog dbLog = apiUsageLogMapper.selectOne(new LambdaQueryWrapper<ApiUsageLog>()
                .eq(ApiUsageLog::getTenantId, tenantId)
                .eq(ApiUsageLog::getRequestId, reqId));
        assertThat(dbLog.getFinishReason()).isEqualTo("client_abort");
        assertThat(dbLog.getCompletionTokens()).isEqualTo(abortMsg.getCompletionTokens());

        System.out.printf("[LIVE-5][DashScope SSE client_abort] effectiveChunks=%d, frozenTokens=%d, "
                        + "settledPrompt=%d, settledCompletion=%d, settledTotal=%d, finishReason=%s, "
                        + "redisAvail=%d, mysqlBalance=%d, partialDelta=%s%n",
                receivedEffectiveChunks, abortMsg.getEstimatedTokens(), abortMsg.getPromptTokens(),
                abortMsg.getCompletionTokens(), abortMsg.getTotalTokens(), abortMsg.getFinishReason(),
                redisAvail, expectedRemaining, abbreviate(clientObservedText.toString(), 60));
    }

    private Response executeChat(
            String apiKey,
            String requestId,
            String model,
            String userContent,
            boolean stream,
            int maxTokens,
            String... extraHeaders) throws Exception {
        Map<String, Object> bodyMap = Map.of(
                "model", model,
                "stream", stream,
                "max_tokens", maxTokens,
                "messages", List.of(Map.of("role", "user", "content", userContent))
        );
        Request.Builder builder = new Request.Builder()
                .url("http://127.0.0.1:" + port + "/v1/chat/completions")
                .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                .header(HeaderConstants.REQUEST_ID_HEADER, requestId)
                .post(RequestBody.create(objectMapper.writeValueAsString(bodyMap), JSON));
        for (int i = 0; i + 1 < extraHeaders.length; i += 2) {
            builder.header(extraHeaders[i], extraHeaders[i + 1]);
        }
        return client.newCall(builder.build()).execute();
    }

    private StreamReadResult readFullSseStream(BufferedSource source) throws Exception {
        StreamReadResult result = new StreamReadResult();
        while (true) {
            String line = source.readUtf8Line();
            if (line == null) {
                break;
            }
            if (!line.startsWith("data:")) {
                continue;
            }
            String payload = line.substring(5).trim();
            if (payload.isEmpty()) {
                continue;
            }
            if ("[DONE]".equals(payload)) {
                result.doneReceived = true;
                break;
            }
            result.dataChunkCount++;
            JsonNode root = objectMapper.readTree(payload);
            JsonNode choices = root.get("choices");
            if (choices != null && choices.isArray() && !choices.isEmpty()) {
                JsonNode delta = choices.get(0).get("delta");
                if (delta != null) {
                    if (delta.hasNonNull("reasoning_content")) {
                        result.reasoning.append(delta.get("reasoning_content").asText(""));
                    }
                    if (delta.hasNonNull("content")) {
                        result.content.append(delta.get("content").asText(""));
                    }
                }
            }
            JsonNode usage = root.get("usage");
            if (usage != null && !usage.isNull() && usage.hasNonNull("total_tokens")) {
                result.promptTokens = usage.hasNonNull("prompt_tokens") ? usage.get("prompt_tokens").asInt() : 0;
                result.completionTokens = usage.hasNonNull("completion_tokens")
                        ? usage.get("completion_tokens").asInt()
                        : 0;
                result.totalTokens = usage.get("total_tokens").asInt();
            }
        }
        return result;
    }

    private String extractDeltaText(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            JsonNode choices = root.get("choices");
            if (choices != null && choices.isArray() && !choices.isEmpty()) {
                JsonNode delta = choices.get(0).get("delta");
                if (delta != null) {
                    StringBuilder sb = new StringBuilder();
                    if (delta.hasNonNull("reasoning_content")) {
                        sb.append(delta.get("reasoning_content").asText(""));
                    }
                    if (delta.hasNonNull("content")) {
                        sb.append(delta.get("content").asText(""));
                    }
                    return sb.toString();
                }
            }
        } catch (Exception ignored) {
            return "";
        }
        return "";
    }

    private BillingMessage awaitAndCaptureBillingMessage(String requestId) throws InterruptedException {
        return awaitAndCaptureBillingMessage(requestId, 5_000L);
    }

    private BillingMessage awaitAndCaptureBillingMessage(String requestId, long timeoutMs)
            throws InterruptedException {
        List<BillingMessage> messages = awaitAndCaptureAllBillingMessages(requestId, timeoutMs);
        return messages.get(messages.size() - 1);
    }

    private List<BillingMessage> awaitAndCaptureAllBillingMessages(String requestId) throws InterruptedException {
        return awaitAndCaptureAllBillingMessages(requestId, 5_000L);
    }

    private List<BillingMessage> awaitAndCaptureAllBillingMessages(String requestId, long timeoutMs)
            throws InterruptedException {
        final List<BillingMessage> matched = new ArrayList<>();
        awaitUntil(() -> {
            matched.clear();
            try {
                ArgumentCaptor<BillingMessage> captor = ArgumentCaptor.forClass(BillingMessage.class);
                verify(rabbitTemplate, atLeastOnce()).convertAndSend(
                        eq(MqConstants.BILLING_EXCHANGE),
                        eq(MqConstants.BILLING_ROUTING_KEY),
                        captor.capture(),
                        any(MessagePostProcessor.class),
                        any(CorrelationData.class)
                );
                for (BillingMessage msg : captor.getAllValues()) {
                    if (requestId.equals(msg.getRequestId())) {
                        matched.add(msg);
                    }
                }
            } catch (AssertionError ignored) {
                return false;
            }
            return !matched.isEmpty();
        }, timeoutMs);
        assertThat(matched).isNotEmpty();
        return List.copyOf(matched);
    }

    private void settleAndAssertMysqlReconciled(
            BillingMessage msg,
            String tenantId,
            long expectedBalanceAfter,
            int expectedEstimatedFlag,
            int expectedTotalTokens,
            long expectedChannelId) {
        boolean newlySettled = billingSettlementTxService.settleAtomically(msg);
        assertThat(newlySettled).isTrue();

        TenantAccount updated = tenantAccountMapper.selectByTenantId(tenantId);
        assertThat(updated).isNotNull();
        assertThat(updated.getBalance()).isEqualTo(expectedBalanceAfter);

        ApiUsageLog logRow = apiUsageLogMapper.selectOne(new LambdaQueryWrapper<ApiUsageLog>()
                .eq(ApiUsageLog::getTenantId, tenantId)
                .eq(ApiUsageLog::getRequestId, msg.getRequestId()));
        assertThat(logRow).isNotNull();
        assertThat(logRow.getTenantId()).isEqualTo(tenantId);
        assertThat(logRow.getProvider()).isEqualTo(PROVIDER_QWEN);
        assertThat(logRow.getChannelId()).isEqualTo(expectedChannelId);
        assertThat(logRow.getTotalTokens()).isEqualTo(expectedTotalTokens);
        assertThat(logRow.getEstimated()).isEqualTo(expectedEstimatedFlag);
    }

    private void insertTenant(String tenantId, String apiKey, long balance) {
        TenantAccount account = TenantAccount.builder()
                .tenantId(tenantId)
                .tenantName("DashScope Live Test " + tenantId)
                .apiKey(apiKey)
                .balance(balance)
                .frozenTokens(0L)
                .rpmLimit(1000)
                .tpmLimit(2_000_000)
                .status(1)
                .version(0)
                .build();
        tenantAccountMapper.insert(account);
        createdTenantIds.add(tenantId);
        createdApiKeys.add(apiKey);
    }

    private LlmChannelKey insertDashScopeChannel(String modelName, String channelName, int priority) {
        return insertChannel(
                channelName,
                PROVIDER_QWEN,
                modelName,
                DASHSCOPE_BASE_URL,
                resolveDashScopeApiKey(),
                100,
                priority
        );
    }

    private LlmChannelKey insertChannel(
            String channelName,
            String provider,
            String modelName,
            String baseUrl,
            String secretKey,
            int weight,
            int priority) {
        LlmChannelKey channel = LlmChannelKey.builder()
                .channelName(channelName)
                .provider(provider)
                .modelName(modelName)
                .baseUrl(baseUrl)
                .secretKey(secretKey)
                .weight(weight)
                .priority(priority)
                .status(1)
                .build();
        channelKeyMapper.insert(channel);
        createdChannelIds.add(channel.getId());
        llmChannelRouter.evictModelCache(modelName);
        return channel;
    }

    private void awaitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50L);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private String abbreviate(String text, int maxLen) {
        if (text == null) {
            return "";
        }
        String singleLine = text.replace('\r', ' ').replace('\n', ' ').trim();
        return singleLine.length() <= maxLen ? singleLine : singleLine.substring(0, maxLen) + "...";
    }

    private String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    private static class StreamReadResult {
        private int dataChunkCount;
        private boolean doneReceived;
        private int promptTokens;
        private int completionTokens;
        private int totalTokens;
        private final StringBuilder reasoning = new StringBuilder();
        private final StringBuilder content = new StringBuilder();
    }
}
