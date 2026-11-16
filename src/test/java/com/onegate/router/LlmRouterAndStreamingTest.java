package com.onegate.router;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.auth.TenantAuthCacheService;
import com.onegate.billing.QuotaFreezeLuaService;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.constant.MqConstants;
import com.onegate.common.constant.RedisKeys;
import com.onegate.controller.MockUpstreamController;
import com.onegate.domain.dto.BillingMessage;
import com.onegate.domain.entity.LlmChannelKey;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.LlmChannelKeyMapper;
import com.onegate.domain.mapper.TenantAccountMapper;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

/**
 * 模块 2 端到端集成测试：
 * 启动真实随机端口 Servlet 容器 + 真实 MySQL 9.0.1 + 真实 Redis 7.0.15，
 * 使用真实 OkHttp 客户端连接 {@code /v1/chat/completions} 与内置 {@code /mock-llm/**}，
 * 验证非流式/SSE 流式、TTFT 超时取消切换、429/500 熔断冷却、客户端断连中断上游与多厂商舱壁隔离。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration",
                "onegate.mock-enabled=true",
                "onegate.non-stream-timeout-ms=450",
                "onegate.ttft-timeout-ms=450",
                "onegate.failure-threshold=3",
                "onegate.cooldown-seconds=30",
                "onegate.max-failover-attempts=2",
                "onegate.pools.deepseek.core-size=2",
                "onegate.pools.deepseek.max-size=2",
                "onegate.pools.deepseek.queue-capacity=1"
        }
)
class LlmRouterAndStreamingTest {

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    @LocalServerPort
    private int port;

    @MockitoBean
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private TenantAccountMapper tenantAccountMapper;

    @Autowired
    private LlmChannelKeyMapper channelKeyMapper;

    @Autowired
    private TenantAuthCacheService tenantAuthCacheService;

    @Autowired
    private QuotaFreezeLuaService quotaFreezeLuaService;

    @Autowired
    private ChannelCircuitBreaker circuitBreaker;

    @Autowired
    private MockUpstreamController mockUpstreamController;

    @Autowired
    private ProviderPoolManager providerPoolManager;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(15, TimeUnit.SECONDS)
            .writeTimeout(5, TimeUnit.SECONDS)
            .build();

    private final List<String> createdTenantIds = new ArrayList<>();
    private final List<String> createdApiKeys = new ArrayList<>();
    private final List<Long> createdChannelIds = new ArrayList<>();
    private final List<String> createdRequestIds = new ArrayList<>();

    @AfterEach
    void cleanup() {
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
    }

    @Test
    void shouldProxyNonStreamingAndSettleExactTokens() throws Exception {
        String suffix = shortId();
        String tenantId = "t_rt_nonstream_" + suffix;
        String apiKey = "sk-rt-nonstream-" + suffix;
        String model = "test-ds-nonstream-" + suffix;
        insertTenant(tenantId, apiKey, 10_000L);
        insertChannelPair(model, "DEEPSEEK");

        String reqId = "req-nonstream-" + suffix;
        createdRequestIds.add(reqId);

        Request request = new Request.Builder()
                .url(gatewayUrl("/v1/chat/completions"))
                .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                .header(HeaderConstants.REQUEST_ID_HEADER, reqId)
                .post(RequestBody.create(chatBody(model, "Hello OneGate", false, 128), JSON))
                .build();

        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.header(HeaderConstants.REQUEST_ID_HEADER)).isEqualTo(reqId);
            assertThat(response.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo("DEEPSEEK");
            assertThat(response.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("false");

            JsonNode root = objectMapper.readTree(response.body().string());
            String content = root.at("/choices/0/message/content").asText();
            int totalTokens = root.at("/usage/total_tokens").asInt();
            assertThat(content).contains("[primary]");
            assertThat(totalTokens).isGreaterThan(0);

            assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, reqId))).isFalse();
            assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isEqualTo(10_000L - totalTokens);
        }

        ArgumentCaptor<BillingMessage> billingCaptor = ArgumentCaptor.forClass(BillingMessage.class);
        verify(rabbitTemplate, atLeastOnce()).convertAndSend(
                eq(MqConstants.BILLING_EXCHANGE),
                eq(MqConstants.BILLING_ROUTING_KEY),
                billingCaptor.capture(),
                any(MessagePostProcessor.class),
                any(CorrelationData.class)
        );
        BillingMessage sent = billingCaptor.getValue();
        assertThat(sent.getRequestId()).isEqualTo(reqId);
        assertThat(sent.getProvider()).isEqualTo("DEEPSEEK");
        assertThat(sent.getEstimated()).isFalse();
    }

    @Test
    void shouldFailoverOn500TtftTimeout429AndTripCircuitBreakerCooldown() throws Exception {
        String suffix = shortId();
        String tenantId = "t_rt_cb_" + suffix;
        String apiKey = "sk-rt-cb-" + suffix;
        String model = "test-ds-cb-" + suffix;
        insertTenant(tenantId, apiKey, 50_000L);
        List<LlmChannelKey> pair = insertChannelPair(model, "DEEPSEEK");
        Long primaryId = pair.get(0).getId();

        // 1. 主渠道 HTTP 500 -> 无缝切备用渠道 (fail=1)
        String req1 = "req-cb-500-" + suffix;
        createdRequestIds.add(req1);
        try (Response resp1 = callChat(apiKey, req1, model, false, 64,
                MockUpstreamController.HEADER_MOCK_PRIMARY_MODE, "error-500")) {
            assertThat(resp1.code()).isEqualTo(200);
            assertThat(resp1.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("true");
            assertThat(resp1.body().string()).contains("[backup]");
        }
        assertThat(circuitBreaker.getFailureCount(primaryId)).isEqualTo(1L);
        assertThat(circuitBreaker.isInCooldown(primaryId)).isFalse();

        // 2. 主渠道 TTFT 超时 (delay=2500ms > 450ms) -> 立即 Call.cancel() 并切备 (fail=2)
        String req2 = "req-cb-ttft-" + suffix;
        createdRequestIds.add(req2);
        long startNanos = System.nanoTime();
        try (Response resp2 = callChat(apiKey, req2, model, false, 64,
                MockUpstreamController.HEADER_MOCK_PRIMARY_MODE, "ttft-timeout",
                MockUpstreamController.HEADER_MOCK_DELAY_MS, "2500")) {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            assertThat(resp2.code()).isEqualTo(200);
            assertThat(resp2.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("true");
            assertThat(resp2.body().string()).contains("[backup]");
            assertThat(elapsedMs).isLessThan(1800L);
        }
        assertThat(circuitBreaker.getFailureCount(primaryId)).isEqualTo(2L);

        // 3. 主渠道 HTTP 429 (第3次失败) -> 触发 Redis 熔断冷却 (cooldown=30s) 并切备成功
        String req3 = "req-cb-429-" + suffix;
        createdRequestIds.add(req3);
        try (Response resp3 = callChat(apiKey, req3, model, false, 64,
                MockUpstreamController.HEADER_MOCK_PRIMARY_MODE, "rate-429")) {
            assertThat(resp3.code()).isEqualTo(200);
            assertThat(resp3.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("true");
            assertThat(resp3.body().string()).contains("[backup]");
        }
        assertThat(circuitBreaker.isInCooldown(primaryId)).isTrue();
        assertThat(circuitBreaker.getFailureCount(primaryId)).isEqualTo(0L);

        // 4. 冷却期内直接屏蔽主渠道，无需任何错误头也直接走备用渠道
        String req4 = "req-cb-cool-" + suffix;
        createdRequestIds.add(req4);
        try (Response resp4 = callChat(apiKey, req4, model, false, 64)) {
            assertThat(resp4.code()).isEqualTo(200);
            assertThat(resp4.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("false");
            assertThat(resp4.body().string()).contains("[backup]");
        }
    }

    @Test
    void shouldFailoverSseStreamOnTtftTimeoutBeforeFirstChunkAndSettleUsage() throws Exception {
        String suffix = shortId();
        String tenantId = "t_rt_sse_" + suffix;
        String apiKey = "sk-rt-sse-" + suffix;
        String model = "test-ds-sse-" + suffix;
        insertTenant(tenantId, apiKey, 20_000L);
        insertChannelPair(model, "DEEPSEEK");

        String reqId = "req-sse-ttft-" + suffix;
        createdRequestIds.add(reqId);

        try (Response response = callChat(apiKey, reqId, model, true, 128,
                MockUpstreamController.HEADER_MOCK_PRIMARY_MODE, "ttft-timeout",
                MockUpstreamController.HEADER_MOCK_DELAY_MS, "2500")) {
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.header("Content-Type")).contains("text/event-stream");
            assertThat(response.header(HeaderConstants.FAILOVER_HEADER)).isEqualTo("true");

            List<String> dataEvents = new ArrayList<>();
            BufferedSource source = response.body().source();
            while (true) {
                String line = source.readUtf8Line();
                if (line == null) {
                    break;
                }
                if (line.startsWith("data:")) {
                    String payload = line.substring(5).trim();
                    dataEvents.add(payload);
                    if ("[DONE]".equals(payload)) {
                        break;
                    }
                }
            }

            assertThat(dataEvents.size()).isGreaterThanOrEqualTo(6);
            assertThat(dataEvents.get(0)).contains("[backup]");
            assertThat(dataEvents.get(dataEvents.size() - 1)).isEqualTo("[DONE]");

            // 提取尾包 usage 并核对精准核销退还差额
            String tailJson = dataEvents.get(dataEvents.size() - 2);
            int actualTotal = objectMapper.readTree(tailJson).at("/usage/total_tokens").asInt();
            assertThat(actualTotal).isGreaterThan(0);

            awaitUntil(() -> !Boolean.TRUE.equals(
                    stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, reqId))), 3000);
            assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isEqualTo(20_000L - actualTotal);
        }
    }

    @Test
    void shouldCancelUpstreamAndSettleEstimatedTokensWhenClientAbortsSseMidStream() throws Exception {
        String suffix = shortId();
        String tenantId = "t_rt_abort_" + suffix;
        String apiKey = "sk-rt-abort-" + suffix;
        String model = "test-ds-abort-" + suffix;
        insertTenant(tenantId, apiKey, 10_000L);
        insertChannelPair(model, "DEEPSEEK");

        mockUpstreamController.resetCounters();

        String reqId = "req-sse-abort-" + suffix;
        createdRequestIds.add(reqId);

        Request request = new Request.Builder()
                .url(gatewayUrl("/v1/chat/completions"))
                .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                .header(HeaderConstants.REQUEST_ID_HEADER, reqId)
                .header(MockUpstreamController.HEADER_MOCK_MODE, "slow-stream")
                .header(MockUpstreamController.HEADER_MOCK_CHUNK_COUNT, "15")
                .header(MockUpstreamController.HEADER_MOCK_CHUNK_DELAY_MS, "180")
                .post(RequestBody.create(chatBody(model, "Tell me a long story", true, 500), JSON))
                .build();

        Call call = client.newCall(request);
        try (Response response = call.execute()) {
            assertThat(response.code()).isEqualTo(200);
            BufferedSource source = response.body().source();
            int receivedChunks = 0;
            while (receivedChunks < 2) {
                String line = source.readUtf8Line();
                if (line == null) {
                    break;
                }
                if (line.startsWith("data:") && !"[DONE]".equals(line.substring(5).trim())) {
                    receivedChunks++;
                }
            }
            assertThat(receivedChunks).isEqualTo(2);
            // 主动断连: 立即关闭客户端流并取消 TCP 连接
            call.cancel();
        }

        // 等待异步写出感知断连 -> 联动 Call.cancel() 终止上游并完成估算核销
        awaitUntil(() -> mockUpstreamController.getAbortedStreamCount().get() >= 1
                && !Boolean.TRUE.equals(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, reqId))), 5000);

        int chunksSentByUpstream = mockUpstreamController.getLastStreamChunksWritten().get();
        assertThat(chunksSentByUpstream).isLessThan(15);

        Long availAfterSettle = quotaFreezeLuaService.getAvailableQuota(tenantId);
        // 预占冻结了 prompt + 500 (>500)，实际只推了少量分片，估算扣款远小于冻结额
        assertThat(availAfterSettle).isBetween(9900L, 9999L);

        ArgumentCaptor<BillingMessage> captor = ArgumentCaptor.forClass(BillingMessage.class);
        verify(rabbitTemplate, atLeastOnce()).convertAndSend(
                eq(MqConstants.BILLING_EXCHANGE),
                eq(MqConstants.BILLING_ROUTING_KEY),
                captor.capture(),
                any(MessagePostProcessor.class),
                any(CorrelationData.class)
        );
        BillingMessage abortBilling = captor.getAllValues().stream()
                .filter(m -> reqId.equals(m.getRequestId()))
                .findFirst()
                .orElseThrow();
        assertThat(abortBilling.getEstimated()).isTrue();
        assertThat(abortBilling.getFinishReason()).isEqualTo("client_abort");
        assertThat(abortBilling.getCompletionTokens()).isGreaterThanOrEqualTo(1);
        assertThat(abortBilling.getTotalTokens())
                .isEqualTo(abortBilling.getPromptTokens() + abortBilling.getCompletionTokens());
        System.out.printf("[TEST] SSE client-abort: upstreamChunks=%d/15, settledTotalTokens=%d, availAfter=%d%n",
                chunksSentByUpstream, abortBilling.getTotalTokens(), availAfterSettle);
    }

    @Test
    void shouldIsolateSaturatedDeepseekPoolWithoutBlockingQwenPool() throws Exception {
        String suffix = shortId();
        String tenantId = "t_rt_bulk_" + suffix;
        String apiKey = "sk-rt-bulk-" + suffix;
        String dsModel = "test-ds-bulk-" + suffix;
        String qwenModel = "test-qwen-bulk-" + suffix;
        insertTenant(tenantId, apiKey, 100_000L);
        insertSingleChannel(dsModel, "DEEPSEEK", "http://127.0.0.1:" + port + "/mock-llm/primary?delayMs=1200");
        insertSingleChannel(qwenModel, "QWEN", "http://127.0.0.1:" + port + "/mock-llm/primary?delayMs=20");

        // 1. 验证 GET /v1/models 可见
        Request modelsReq = new Request.Builder()
                .url(gatewayUrl("/v1/models"))
                .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                .get()
                .build();
        try (Response modelsResp = client.newCall(modelsReq).execute()) {
            assertThat(modelsResp.code()).isEqualTo(200);
            assertThat(modelsResp.body().string()).contains(dsModel, qwenModel);
        }

        // 2. 依次占满 DEEPSEEK 隔离线程池 (core=2, max=2, queue=1 -> 2 active + 1 queued), 避免并发争抢单容量队列误伤第3个请求
        ThreadPoolExecutor dsExecutor = providerPoolManager.getExecutor("DEEPSEEK");
        assertThat(waitUntil(() -> dsExecutor.getActiveCount() == 0 && dsExecutor.getQueue().isEmpty(), 3000L))
                .isTrue();

        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<Integer>> slowFutures = new ArrayList<>();
        try {
            // 先让前 2 个慢请求稳定占用 2 个工作线程
            for (int i = 0; i < 2; i++) {
                final int activeTarget = i + 1;
                String reqId = "req-bulk-slow-" + suffix + "-" + i;
                createdRequestIds.add(reqId);
                slowFutures.add(pool.submit(() -> {
                    try (Response r = callChat(apiKey, reqId, dsModel, false, 32)) {
                        return r.code();
                    }
                }));
                assertThat(waitUntil(() -> dsExecutor.getActiveCount() >= activeTarget, 3000L)).isTrue();
            }
            // 再提交第 3 个慢请求稳定进入有界队列 (queueSize=1)
            String queuedReqId = "req-bulk-slow-" + suffix + "-2";
            createdRequestIds.add(queuedReqId);
            slowFutures.add(pool.submit(() -> {
                try (Response r = callChat(apiKey, queuedReqId, dsModel, false, 32)) {
                    return r.code();
                }
            }));
            assertThat(waitUntil(() -> dsExecutor.getQueue().size() == 1, 3000L)).isTrue();

            // 3. 第 4 个 DEEPSEEK 请求应因舱壁队列满立刻被拒绝返回 503，且回滚预冻
            String rejectedReqId = "req-bulk-rej-" + suffix;
            createdRequestIds.add(rejectedReqId);
            try (Response rejectedResp = callChat(apiKey, rejectedReqId, dsModel, false, 32)) {
                assertThat(rejectedResp.code()).isEqualTo(503);
                assertThat(rejectedResp.body().string()).contains("service_unavailable");
            }
            assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, rejectedReqId))).isFalse();

            // 4. 同一时刻 QWEN 请求走独立线程池, 不受 DEEPSEEK 饱和影响, 毫秒级 200 返回
            String qwenReqId = "req-bulk-qwen-" + suffix;
            createdRequestIds.add(qwenReqId);
            long qwenStart = System.nanoTime();
            try (Response qwenResp = callChat(apiKey, qwenReqId, qwenModel, false, 32)) {
                long qwenMs = (System.nanoTime() - qwenStart) / 1_000_000L;
                assertThat(qwenResp.code()).isEqualTo(200);
                assertThat(qwenResp.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo("QWEN");
                assertThat(qwenMs).isLessThan(800L);
                System.out.printf("[TEST] Bulkhead isolation: DEEPSEEK rejected=503, QWEN latency=%d ms%n", qwenMs);
            }

            for (Future<Integer> f : slowFutures) {
                f.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private boolean waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                return false;
            }
            Thread.sleep(10L);
        }
        return true;
    }

    private Response callChat(String apiKey, String requestId, String model, boolean stream, int maxTokens,
                              String... extraHeaders) throws Exception {
        Request.Builder builder = new Request.Builder()
                .url(gatewayUrl("/v1/chat/completions"))
                .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                .header(HeaderConstants.REQUEST_ID_HEADER, requestId)
                .post(RequestBody.create(chatBody(model, "Hello test prompt", stream, maxTokens), JSON));
        for (int i = 0; i + 1 < extraHeaders.length; i += 2) {
            builder.header(extraHeaders[i], extraHeaders[i + 1]);
        }
        return client.newCall(builder.build()).execute();
    }

    private String gatewayUrl(String path) {
        return "http://127.0.0.1:" + port + path;
    }

    private String chatBody(String model, String content, boolean stream, int maxTokens) {
        return String.format(
                "{\"model\":\"%s\",\"stream\":%s,\"max_tokens\":%d,\"messages\":[{\"role\":\"user\",\"content\":\"%s\"}]}",
                model, stream, maxTokens, content
        );
    }

    private void insertTenant(String tenantId, String apiKey, long balance) {
        TenantAccount account = TenantAccount.builder()
                .tenantId(tenantId)
                .tenantName("Router Test " + tenantId)
                .apiKey(apiKey)
                .balance(balance)
                .frozenTokens(0L)
                .rpmLimit(5000)
                .tpmLimit(5_000_000)
                .status(1)
                .version(0)
                .build();
        tenantAccountMapper.insert(account);
        createdTenantIds.add(tenantId);
        createdApiKeys.add(apiKey);
    }

    private List<LlmChannelKey> insertChannelPair(String modelName, String provider) {
        LlmChannelKey primary = LlmChannelKey.builder()
                .channelName(modelName + "-primary")
                .provider(provider)
                .modelName(modelName)
                .baseUrl("http://127.0.0.1:" + port + "/mock-llm/primary")
                .secretKey("mock-primary-key")
                .weight(100)
                .priority(1)
                .status(1)
                .build();
        channelKeyMapper.insert(primary);
        createdChannelIds.add(primary.getId());

        LlmChannelKey backup = LlmChannelKey.builder()
                .channelName(modelName + "-backup")
                .provider(provider)
                .modelName(modelName)
                .baseUrl("http://127.0.0.1:" + port + "/mock-llm/backup")
                .secretKey("mock-backup-key")
                .weight(50)
                .priority(2)
                .status(1)
                .build();
        channelKeyMapper.insert(backup);
        createdChannelIds.add(backup.getId());
        return List.of(primary, backup);
    }

    private void insertSingleChannel(String modelName, String provider, String baseUrl) {
        LlmChannelKey ch = LlmChannelKey.builder()
                .channelName(modelName + "-single-" + shortId())
                .provider(provider)
                .modelName(modelName)
                .baseUrl(baseUrl)
                .secretKey("mock-single-key")
                .weight(100)
                .priority(1)
                .status(1)
                .build();
        channelKeyMapper.insert(ch);
        createdChannelIds.add(ch.getId());
    }

    private void awaitUntil(java.util.function.BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(30L);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }
}
