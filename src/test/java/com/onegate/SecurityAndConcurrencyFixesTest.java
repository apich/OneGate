package com.onegate;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.auth.AdminAuthInterceptor;
import com.onegate.auth.AdminTokenVerifier;
import com.onegate.auth.RateLimiterLuaService;
import com.onegate.auth.TenantAuthCacheService;
import com.onegate.auth.TenantContext;
import com.onegate.billing.BillingSettlementTxService;
import com.onegate.billing.QuotaFreezeLuaService;
import com.onegate.common.config.OneGateProperties;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.constant.RedisKeys;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.common.model.RateLimitResult;
import com.onegate.controller.MockUpstreamController;
import com.onegate.domain.dto.AdminKeyUpdateRequest;
import com.onegate.domain.dto.AdminRechargeRequest;
import com.onegate.domain.dto.BillingMessage;
import com.onegate.domain.dto.ChatCompletionRequest;
import com.onegate.domain.dto.TenantAuthInfo;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.entity.BillingDeadLetterRecord;
import com.onegate.domain.entity.LlmChannelKey;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import com.onegate.domain.mapper.BillingDeadLetterMapper;
import com.onegate.domain.mapper.LlmChannelKeyMapper;
import com.onegate.domain.mapper.TenantAccountMapper;
import com.onegate.router.ChannelCircuitBreaker;
import com.onegate.router.TokenEstimator;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * P0/P1 安全加固、并发防重与故障边界回归测试套件。
 * <p>
 * 默认不开启 {@code onegate.mock-enabled} (保持生产默认 {@code false})，
 * 直连真实 MySQL 9.0.1 与 Redis 7.0.15，仅隔离 RabbitMQ 自动装配。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration",
                "onegate.admin-token=onegate-admin-secret",
                "onegate.ttft-timeout-ms=3000",
                "onegate.non-stream-timeout-ms=3000",
                "onegate.failure-threshold=3",
                "onegate.cooldown-seconds=30",
                "onegate.max-failover-attempts=2"
        }
)
@AutoConfigureMockMvc
class SecurityAndConcurrencyFixesTest {

    private static final String ADMIN_TOKEN = "onegate-admin-secret";

    @MockitoBean
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private OneGateProperties properties;

    @Autowired
    private AdminTokenVerifier adminTokenVerifier;

    @Autowired
    private RateLimiterLuaService rateLimiterLuaService;

    @Autowired
    private QuotaFreezeLuaService quotaFreezeLuaService;

    @Autowired
    private BillingSettlementTxService billingSettlementTxService;

    @Autowired
    private TenantAuthCacheService tenantAuthCacheService;

    @Autowired
    private ChannelCircuitBreaker circuitBreaker;

    @Autowired
    private TokenEstimator tokenEstimator;

    @Autowired
    private TenantAccountMapper tenantAccountMapper;

    @Autowired
    private LlmChannelKeyMapper channelKeyMapper;

    @Autowired
    private ApiUsageLogMapper apiUsageLogMapper;

    @Autowired
    private BillingDeadLetterMapper billingDeadLetterMapper;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private final List<String> createdTenantIds = new ArrayList<>();
    private final List<String> createdApiKeys = new ArrayList<>();
    private final List<Long> createdChannelIds = new ArrayList<>();
    private final List<String> createdRequestIds = new ArrayList<>();
    private final List<String> createdRedisKeys = new ArrayList<>();

    @AfterEach
    void cleanup() {
        for (String reqId : createdRequestIds) {
            billingDeadLetterMapper.delete(
                    new LambdaQueryWrapper<BillingDeadLetterRecord>().eq(BillingDeadLetterRecord::getRequestId, reqId));
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
            apiUsageLogMapper.delete(new LambdaQueryWrapper<ApiUsageLog>().eq(ApiUsageLog::getTenantId, tenantId));
            billingDeadLetterMapper.delete(
                    new LambdaQueryWrapper<BillingDeadLetterRecord>().eq(BillingDeadLetterRecord::getTenantId, tenantId));
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
        for (String key : createdRedisKeys) {
            stringRedisTemplate.delete(key);
        }
        createdChannelIds.clear();
        createdApiKeys.clear();
        createdTenantIds.clear();
        createdRequestIds.clear();
        createdRedisKeys.clear();
        TenantContext.clear();
    }

    @Test
    void shouldRejectBlankOrMissingAdminTokenAndReturnMaskedTenantDto() throws Exception {
        // 1. 当服务端 onegate.admin-token 为空串时，传 ""、空白、null、任意口令一律拒绝（防空串等值绕过）
        OneGateProperties blankTokenProps = new OneGateProperties();
        assertThat(blankTokenProps.getAdminToken()).isEmpty();
        AdminTokenVerifier emptyVerifier = new AdminTokenVerifier(blankTokenProps);
        AdminAuthInterceptor emptyInterceptor = new AdminAuthInterceptor(emptyVerifier);

        for (String candidate : new String[]{"", "   ", null, "onegate-admin-secret"}) {
            assertThatThrownBy(() -> emptyVerifier.verifyOrThrow(candidate))
                    .isInstanceOf(GatewayException.class)
                    .satisfies(ex -> assertThat(((GatewayException) ex).getErrorCode())
                            .isEqualTo(ErrorCode.ADMIN_UNAUTHORIZED));
        }

        MockHttpServletRequest interceptorReq = new MockHttpServletRequest("GET", "/admin/tenants/t_1001");
        interceptorReq.addHeader(HeaderConstants.ADMIN_TOKEN_HEADER, "");
        assertThatThrownBy(() -> emptyInterceptor.preHandle(interceptorReq, new MockHttpServletResponse(), new Object()))
                .isInstanceOf(GatewayException.class)
                .satisfies(ex -> assertThat(((GatewayException) ex).getErrorCode())
                        .isEqualTo(ErrorCode.ADMIN_UNAUTHORIZED));

        // 2. 配置正确 admin-token 时，空头/空值/错口令请求 /admin/** 一律被拦截返回 403
        String suffix = shortId();
        String tenantId = "t_sec_admin_" + suffix;
        String rawKey = "sk-sec-secret-" + suffix + "9876";
        insertTenant(tenantId, rawKey, 2000L, 200, 100_000, 1);

        mockMvc.perform(get("/admin/tenants/{tenantId}", tenantId))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("admin_unauthorized"));

        mockMvc.perform(get("/admin/tenants/{tenantId}", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ""))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("admin_unauthorized"));

        mockMvc.perform(get("/admin/tenants/{tenantId}", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, "wrong-admin-token"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("admin_unauthorized"));

        // 3. 合法口令 GET /admin/tenants/{id} 仅返回脱敏 apiKeyMasked，apiKey 严格为 null（不序列化）
        assertThatCodeTokenVerifierMatches();
        mockMvc.perform(get("/admin/tenants/{tenantId}", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.tenantId").value(tenantId))
                .andExpect(jsonPath("$.data.apiKey").doesNotExist())
                .andExpect(jsonPath("$.data.apiKeyMasked").value("sk-****9876"));
    }

    private void assertThatCodeTokenVerifierMatches() {
        adminTokenVerifier.verifyOrThrow(ADMIN_TOKEN);
    }

    @Test
    void shouldDisableMockUpstreamAndIgnoreMockPassthroughHeadersByDefault() throws Exception {
        // 1. 验证默认配置关闭了 MockUpstreamController
        assertThat(properties.isMockEnabled()).isFalse();
        assertThat(applicationContext.getBeansOfType(MockUpstreamController.class)).isEmpty();

        mockMvc.perform(post("/mock-llm/primary/v1/chat/completions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"model\":\"deepseek-chat\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"))
                .andExpect(status().isNotFound());

        // 2. 启动本地真实上游探测 mock-passthrough 请求头在默认配置下不被透传
        AtomicReference<Headers> capturedHeaders = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            Headers reqHeaders = new Headers();
            reqHeaders.putAll(exchange.getRequestHeaders());
            capturedHeaders.set(reqHeaders);

            byte[] okJson = """
                    {"id":"chatcmpl-sec-default","object":"chat.completion","created":1731500000,"model":"sec-default-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}"""
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, okJson.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(okJson);
            }
        });
        server.start();

        try {
            String suffix = shortId();
            String model = "sec-mock-off-" + suffix;
            String tenantId = "t_sec_mock_" + suffix;
            String apiKey = "sk-sec-mock-" + suffix;
            String reqId = "req-sec-mock-" + suffix;
            createdRequestIds.add(reqId);
            insertTenant(tenantId, apiKey, 5000L, 100, 100_000, 1);
            insertChannel("sec-upstream-" + suffix, "DEEPSEEK", model,
                    "http://127.0.0.1:" + server.getAddress().getPort(), 1);

            String body = objectMapper.writeValueAsString(Map.of(
                    "model", model,
                    "messages", List.of(Map.of("role", "user", "content", "hello")),
                    "max_tokens", 16
            ));

            mockMvc.perform(post("/v1/chat/completions")
                            .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                            .header(HeaderConstants.REQUEST_ID_HEADER, reqId)
                            .header("X-Mock-Mode", "error_500")
                            .header("X-Mock-Delay-Ms", "5000")
                            .header("X-Mock-Content", "injected-spoof")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.choices[0].message.content").value("ok"));

            Headers upstreamHeaders = capturedHeaders.get();
            assertThat(upstreamHeaders).isNotNull();
            assertThat(upstreamHeaders.getFirst("X-Mock-Mode")).isNull();
            assertThat(upstreamHeaders.getFirst("X-Mock-Delay-Ms")).isNull();
            assertThat(upstreamHeaders.getFirst("X-Mock-Content")).isNull();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void shouldRejectOverflowMaxTokensAndOverlongRequestIdWithHttp400BeforeLua() throws Exception {
        String suffix = shortId();
        String tenantId = "t_sec_val_" + suffix;
        String apiKey = "sk-sec-val-" + suffix;
        insertTenant(tenantId, apiKey, 1000L, 100, 100_000, 1);

        // 1. max_tokens = Integer.MAX_VALUE 在 DTO @Valid 阶段被直接拦截返回 400，不得穿透到 Lua 扣减或溢出
        String reqOverflow = "req-overflow-" + suffix;
        createdRequestIds.add(reqOverflow);
        String overflowJson = """
                {"model":"deepseek-chat","messages":[{"role":"user","content":"hi"}],"max_tokens":2147483647}""";
        mockMvc.perform(post("/v1/chat/completions")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                        .header(HeaderConstants.REQUEST_ID_HEADER, reqOverflow)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(overflowJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("invalid_request"));

        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isNull();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantId, reqOverflow))).isFalse();

        // 兜底验证 TokenEstimator 在直接调用场景下同样封顶防 int 溢出
        ChatCompletionRequest extremeReq = ChatCompletionRequest.builder()
                .model("deepseek-chat")
                .messages(List.of(ChatCompletionRequest.ChatMessage.builder().role("user").content("hello").build()))
                .maxTokens(Integer.MAX_VALUE)
                .build();
        int estimated = tokenEstimator.estimateTotalFrozenTokens(extremeReq);
        assertThat(estimated).isPositive().isLessThanOrEqualTo(properties.getMaxTokensLimit() + 100);

        // 2. 超长 (100 字符) 与含非法字符的 X-OneGate-Request-Id 在拦截器阶段直接返回 400
        String overlongReqId = "r".repeat(100);
        createdRequestIds.add(overlongReqId);
        String validJson = """
                {"model":"deepseek-chat","messages":[{"role":"user","content":"hi"}],"max_tokens":32}""";
        mockMvc.perform(post("/v1/chat/completions")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                        .header(HeaderConstants.REQUEST_ID_HEADER, overlongReqId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("invalid_request"));

        mockMvc.perform(post("/v1/chat/completions")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                        .header(HeaderConstants.REQUEST_ID_HEADER, "bad/id with space!")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("invalid_request"));

        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isNull();
    }

    @Test
    void shouldKeepDistinctRpmEntriesAndEnforcePerTenantReplayAndCrossTenantIsolation() {
        String suffix = shortId();
        String tenantA = "t_sec_iso_a_" + suffix;
        String tenantB = "t_sec_iso_b_" + suffix;
        insertTenant(tenantA, "sk-sec-iso-a-" + suffix, 1000L, 10, 100_000, 1);
        insertTenant(tenantB, "sk-sec-iso-b-" + suffix, 1000L, 10, 100_000, 1);

        // 1. 同一 requestId 连续调用 RateLimiterLuaService.tryAcquire 两次，currentRpm 必须严格递增至 2
        String sameRpmReqId = "req-same-rpm-" + suffix;
        TenantAuthInfo tenantAInfo = TenantAuthInfo.builder()
                .exists(true)
                .tenantId(tenantA)
                .rpmLimit(10)
                .tpmLimit(100_000)
                .status(1)
                .build();
        RateLimitResult rpm1 = rateLimiterLuaService.tryAcquire(tenantAInfo, sameRpmReqId, 50);
        RateLimitResult rpm2 = rateLimiterLuaService.tryAcquire(tenantAInfo, sameRpmReqId, 50);
        assertThat(rpm1.isAllowed()).isTrue();
        assertThat(rpm1.getCurrentRpm()).isEqualTo(1L);
        assertThat(rpm2.isAllowed()).isTrue();
        assertThat(rpm2.getCurrentRpm()).isEqualTo(2L);
        assertThat(stringRedisTemplate.opsForZSet().zCard(RedisKeys.rateLimitRpmKey(tenantA))).isEqualTo(2L);

        // 2. 同租户结算完成后 seenKey 持久拦截同一 requestId 重放冻结
        String replayReqId = "req-replay-" + suffix;
        createdRequestIds.add(replayReqId);
        long afterFreeze = quotaFreezeLuaService.freezeQuotaOrThrow(tenantA, replayReqId, 200);
        assertThat(afterFreeze).isEqualTo(800L);
        QuotaFreezeLuaService.SettleResult settled = quotaFreezeLuaService.settleQuota(tenantA, replayReqId, 120);
        assertThat(settled.isApplied()).isTrue();
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantA)).isEqualTo(880L);
        assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantA, replayReqId))).isFalse();
        assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaSeenKey(tenantA, replayReqId))).isTrue();

        assertThatThrownBy(() -> quotaFreezeLuaService.freezeQuotaOrThrow(tenantA, replayReqId, 200))
                .isInstanceOf(GatewayException.class)
                .hasMessageContaining("Duplicate or replayed request_id")
                .satisfies(ex -> assertThat(((GatewayException) ex).getErrorCode())
                        .isEqualTo(ErrorCode.INVALID_REQUEST));

        // 3. 跨租户 t_A 与 t_B 使用完全相同的 requestId，依赖 (tenant_id, request_id) 联合唯一约束互不碰撞、各自真实扣款
        String sharedReqId = "req-cross-tenant-same-" + suffix;
        createdRequestIds.add(sharedReqId);

        assertThat(billingSettlementTxService.settleAtomically(buildBillingMessage(tenantA, sharedReqId, 180))).isTrue();
        assertThat(billingSettlementTxService.settleAtomically(buildBillingMessage(tenantB, sharedReqId, 250))).isTrue();
        // 同租户重复投递依然严格幂等返回 false
        assertThat(billingSettlementTxService.settleAtomically(buildBillingMessage(tenantA, sharedReqId, 180))).isFalse();

        assertThat(tenantAccountMapper.selectByTenantId(tenantA).getBalance()).isEqualTo(820L);
        assertThat(tenantAccountMapper.selectByTenantId(tenantB).getBalance()).isEqualTo(750L);
        Long sharedCount = apiUsageLogMapper.selectCount(
                new LambdaQueryWrapper<ApiUsageLog>()
                        .in(ApiUsageLog::getTenantId, List.of(tenantA, tenantB))
                        .eq(ApiUsageLog::getRequestId, sharedReqId));
        assertThat(sharedCount).isEqualTo(2L);
    }

    @Test
    void shouldClampRedisAvailableBalanceAtZeroOnOverdraftSettlement() {
        String suffix = shortId();
        String tenantId = "t_sec_clamp_" + suffix;
        insertTenant(tenantId, "sk-sec-clamp-" + suffix, 100L, 100, 100_000, 1);

        String reqId = "req-overdraft-" + suffix;
        createdRequestIds.add(reqId);
        long availAfterFreeze = quotaFreezeLuaService.freezeQuotaOrThrow(tenantId, reqId, 80);
        assertThat(availAfterFreeze).isEqualTo(20L);

        // actualCost=200 大于 frozen(80) + avail(20)=100，Redis 可用额度必须钳制在 0 而非负数 (-100)
        QuotaFreezeLuaService.SettleResult result = quotaFreezeLuaService.settleQuota(tenantId, reqId, 200);
        assertThat(result.isApplied()).isTrue();
        assertThat(result.isOverdraftClamped()).isTrue();
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isEqualTo(0L);

        assertThatThrownBy(() -> quotaFreezeLuaService.freezeQuotaOrThrow(tenantId, "req-after-clamp-" + suffix, 1))
                .isInstanceOf(GatewayException.class)
                .satisfies(ex -> assertThat(((GatewayException) ex).getErrorCode())
                        .isEqualTo(ErrorCode.INSUFFICIENT_QUOTA));
    }

    @Test
    void shouldPreserveConcurrentBalanceOnPartialAdminUpdateAndSynchronizeRechargeUnderInitLock() throws Exception {
        // 1. 验证 AdminTenantController 局部更新状态/RPM 不覆盖并发已扣减的余额
        String suffix = shortId();
        String tenantId = "t_sec_partial_" + suffix;
        String originalKey = "sk-sec-partial-" + suffix;
        insertTenant(tenantId, originalKey, 1000L, 100, 50_000, 1);

        int deducted = tenantAccountMapper.deductBalanceAtomically(tenantId, 300L);
        assertThat(deducted).isEqualTo(1);
        assertThat(tenantAccountMapper.selectByTenantId(tenantId).getBalance()).isEqualTo(700L);

        AdminKeyUpdateRequest partialReq = AdminKeyUpdateRequest.builder()
                .status(0)
                .rpmLimit(250)
                .build();
        mockMvc.perform(post("/admin/tenants/{tenantId}/api-key", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(partialReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.balance").value(700))
                .andExpect(jsonPath("$.data.status").value(0))
                .andExpect(jsonPath("$.data.rpmLimit").value(250))
                .andExpect(jsonPath("$.data.apiKey").doesNotExist())
                .andExpect(jsonPath("$.data.apiKeyMasked").isNotEmpty());

        TenantAccount updated = tenantAccountMapper.selectByTenantId(tenantId);
        assertThat(updated.getBalance()).isEqualTo(700L);
        assertThat(updated.getStatus()).isEqualTo(0);
        assertThat(updated.getRpmLimit()).isEqualTo(250);
        assertThat(updated.getTpmLimit()).isEqualTo(50_000);
        assertThat(updated.getApiKey()).isEqualTo(originalKey);

        // 2. 验证充值与冷启动 freezeQuotaOrThrow 并发抢锁下 DB 与 Redis 总额严格守恒
        String lockTenant = "t_sec_lock_" + suffix;
        insertTenant(lockTenant, "sk-sec-lock-" + suffix, 500L, 500, 500_000, 1);
        stringRedisTemplate.delete(RedisKeys.quotaAvailKey(lockTenant));

        int freezeThreads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(freezeThreads + 1);
        CountDownLatch ready = new CountDownLatch(freezeThreads + 1);
        CountDownLatch startGun = new CountDownLatch(1);
        AtomicInteger successfulFreezes = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        try {
            futures.add(pool.submit(() -> {
                ready.countDown();
                startGun.await(5, TimeUnit.SECONDS);
                AdminRechargeRequest rechargeReq = AdminRechargeRequest.builder()
                        .amount(1000L)
                        .remark("concurrent-init-lock-test")
                        .build();
                mockMvc.perform(post("/admin/tenants/{tenantId}/recharge", lockTenant)
                                .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(rechargeReq)))
                        .andExpect(status().isOk());
                return null;
            }));

            for (int i = 0; i < freezeThreads; i++) {
                String freezeReqId = "req-lock-freeze-" + suffix + "-" + i;
                createdRequestIds.add(freezeReqId);
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    startGun.await(5, TimeUnit.SECONDS);
                    try {
                        quotaFreezeLuaService.freezeQuotaOrThrow(lockTenant, freezeReqId, 50);
                        successfulFreezes.incrementAndGet();
                    } catch (GatewayException ex) {
                        if (ex.getErrorCode() != ErrorCode.INSUFFICIENT_QUOTA) {
                            throw ex;
                        }
                    }
                    return null;
                }));
            }

            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            startGun.countDown();
            for (Future<?> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(tenantAccountMapper.selectByTenantId(lockTenant).getBalance()).isEqualTo(1500L);
        assertThat(successfulFreezes.get()).isEqualTo(freezeThreads);
        assertThat(quotaFreezeLuaService.getAvailableQuota(lockTenant))
                .isEqualTo(1500L - successfulFreezes.get() * 50L);
    }

    @Test
    void shouldNotTripCircuitBreakerOrFailoverOnUpstreamHttp400() throws Exception {
        AtomicInteger primaryHits = new AtomicInteger(0);
        AtomicInteger backupHits = new AtomicInteger(0);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/primary/v1/chat/completions", exchange -> {
            primaryHits.incrementAndGet();
            byte[] errJson = """
                    {"error":{"message":"invalid model parameter from client","type":"invalid_request_error","code":"invalid_request"}}"""
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(400, errJson.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(errJson);
            }
        });
        server.createContext("/backup/v1/chat/completions", exchange -> {
            backupHits.incrementAndGet();
            byte[] okJson = """
                    {"id":"chatcmpl-backup-unused","object":"chat.completion","created":1731500000,"model":"sec-400","choices":[{"index":0,"message":{"role":"assistant","content":"backup"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}"""
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, okJson.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(okJson);
            }
        });
        server.start();

        try {
            String suffix = shortId();
            String model = "sec-400-no-cb-" + suffix;
            String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
            LlmChannelKey primary = insertChannel("sec-400-pri-" + suffix, "DEEPSEEK", model, baseUrl + "/primary", 1);
            insertChannel("sec-400-bak-" + suffix, "OPENAI", model, baseUrl + "/backup", 2);

            String tenantId = "t_sec_400_" + suffix;
            String apiKey = "sk-sec-400-" + suffix;
            insertTenant(tenantId, apiKey, 10_000L, 100, 100_000, 1);

            String nonStreamPayload = objectMapper.writeValueAsString(Map.of(
                    "model", model,
                    "messages", List.of(Map.of("role", "user", "content", "trigger 400")),
                    "stream", false,
                    "max_tokens", 16
            ));

            // 连续触发 4 次非流式 HTTP 400（大于 failure-threshold=3）
            for (int i = 0; i < 4; i++) {
                String reqId = "req-400-non-" + suffix + "-" + i;
                createdRequestIds.add(reqId);
                mockMvc.perform(post("/v1/chat/completions")
                                .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                                .header(HeaderConstants.REQUEST_ID_HEADER, reqId)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(nonStreamPayload))
                        .andExpect(status().isBadRequest())
                        .andExpect(jsonPath("$.error.code").value("invalid_request"));
            }

            // 流式首包前收到 HTTP 400 同样直接报错并回滚预占，不计失败也不 failover
            String streamPayload = objectMapper.writeValueAsString(Map.of(
                    "model", model,
                    "messages", List.of(Map.of("role", "user", "content", "trigger stream 400")),
                    "stream", true,
                    "max_tokens", 16
            ));
            String streamReqId = "req-400-stream-" + suffix;
            createdRequestIds.add(streamReqId);
            mockMvc.perform(post("/v1/chat/completions")
                            .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                            .header(HeaderConstants.REQUEST_ID_HEADER, streamReqId)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(streamPayload))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.error.code").value("invalid_request"));

            assertThat(primaryHits.get()).isEqualTo(5);
            assertThat(backupHits.get()).isZero();
            assertThat(circuitBreaker.getFailureCount(primary.getId())).isZero();
            assertThat(circuitBreaker.isInCooldown(primary.getId())).isFalse();
            assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isEqualTo(10_000L);
        } finally {
            server.stop(0);
        }
    }

    private void insertTenant(
            String tenantId,
            String apiKey,
            long balance,
            int rpmLimit,
            int tpmLimit,
            int status) {
        TenantAccount account = TenantAccount.builder()
                .tenantId(tenantId)
                .tenantName("Security Regression " + tenantId)
                .apiKey(apiKey)
                .balance(balance)
                .frozenTokens(0L)
                .rpmLimit(rpmLimit)
                .tpmLimit(tpmLimit)
                .status(status)
                .version(0)
                .build();
        tenantAccountMapper.insert(account);
        createdTenantIds.add(tenantId);
        createdApiKeys.add(apiKey);
    }

    private LlmChannelKey insertChannel(
            String name,
            String provider,
            String modelName,
            String baseUrl,
            int priority) {
        LlmChannelKey channel = LlmChannelKey.builder()
                .channelName(name)
                .provider(provider)
                .modelName(modelName)
                .baseUrl(baseUrl)
                .secretKey("sk-sec-test-channel")
                .weight(100)
                .priority(priority)
                .status(1)
                .build();
        channelKeyMapper.insert(channel);
        createdChannelIds.add(channel.getId());
        return channel;
    }

    private BillingMessage buildBillingMessage(String tenantId, String requestId, int totalTokens) {
        int prompt = totalTokens / 2;
        int completion = totalTokens - prompt;
        return BillingMessage.builder()
                .requestId(requestId)
                .tenantId(tenantId)
                .modelName("deepseek-chat")
                .provider("DEEPSEEK")
                .channelId(1L)
                .estimatedTokens(totalTokens + 50)
                .promptTokens(prompt)
                .completionTokens(completion)
                .totalTokens(totalTokens)
                .costMs(95)
                .finishReason("stop")
                .estimated(false)
                .requestTime(LocalDateTime.now())
                .build();
    }

    private String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }
}
