package com.onegate.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.constant.RedisKeys;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.common.model.RateLimitResult;
import com.onegate.common.model.RateLimitService;
import com.onegate.domain.dto.AdminKeyUpdateRequest;
import com.onegate.domain.dto.AdminRechargeRequest;
import com.onegate.domain.dto.TenantAuthInfo;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.TenantAccountMapper;
import jakarta.servlet.DispatcherType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 模块 1 真实环境集成测试 (直连本地 MySQL 9.0.1 + Redis 7.0.15, 仅排除 RabbitMQ 自动装配)。
 */
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration",
        "onegate.admin-token=onegate-admin-secret"
})
@AutoConfigureMockMvc
@Import(AuthAndRateLimitIntegrationTest.TestGatewayEndpointConfig.class)
class AuthAndRateLimitIntegrationTest {

    private static final String ADMIN_TOKEN = "onegate-admin-secret";

    @Autowired
    private TenantAuthCacheService tenantAuthCacheService;

    @Autowired
    private RateLimitService rateLimitService;

    @Autowired
    private RateLimiterLuaService rateLimiterLuaService;

    @Autowired
    private ApiKeyAuthInterceptor apiKeyAuthInterceptor;

    @Autowired
    private TenantAccountMapper tenantAccountMapper;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private final List<String> createdTenantIds = new ArrayList<>();
    private final List<String> createdRedisKeys = new ArrayList<>();
    private final List<String> createdApiKeys = new ArrayList<>();

    @AfterEach
    void cleanupTestArtifacts() {
        for (String apiKey : createdApiKeys) {
            tenantAuthCacheService.evictLocalCache(apiKey);
            stringRedisTemplate.delete(RedisKeys.authKey(apiKey));
            stringRedisTemplate.delete("onegate:lock:auth:" + apiKey);
        }
        for (String tenantId : createdTenantIds) {
            stringRedisTemplate.delete(RedisKeys.rateLimitRpmKey(tenantId));
            stringRedisTemplate.delete(RedisKeys.rateLimitTpmKey(tenantId));
            stringRedisTemplate.delete(RedisKeys.quotaAvailKey(tenantId));
            stringRedisTemplate.delete("onegate:lock:quota:init:" + tenantId);
            TenantAccount account = tenantAccountMapper.selectByTenantId(tenantId);
            if (account != null) {
                tenantAccountMapper.deleteById(account.getId());
            }
        }
        for (String redisKey : createdRedisKeys) {
            stringRedisTemplate.delete(redisKey);
        }
        createdApiKeys.clear();
        createdTenantIds.clear();
        createdRedisKeys.clear();
        TenantContext.clear();
    }

    @Test
    void shouldSupportL1CaffeineL2RedisAndNullCacheAntiPenetration() {
        String tenantId = "t_it_cache_" + shortId();
        String apiKey = "sk-it-cache-" + shortId();
        insertTestTenant(tenantId, apiKey, 1000L, 100, 50_000, 1);

        // 确保 L1/L2 全空
        tenantAuthCacheService.evictLocalCache(apiKey);
        stringRedisTemplate.delete(RedisKeys.authKey(apiKey));

        long dbBefore = tenantAuthCacheService.getDbFallbackCount();
        long l1HitsBefore = tenantAuthCacheService.getLocalCacheStats().hitCount();

        // 1. 首次查询: 回源 MySQL 并写入 L2 Redis + L1 Caffeine
        TenantAuthInfo first = tenantAuthCacheService.authenticate(apiKey);
        assertThat(first.isExists()).isTrue();
        assertThat(first.getTenantId()).isEqualTo(tenantId);
        assertThat(tenantAuthCacheService.getDbFallbackCount()).isEqualTo(dbBefore + 1);
        assertThat(tenantAuthCacheService.isPresentInLocalCache(apiKey)).isTrue();
        String l2Json = stringRedisTemplate.opsForValue().get(RedisKeys.authKey(apiKey));
        assertThat(l2Json).isNotBlank().contains(tenantId);

        // 2. 二次查询: 命中 L1 Caffeine, 不回源 DB
        TenantAuthInfo second = tenantAuthCacheService.authenticate(apiKey);
        assertThat(second.getTenantId()).isEqualTo(tenantId);
        assertThat(tenantAuthCacheService.getDbFallbackCount()).isEqualTo(dbBefore + 1);
        assertThat(tenantAuthCacheService.getLocalCacheStats().hitCount()).isGreaterThan(l1HitsBefore);

        // 3. 仅淘汰 L1 Caffeine, 模拟跨节点或本地淘汰后命中 L2 Redis 并回填 L1
        tenantAuthCacheService.evictLocalCache(apiKey);
        assertThat(tenantAuthCacheService.isPresentInLocalCache(apiKey)).isFalse();
        TenantAuthInfo third = tenantAuthCacheService.authenticate(apiKey);
        assertThat(third.getTenantId()).isEqualTo(tenantId);
        assertThat(tenantAuthCacheService.getDbFallbackCount()).isEqualTo(dbBefore + 1);
        assertThat(tenantAuthCacheService.isPresentInLocalCache(apiKey)).isTrue();

        // 4. 空值 __NULL__ 防穿透: 查询不存在的非法 API Key
        String invalidKey = "sk-invalid-penetration-" + shortId();
        createdApiKeys.add(invalidKey);
        long dbBeforeInvalid = tenantAuthCacheService.getDbFallbackCount();

        TenantAuthInfo notFoundFirst = tenantAuthCacheService.authenticate(invalidKey);
        assertThat(notFoundFirst.isExists()).isFalse();
        assertThat(tenantAuthCacheService.getDbFallbackCount()).isEqualTo(dbBeforeInvalid + 1);
        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.authKey(invalidKey)))
                .isEqualTo(TenantAuthCacheService.NULL_CACHE_VALUE);

        // 连续请求 10 次 L1 空值命中 + 清除 L1 后连续请求 5 次 L2 __NULL__ 命中, DB 回源增量均为 0
        for (int i = 0; i < 10; i++) {
            assertThat(tenantAuthCacheService.authenticate(invalidKey).isExists()).isFalse();
        }
        tenantAuthCacheService.evictLocalCache(invalidKey);
        for (int i = 0; i < 5; i++) {
            assertThat(tenantAuthCacheService.authenticate(invalidKey).isExists()).isFalse();
        }
        assertThat(tenantAuthCacheService.getDbFallbackCount()).isEqualTo(dbBeforeInvalid + 1);
    }

    @Test
    void shouldPreventCacheBreakdownWithRedissonLockAndDcl() throws Exception {
        String tenantId = "t_it_lock_" + shortId();
        String apiKey = "sk-it-lock-" + shortId();
        insertTestTenant(tenantId, apiKey, 5000L, 120, 100_000, 1);

        tenantAuthCacheService.evictLocalCache(apiKey);
        stringRedisTemplate.delete(RedisKeys.authKey(apiKey));

        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch readyLatch = new CountDownLatch(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        long dbBefore = tenantAuthCacheService.getDbFallbackCount();

        try {
            List<Future<TenantAuthInfo>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                Callable<TenantAuthInfo> task = () -> {
                    readyLatch.countDown();
                    startGun.await(5, TimeUnit.SECONDS);
                    return tenantAuthCacheService.authenticate(apiKey);
                };
                futures.add(pool.submit(task));
            }
            assertThat(readyLatch.await(5, TimeUnit.SECONDS)).isTrue();
            startGun.countDown();

            for (Future<TenantAuthInfo> future : futures) {
                TenantAuthInfo info = future.get(10, TimeUnit.SECONDS);
                assertThat(info.isExists()).isTrue();
                assertThat(info.getTenantId()).isEqualTo(tenantId);
            }
            // 20 线程并发击穿同一热点 Key, Redisson RLock + DCL 严格仅回源 1 次 MySQL
            assertThat(tenantAuthCacheService.getDbFallbackCount() - dbBefore).isEqualTo(1L);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void shouldInvalidateCachesViaAdminApiAndRedisPubSub() throws Exception {
        String tenantId = "t_it_admin_" + shortId();
        String oldKey = "sk-it-old-" + shortId();
        insertTestTenant(tenantId, oldKey, 1000L, 200, 50_000, 1);

        // 1. 合法 Key 通过 /v1/auth-test/ping 鉴权与拦截器验证
        mockMvc.perform(get("/v1/auth-test/ping")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + oldKey))
                .andExpect(status().isOk())
                .andExpect(header().exists(HeaderConstants.REQUEST_ID_HEADER))
                .andExpect(header().exists(HeaderConstants.AUTH_LATENCY_US_HEADER))
                .andExpect(jsonPath("$.tenantId").value(tenantId));
        assertThat(TenantContext.get()).isNull();
        assertThat(tenantAuthCacheService.isPresentInLocalCache(oldKey)).isTrue();

        // 2. 管理端鉴权失败返回 403
        mockMvc.perform(get("/admin/tenants/{tenantId}", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, "wrong-token"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("admin_unauthorized"));

        // 3. 管理端查询租户信息 (AdminTenantResponse 脱敏，不返回明文 apiKey)
        mockMvc.perform(get("/admin/tenants/{tenantId}", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.balance").value(1000))
                .andExpect(jsonPath("$.data.status").value(1))
                .andExpect(jsonPath("$.data.apiKey").doesNotExist())
                .andExpect(jsonPath("$.data.apiKeyMasked").isNotEmpty());

        // 4. 管理端禁用租户: 删 Redis + Pub/Sub 广播失效本地 Caffeine, 旧 Key 立即被拒绝 (403)
        AdminKeyUpdateRequest disableReq = AdminKeyUpdateRequest.builder().status(0).build();
        mockMvc.perform(post("/admin/tenants/{tenantId}/api-key", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(disableReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value(0));

        assertThat(stringRedisTemplate.opsForValue().get(RedisKeys.authKey(oldKey))).isNull();
        mockMvc.perform(get("/v1/auth-test/ping")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + oldKey))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("tenant_disabled"));

        // 5. 管理端恢复启用并轮换 API Key: 旧 Key 立刻 401, 新 Key 立刻 200
        AdminKeyUpdateRequest rotateReq = AdminKeyUpdateRequest.builder()
                .status(1)
                .rotateApiKey(true)
                .rpmLimit(500)
                .build();
        MvcResult rotateRes = mockMvc.perform(post("/admin/tenants/{tenantId}/api-key", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(rotateReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value(1))
                .andExpect(jsonPath("$.data.rpmLimit").value(500))
                .andExpect(jsonPath("$.data.apiKeyMasked").isNotEmpty())
                .andReturn();

        JsonNode rotateJson = objectMapper.readTree(rotateRes.getResponse().getContentAsString());
        String newKey = rotateJson.path("data").path("apiKey").asText();
        assertThat(newKey).isNotBlank().isNotEqualTo(oldKey);
        createdApiKeys.add(newKey);

        mockMvc.perform(get("/v1/auth-test/ping")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + oldKey))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error.code").value("invalid_api_key"));

        mockMvc.perform(get("/v1/auth-test/ping")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + newKey))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tenantId").value(tenantId));

        // 6. 独立验证 Redis Pub/Sub 广播机制 (模拟其他节点收到 ADMIN 广播并清理本机 L1 Caffeine)
        assertThat(tenantAuthCacheService.isPresentInLocalCache(newKey)).isTrue();
        stringRedisTemplate.convertAndSend(RedisKeys.INVALIDATE_CHANNEL, newKey);
        long deadline = System.currentTimeMillis() + 2000L;
        while (tenantAuthCacheService.isPresentInLocalCache(newKey) && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertThat(tenantAuthCacheService.isPresentInLocalCache(newKey)).isFalse();

        // 7. 管理端充值:
        // 7a. 若 Redis quotaAvailKey 尚不存在, 充值仅增加 MySQL balance, 绝不误建只含充值额的 Redis Key
        String availKey = RedisKeys.quotaAvailKey(tenantId);
        stringRedisTemplate.delete(availKey);
        AdminRechargeRequest firstRecharge = AdminRechargeRequest.builder()
                .amount(500L)
                .remark("integration-test-topup-no-redis-key")
                .build();
        mockMvc.perform(post("/admin/tenants/{tenantId}/recharge", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(firstRecharge)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.balance").value(1500))
                .andExpect(jsonPath("$.data.apiKey").doesNotExist())
                .andExpect(jsonPath("$.data.apiKeyMasked").isNotEmpty());
        assertThat(stringRedisTemplate.hasKey(availKey)).isFalse();

        // 7b. 若 Redis quotaAvailKey 已存在, 同步原子 INCRBY 保持库与缓存一致
        stringRedisTemplate.opsForValue().set(availKey, "1500");
        AdminRechargeRequest secondRecharge = AdminRechargeRequest.builder()
                .amount(5000L)
                .remark("integration-test-topup")
                .build();
        mockMvc.perform(post("/admin/tenants/{tenantId}/recharge", tenantId)
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(secondRecharge)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.balance").value(6500))
                .andExpect(jsonPath("$.data.apiKey").doesNotExist())
                .andExpect(jsonPath("$.data.apiKeyMasked").isNotEmpty());

        assertThat(stringRedisTemplate.opsForValue().get(availKey)).isEqualTo("6500");
    }

    @Test
    void shouldEnforceDualRateLimitAtomicallyWithoutCrossPollution() throws Exception {
        // Case A: RPM 超限拦截绝不扣减 TPM 桶令牌
        String tenantRpm = "t_it_rpm_" + shortId();
        createdTenantIds.add(tenantRpm);
        TenantAuthInfo rpmTenant = TenantAuthInfo.builder()
                .exists(true)
                .tenantId(tenantRpm)
                .rpmLimit(3)
                .tpmLimit(1000)
                .status(1)
                .build();

        for (int i = 1; i <= 3; i++) {
            RateLimitResult res = rateLimitService.acquireOrThrow(rpmTenant, "req-rpm-" + i, 100);
            assertThat(res.isAllowed()).isTrue();
            assertThat(res.getCurrentRpm()).isEqualTo(i);
        }

        // 第 4 次请求预估消耗 200 tokens, 应被 RPM 拒绝, 且 TPM 不得从 ~700 被扣减成 ~500
        assertThatThrownBy(() -> rateLimitService.acquireOrThrow(rpmTenant, "req-rpm-4", 200))
                .isInstanceOf(GatewayException.class)
                .hasMessageContaining("RPM")
                .satisfies(ex -> assertThat(((GatewayException) ex).getErrorCode())
                        .isEqualTo(ErrorCode.RATE_LIMIT_EXCEEDED));

        Object storedTokensObj = stringRedisTemplate.opsForHash()
                .get(RedisKeys.rateLimitTpmKey(tenantRpm), "tokens");
        assertThat(storedTokensObj).isNotNull();
        double storedTokens = Double.parseDouble(storedTokensObj.toString());
        assertThat(storedTokens).isBetween(699.0, 705.0);

        // Case B: TPM 超限拦截绝不写入 RPM ZSet
        String tenantTpm = "t_it_tpm_" + shortId();
        createdTenantIds.add(tenantTpm);
        TenantAuthInfo tpmTenant = TenantAuthInfo.builder()
                .exists(true)
                .tenantId(tenantTpm)
                .rpmLimit(10)
                .tpmLimit(500)
                .status(1)
                .build();

        RateLimitResult firstTpm = rateLimitService.acquireOrThrow(tpmTenant, "req-tpm-1", 400);
        assertThat(firstTpm.isAllowed()).isTrue();
        assertThat(firstTpm.getCurrentRpm()).isEqualTo(1L);
        assertThat(firstTpm.getRemainingTpm()).isEqualTo(100L);

        // 第 2 次请求需要 200 tokens (> ~100), 应被 TPM 拦截且 ZCARD 必须保持为 1
        assertThatThrownBy(() -> rateLimitService.acquireOrThrow(tpmTenant, "req-tpm-2", 200))
                .isInstanceOf(GatewayException.class)
                .hasMessageContaining("TPM");

        Long rpmCardAfterReject = stringRedisTemplate.opsForZSet().zCard(RedisKeys.rateLimitRpmKey(tenantTpm));
        assertThat(rpmCardAfterReject).isEqualTo(1L);

        // 第 3 次小请求 (50 tokens) 应成功放行, currentRpm 变为 2
        RateLimitResult smallOk = rateLimitService.acquireOrThrow(tpmTenant, "req-tpm-3", 50);
        assertThat(smallOk.isAllowed()).isTrue();
        assertThat(smallOk.getCurrentRpm()).isEqualTo(2L);

        // Case C: TPM 毫秒级平滑回充 (6000 TPM = 0.1 token/ms, 休眠 600ms 应回充约 60 tokens)
        String tenantRefill = "t_it_refill_" + shortId();
        createdTenantIds.add(tenantRefill);
        TenantAuthInfo refillTenant = TenantAuthInfo.builder()
                .exists(true)
                .tenantId(tenantRefill)
                .rpmLimit(100)
                .tpmLimit(6000)
                .status(1)
                .build();

        rateLimitService.acquireOrThrow(refillTenant, "req-refill-drain", 6000);
        RateLimitResult immediateDenied = rateLimiterLuaService.tryAcquire(refillTenant, "req-refill-deny", 50);
        assertThat(immediateDenied.isAllowed()).isFalse();
        assertThat(immediateDenied.getRejectedBy()).isEqualTo("TPM");

        Thread.sleep(600L);
        RateLimitResult refilledOk = rateLimitService.acquireOrThrow(refillTenant, "req-refill-ok", 40);
        assertThat(refilledOk.isAllowed()).isTrue();
    }

    @Test
    void shouldAchieveSub3msP99LatencyUnder1000AuthAndRateLimitRequests() throws Exception {
        String tenantId = "t_it_perf_" + shortId();
        String apiKey = "sk-it-perf-" + shortId();
        insertTestTenant(tenantId, apiKey, 10_000_000L, 10_000, 100_000_000, 1);

        // 预热 JVM JIT / Lettuce 连接池 / Lua EVALSHA / L1 Caffeine
        for (int i = 0; i < 50; i++) {
            TenantAuthInfo auth = tenantAuthCacheService.authenticate(apiKey);
            rateLimitService.acquireOrThrow(auth, "req-warmup-" + i, 100);
        }

        // 1. 顺序 1000 次全链路鉴权 + 双指标原子限流压测
        int totalRequests = 1000;
        long[] seqLatenciesUs = new long[totalRequests];
        for (int i = 0; i < totalRequests; i++) {
            long startNanos = System.nanoTime();
            TenantAuthInfo auth = tenantAuthCacheService.authenticate(apiKey);
            rateLimitService.acquireOrThrow(auth, "req-perf-" + i, 100);
            seqLatenciesUs[i] = (System.nanoTime() - startNanos) / 1_000L;
        }
        long seqP99Us = printAndVerifyPerfStats("Sequential-1000", seqLatenciesUs);
        assertThat(seqP99Us).isLessThan(3000L);

        // 2. 20 线程并发 1000 次鉴权 + 原子限流压测, 同时验证同毫秒唯一 requestId 绝不丢失 ZSet 计数
        String concTenantId = "t_it_perf_conc_" + shortId();
        String concApiKey = "sk-it-perf-conc-" + shortId();
        insertTestTenant(concTenantId, concApiKey, 10_000_000L, 10_000, 100_000_000, 1);
        tenantAuthCacheService.authenticate(concApiKey); // 预热 L1 缓存

        int threads = 20;
        int perThread = 50;
        int concTotal = threads * perThread;
        long[] concLatenciesUs = new long[concTotal];
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        long concWallStartNanos;
        long concWallEndNanos;
        try {
            // 先预热 20 个工作线程, 避免把线程冷启动调度算作网关限流延迟
            List<Future<Void>> warmupFutures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int wIdx = t;
                warmupFutures.add(pool.submit(() -> {
                    TenantAuthInfo auth = tenantAuthCacheService.authenticate(concApiKey);
                    rateLimitService.acquireOrThrow(auth, "req-conc-warm-" + wIdx, 10);
                    return null;
                }));
            }
            for (Future<Void> wf : warmupFutures) {
                wf.get(5, TimeUnit.SECONDS);
            }

            // 清理预热在 ZSet 中的 20 条记录, 以便精确验证 1000 次并发后 ZCARD == 1000
            stringRedisTemplate.delete(RedisKeys.rateLimitRpmKey(concTenantId));
            stringRedisTemplate.delete(RedisKeys.rateLimitTpmKey(concTenantId));

            List<Future<Void>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int threadIdx = t;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    startGun.await(5, TimeUnit.SECONDS);
                    for (int j = 0; j < perThread; j++) {
                        int idx = threadIdx * perThread + j;
                        long s = System.nanoTime();
                        TenantAuthInfo auth = tenantAuthCacheService.authenticate(concApiKey);
                        rateLimitService.acquireOrThrow(auth, "req-conc-" + idx, 100);
                        concLatenciesUs[idx] = (System.nanoTime() - s) / 1_000L;
                    }
                    return null;
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            concWallStartNanos = System.nanoTime();
            startGun.countDown();
            for (Future<Void> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }
            concWallEndNanos = System.nanoTime();
        } finally {
            pool.shutdownNow();
        }

        long concP99Us = printAndVerifyPerfStats("Concurrent-20x50(1000)", concLatenciesUs);
        double wallMs = (concWallEndNanos - concWallStartNanos) / 1_000_000.0;
        double qps = concTotal * 1000.0 / wallMs;
        System.out.printf("[PERF][Concurrent-20x50(1000)] wallTime=%.2f ms, throughput=%.1f req/s, P99=%.3f ms%n",
                wallMs, qps, concP99Us / 1000.0);

        // 验证并发同毫秒下 ZSet 成员无覆盖丢失: ZCARD 必须严格等于 1000
        Long zCard = stringRedisTemplate.opsForZSet().zCard(RedisKeys.rateLimitRpmKey(concTenantId));
        assertThat(zCard).isEqualTo((long) concTotal);
        assertThat(qps).isGreaterThan(1000.0);
    }

    private long printAndVerifyPerfStats(String label, long[] latenciesUs) {
        long[] sorted = Arrays.copyOf(latenciesUs, latenciesUs.length);
        Arrays.sort(sorted);
        int n = sorted.length;
        long minUs = sorted[0];
        long p50Us = sorted[(int) (n * 0.50)];
        long p95Us = sorted[(int) (n * 0.95)];
        long p99Us = sorted[(int) (n * 0.99)];
        long maxUs = sorted[n - 1];
        double avgUs = Arrays.stream(sorted).average().orElse(0.0);

        System.out.printf(
                "[PERF][%s] %d requests -> min=%d us, avg=%.1f us, P50=%d us, P95=%d us, P99=%d us (%.3f ms), max=%d us%n",
                label, n, minUs, avgUs, p50Us, p95Us, p99Us, p99Us / 1000.0, maxUs
        );
        return p99Us;
    }

    @Test
    void shouldBypassNonRequestDispatcherAndRejectInvalidAuthHeaders() throws Exception {
        // 1. 缺失 Authorization Header -> 401
        mockMvc.perform(get("/v1/auth-test/ping"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error.code").value("invalid_api_key"));

        // 2. 错误前缀 -> 401
        mockMvc.perform(get("/v1/auth-test/ping")
                        .header(HeaderConstants.AUTHORIZATION, "Basic dXNlcjpwYXNz"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error.code").value("invalid_api_key"));

        // 3. ASYNC 异步派发阶段直接放行, 防止 SSE 异步派发重入
        MockHttpServletRequest asyncReq = new MockHttpServletRequest("GET", "/v1/auth-test/ping");
        asyncReq.setDispatcherType(DispatcherType.ASYNC);
        MockHttpServletResponse asyncResp = new MockHttpServletResponse();
        boolean allowed = apiKeyAuthInterceptor.preHandle(asyncReq, asyncResp, new Object());
        assertThat(allowed).isTrue();
    }

    private void insertTestTenant(String tenantId, String apiKey, long balance, int rpmLimit, int tpmLimit, int status) {
        TenantAccount account = TenantAccount.builder()
                .tenantId(tenantId)
                .tenantName("Integration Test " + tenantId)
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

    private String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    /**
     * 仅在当前测试上下文挂载的轻量 /v1 测试接口，用于端到端验证 WebMvcConfig + ApiKeyAuthInterceptor。
     */
    @TestConfiguration
    static class TestGatewayEndpointConfig {

        @RestController
        static class TestPingController {

            @GetMapping("/v1/auth-test/ping")
            public Map<String, String> ping() {
                TenantAuthInfo current = TenantContext.get();
                return Collections.singletonMap("tenantId", current != null ? current.getTenantId() : "null");
            }
        }
    }
}
