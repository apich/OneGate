package com.onegate.benchmark;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.auth.TenantAuthCacheService;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.constant.RedisKeys;
import com.onegate.domain.dto.UsageBenchmarkResult;
import com.onegate.domain.dto.UsageBenchmarkResult.ExplainRow;
import com.onegate.domain.dto.UsageBenchmarkResult.SeedSummary;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import com.onegate.domain.mapper.TenantAccountMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 模块 4 端到端验收与 200 万级真实 MySQL 9.0.1 慢 SQL / 深分页治理基准测试。
 * <p>
 * 保留压测租户 {@code t_bench_2m} 的 200 万条流水在本地 {@code one_gate} 库中供后续演示，
 * 所有其他临时测试租户及其流水在 {@code @AfterEach} 中彻底清理，绝不污染 {@code t_1001}/{@code t_1002}。
 */
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration",
        "onegate.admin-token=onegate-admin-secret"
})
@AutoConfigureMockMvc
class UsageAnalyticsAndBenchmarkTest {

    private static final String BENCH_TENANT_ID = "t_bench_2m";
    private static final String ADMIN_TOKEN = "onegate-admin-secret";
    private static final int TARGET_BENCH_ROWS = 2_000_000;

    @MockitoBean
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ApiUsageLogMapper apiUsageLogMapper;

    @Autowired
    private TenantAccountMapper tenantAccountMapper;

    @Autowired
    private TenantAuthCacheService tenantAuthCacheService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private UsageDataBenchmarkRunner benchmarkRunner;

    @Autowired
    private javax.sql.DataSource dataSource;

    private final List<String> tempTenantIds = new ArrayList<>();
    private final List<String> tempApiKeys = new ArrayList<>();

    @AfterEach
    void cleanupTempTenants() {
        for (String apiKey : tempApiKeys) {
            tenantAuthCacheService.evictLocalCache(apiKey);
            stringRedisTemplate.delete(RedisKeys.authKey(apiKey));
            stringRedisTemplate.delete("onegate:lock:auth:" + apiKey);
        }
        for (String tenantId : tempTenantIds) {
            apiUsageLogMapper.delete(
                    new LambdaQueryWrapper<ApiUsageLog>().eq(ApiUsageLog::getTenantId, tenantId));
            stringRedisTemplate.delete(RedisKeys.rateLimitRpmKey(tenantId));
            stringRedisTemplate.delete(RedisKeys.rateLimitTpmKey(tenantId));
            stringRedisTemplate.delete(RedisKeys.quotaAvailKey(tenantId));
            TenantAccount acc = tenantAccountMapper.selectByTenantId(tenantId);
            if (acc != null) {
                tenantAccountMapper.deleteById(acc.getId());
            }
        }
        tempApiKeys.clear();
        tempTenantIds.clear();
    }

    @Test
    void shouldAuthenticateUsageEndpointsAndPaginateSameMillisClustersWithZeroLossOrDuplicates() throws Exception {
        String suffix = shortId();
        String tenantId = "t_usage_http_" + suffix;
        String apiKey = "sk-usage-http-" + suffix;
        insertTempTenant(tenantId, apiKey);

        // 1. 鉴权守卫校验: /v1/usage/** 缺 Key 返回 401; /admin/benchmark/** 缺 Token 返回 403
        mockMvc.perform(get("/v1/usage/stats"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error.code").value("invalid_api_key"));

        mockMvc.perform(get("/v1/usage/logs"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error.code").value("invalid_api_key"));

        mockMvc.perform(get("/admin/benchmark/compare")
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, "bad-token"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error.code").value("admin_unauthorized"));

        // 参数成对校验: 只传 lastCreateTime 不传 lastId 返回 400
        mockMvc.perform(get("/v1/usage/logs")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                        .param("lastCreateTime", "2026-11-13 12:00:00.000"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("invalid_request"));

        // 2. 插入 25 条记录: 5 条较早 + 15 条完全同毫秒 create_time + 5 条较晚
        LocalDateTime earlier = LocalDateTime.of(2026, 11, 13, 10, 0, 0, 100_000_000);
        LocalDateTime sameMillis = LocalDateTime.of(2026, 11, 13, 12, 0, 0, 555_000_000);
        LocalDateTime later = LocalDateTime.of(2026, 11, 13, 14, 0, 0, 900_000_000);

        List<Long> expectedIdsDesc = new ArrayList<>();
        long expectedTotalTokens = 0L;
        long deepseekTokens = 0L;
        int deepseekCount = 0;

        for (int i = 0; i < 25; i++) {
            LocalDateTime ts = (i < 5) ? earlier.plusSeconds(i)
                    : (i < 20 ? sameMillis : later.plusSeconds(i - 20));
            String model = (i % 2 == 0) ? "deepseek-chat" : "qwen-plus";
            int tokens = 100 + i * 10;
            expectedTotalTokens += tokens;
            if ("deepseek-chat".equals(model)) {
                deepseekTokens += tokens;
                deepseekCount++;
            }

            ApiUsageLog row = ApiUsageLog.builder()
                    .requestId("req-cur-" + suffix + "-" + i)
                    .tenantId(tenantId)
                    .modelName(model)
                    .provider("deepseek-chat".equals(model) ? "DEEPSEEK" : "QWEN")
                    .channelId(1L)
                    .promptTokens(60)
                    .completionTokens(tokens - 60)
                    .totalTokens(tokens)
                    .costMs(120 + i)
                    .finishReason("stop")
                    .estimated(0)
                    .createTime(ts)
                    .build();
            apiUsageLogMapper.insert(row);
            expectedIdsDesc.add(row.getId());
        }
        // 期望顺序: 按 (create_time DESC, id DESC) 排序即插入顺序完全逆序
        List<Long> reversedExpected = new ArrayList<>(expectedIdsDesc.size());
        for (int i = expectedIdsDesc.size() - 1; i >= 0; i--) {
            reversedExpected.add(expectedIdsDesc.get(i));
        }

        // 3. 验证 /v1/usage/stats 覆盖索引聚合准确性
        mockMvc.perform(get("/v1/usage/stats")
                        .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                        .param("modelName", "deepseek-chat"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.tenantId").value(tenantId))
                .andExpect(jsonPath("$.data.modelName").value("deepseek-chat"))
                .andExpect(jsonPath("$.data.requestCount").value(deepseekCount))
                .andExpect(jsonPath("$.data.totalTokens").value(deepseekTokens))
                .andExpect(jsonPath("$.data.queryElapsedMs").isNumber());

        // 4. 以 pageSize=4 (非 15 的约数) 逐页遍历全部 25 条，刻意多次切断同毫秒记录群
        List<Long> collectedIds = new ArrayList<>();
        Set<Long> uniqueIds = new HashSet<>();
        String nextCreateTime = null;
        Long nextId = null;
        int pageCount = 0;

        while (true) {
            var reqBuilder = get("/v1/usage/logs")
                    .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                    .param("pageSize", "4");
            if (nextCreateTime != null && nextId != null) {
                reqBuilder.param("lastCreateTime", nextCreateTime)
                        .param("lastId", String.valueOf(nextId));
            }
            MvcResult res = mockMvc.perform(reqBuilder)
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.code").value(200))
                    .andReturn();

            JsonNode data = objectMapper.readTree(res.getResponse().getContentAsString()).path("data");
            JsonNode records = data.path("records");
            for (JsonNode rec : records) {
                long id = rec.path("id").asLong();
                collectedIds.add(id);
                assertThat(uniqueIds.add(id))
                        .as("Cursor pagination must never return duplicate id=%d", id)
                        .isTrue();
            }
            pageCount++;
            if (!data.path("hasMore").asBoolean()) {
                break;
            }
            nextCreateTime = data.path("nextCreateTime").asText();
            nextId = data.path("nextId").asLong();
            assertThat(nextCreateTime).isNotBlank();
            assertThat(pageCount).isLessThan(20);
        }

        assertThat(collectedIds).hasSize(25);
        assertThat(collectedIds).containsExactlyElementsOf(reversedExpected);
        System.out.printf("[TEST] Same-millisecond cursor traversal: 25 rows across %d pages (pageSize=4), "
                        + "15 identical-ms rows split cleanly with 0 loss & 0 duplicates (totalTokens=%d)%n",
                pageCount, expectedTotalTokens);
    }

    @Tag("benchmark")
    @Test
    void shouldSeedTwoMillionRowsAndBenchmarkExplainWithMassiveSpeedup() throws Exception {
        // 1. 幂等写入 2,000,000 条真实仿真数据至压测租户 t_bench_2m
        SeedSummary seedSummary = benchmarkRunner.seedTwoMillionRowsIfNeeded(BENCH_TENANT_ID, TARGET_BENCH_ROWS);
        assertThat(seedSummary.getTotalRowsAfter()).isGreaterThanOrEqualTo(TARGET_BENCH_ROWS);
        System.out.printf("[BENCHMARK-SEED] tenant=%s, existingBefore=%d, inserted=%d, totalAfter=%d, skipped=%s, elapsedMs=%d%n",
                seedSummary.getTenantId(),
                seedSummary.getExistingRowsBefore(),
                seedSummary.getInsertedRows(),
                seedSummary.getTotalRowsAfter(),
                seedSummary.isSkipped(),
                seedSummary.getElapsedMs());

        // 二次调用验证幂等跳过 + 管理接口触发校验
        MvcResult idempotentSeedRes = mockMvc.perform(post("/admin/benchmark/seed-2m")
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                        .param("tenantId", BENCH_TENANT_ID)
                        .param("targetRows", String.valueOf(TARGET_BENCH_ROWS)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.skipped").value(true))
                .andExpect(jsonPath("$.data.insertedRows").value(0))
                .andReturn();
        assertThat(idempotentSeedRes.getResponse().getStatus()).isEqualTo(200);

        // 2. 执行全租户深分页 (offset=1,000,000, pageSize=20) 与覆盖索引统计真实压测
        UsageBenchmarkResult benchAll = benchmarkRunner.runDeepPaginationAndCoveringBenchmark(
                BENCH_TENANT_ID, null, 1_000_000L, 20);
        printExplainDetails("ALL_MODELS(offset=1,000,000)", benchAll);

        assertThat(benchAll.getTotalRows()).isGreaterThanOrEqualTo(TARGET_BENCH_ROWS);
        assertThat(benchAll.getResultsConsistent()).isTrue();
        assertThat(benchAll.getTraditionalResultCount()).isEqualTo(20);
        assertThat(benchAll.getCursorResultCount()).isEqualTo(20);
        assertThat(benchAll.getCursorSeekMs()).isLessThan(benchAll.getTraditionalOffsetMs());
        assertThat(benchAll.getCursorSeekMs()).isLessThanOrEqualTo(50L);

        // 校验覆盖索引 EXPLAIN 命中 idx_tenant_model_time_tokens 且 Using index、无 Using filesort
        assertThat(benchAll.getExplainCoveringRows()).isNotEmpty();
        ExplainRow coveringPlan = benchAll.getExplainCoveringRows().get(0);
        assertThat(coveringPlan.getKey()).isEqualTo("idx_tenant_model_time_tokens");
        assertThat(coveringPlan.getExtra()).contains("Using index");
        assertThat(coveringPlan.getExtra()).doesNotContain("Using filesort");

        // 校验传统 LIMIT 公平命中 idx_tenant_create_time（消除 Using filesort，纯比拼深偏移扫描成本）
        assertThat(benchAll.getExplainTraditionalRows()).isNotEmpty();
        ExplainRow tradPlan = benchAll.getExplainTraditionalRows().get(0);
        assertThat(tradPlan.getKey()).isEqualTo("idx_tenant_create_time");
        assertThat(tradPlan.getExtra()).doesNotContain("Using filesort");

        // 校验复合游标 EXPLAIN 命中 idx_tenant_create_time 且无 Using filesort
        assertThat(benchAll.getExplainCursorRows()).isNotEmpty();
        ExplainRow cursorPlan = benchAll.getExplainCursorRows().get(0);
        assertThat(cursorPlan.getKey()).isEqualTo("idx_tenant_create_time");
        assertThat(cursorPlan.getExtra()).doesNotContain("Using filesort");

        // 2b. 按模型过滤深分页 (modelName=deepseek-chat, offset=300,000, pageSize=20): 命中 idx_tenant_model_time
        UsageBenchmarkResult benchModel = benchmarkRunner.runDeepPaginationAndCoveringBenchmark(
                BENCH_TENANT_ID, "deepseek-chat", 300_000L, 20);
        printExplainDetails("MODEL=deepseek-chat(offset=300,000)", benchModel);

        assertThat(benchModel.getResultsConsistent()).isTrue();
        assertThat(benchModel.getTraditionalResultCount()).isEqualTo(20);
        assertThat(benchModel.getCursorResultCount()).isEqualTo(20);
        assertThat(benchModel.getCursorSeekMs()).isLessThan(benchModel.getTraditionalOffsetMs());
        assertThat(benchModel.getCursorSeekMs()).isLessThanOrEqualTo(50L);

        assertThat(benchModel.getExplainCoveringRows()).isNotEmpty();
        ExplainRow modelCoveringPlan = benchModel.getExplainCoveringRows().get(0);
        assertThat(modelCoveringPlan.getKey()).isEqualTo("idx_tenant_model_time_tokens");
        assertThat(modelCoveringPlan.getExtra()).contains("Using index");
        assertThat(modelCoveringPlan.getExtra()).doesNotContain("Using filesort");

        assertThat(benchModel.getExplainTraditionalRows()).isNotEmpty();
        ExplainRow modelTradPlan = benchModel.getExplainTraditionalRows().get(0);
        assertThat(modelTradPlan.getKey()).isEqualTo("idx_tenant_model_time");
        assertThat(modelTradPlan.getExtra()).doesNotContain("Using filesort");

        assertThat(benchModel.getExplainCursorRows()).isNotEmpty();
        ExplainRow modelCursorPlan = benchModel.getExplainCursorRows().get(0);
        assertThat(modelCursorPlan.getKey()).isEqualTo("idx_tenant_model_time");
        assertThat(modelCursorPlan.getExtra()).doesNotContain("Using filesort");

        // 3. 在 200 万数据集的百万偏移处连续验证 3 页同毫秒边界游标翻页（与 OFFSET 完全等价）
        List<ApiUsageLog> boundaryBefore = apiUsageLogMapper.selectByTraditionalOffset(
                BENCH_TENANT_ID, null, 999_999L, 1);
        assertThat(boundaryBefore).hasSize(1);
        LocalDateTime cursorTime = boundaryBefore.get(0).getCreateTime();
        Long cursorId = boundaryBefore.get(0).getId();

        List<ApiUsageLog> expected30 = apiUsageLogMapper.selectByTraditionalOffset(
                BENCH_TENANT_ID, null, 1_000_000L, 30);
        List<Long> cursor30Ids = new ArrayList<>(30);
        for (int p = 0; p < 3; p++) {
            List<ApiUsageLog> page = apiUsageLogMapper.selectByCursor(
                    BENCH_TENANT_ID, null, cursorTime, cursorId, 10);
            assertThat(page).hasSize(10);
            for (ApiUsageLog row : page) {
                cursor30Ids.add(row.getId());
            }
            ApiUsageLog tail = page.get(page.size() - 1);
            cursorTime = tail.getCreateTime();
            cursorId = tail.getId();
        }
        List<Long> expected30Ids = expected30.stream().map(ApiUsageLog::getId).toList();
        assertThat(cursor30Ids).containsExactlyElementsOf(expected30Ids);

        // 3b. 实测 3 轮全租户深分页 offset=1,000,000 (Traditional USE INDEX vs CompositeCursor) + 无 modelName 覆盖索引 EXPLAIN
        LocalDateTime boundaryTime = boundaryBefore.get(0).getCreateTime();
        Long boundaryId = boundaryBefore.get(0).getId();
        long[] tradRoundsMs = new long[3];
        long[] cursorRoundsMs = new long[3];
        for (int r = 0; r < 3; r++) {
            long tStart = System.nanoTime();
            List<ApiUsageLog> tradPage = apiUsageLogMapper.selectByTraditionalOffset(
                    BENCH_TENANT_ID, null, 1_000_000L, 20);
            tradRoundsMs[r] = Math.max(1L, (System.nanoTime() - tStart) / 1_000_000L);
            assertThat(tradPage).hasSize(20);

            long cStart = System.nanoTime();
            List<ApiUsageLog> curPage = apiUsageLogMapper.selectByCursor(
                    BENCH_TENANT_ID, null, boundaryTime, boundaryId, 20);
            cursorRoundsMs[r] = Math.max(1L, (System.nanoTime() - cStart) / 1_000_000L);
            assertThat(curPage).hasSize(20);
        }
        long[] sortedTrad = Arrays.copyOf(tradRoundsMs, 3);
        long[] sortedCursor = Arrays.copyOf(cursorRoundsMs, 3);
        Arrays.sort(sortedTrad);
        Arrays.sort(sortedCursor);
        System.out.printf("[BENCHMARK-3ROUNDS][offset=1,000,000, USE INDEX idx_tenant_create_time] traditionalMs=%s (median=%d ms), cursorMs=%s (median=%d ms), speedup=%.2fx%n",
                Arrays.toString(tradRoundsMs), sortedTrad[1],
                Arrays.toString(cursorRoundsMs), sortedCursor[1],
                sortedTrad[1] / (double) Math.max(1L, sortedCursor[1]));

        LocalDateTime statsStart = LocalDateTime.of(2026, 9, 1, 0, 0, 0);
        LocalDateTime statsEnd = LocalDateTime.of(2026, 12, 31, 23, 59, 59);
        long statsAllStart = System.nanoTime();
        var statsAll = apiUsageLogMapper.selectCoveringStats(BENCH_TENANT_ID, null, statsStart, statsEnd);
        long statsAllMs = Math.max(1L, (System.nanoTime() - statsAllStart) / 1_000_000L);
        assertThat(statsAll.getRequestCount()).isEqualTo(TARGET_BENCH_ROWS);
        printNoModelCoveringStatsExplain(BENCH_TENANT_ID, statsStart, statsEnd, statsAllMs,
                statsAll.getRequestCount(), statsAll.getTotalTokens());

        // 4. 管理侧 HTTP 接口 /admin/benchmark/compare 验证 (全模型 + 指定模型)
        mockMvc.perform(get("/admin/benchmark/compare")
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                        .param("tenantId", BENCH_TENANT_ID)
                        .param("offset", "1000000")
                        .param("pageSize", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.resultsConsistent").value(true))
                .andExpect(jsonPath("$.data.cursorResultCount").value(20));

        mockMvc.perform(get("/admin/benchmark/compare")
                        .header(HeaderConstants.ADMIN_TOKEN_HEADER, ADMIN_TOKEN)
                        .param("tenantId", BENCH_TENANT_ID)
                        .param("modelName", "deepseek-chat")
                        .param("offset", "300000")
                        .param("pageSize", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.modelName").value("deepseek-chat"))
                .andExpect(jsonPath("$.data.resultsConsistent").value(true))
                .andExpect(jsonPath("$.data.cursorResultCount").value(20));

        // 5. 验证默认种子租户 t_1001 / t_1002 未被压测污染
        assertThat(apiUsageLogMapper.countByTenant("t_1001")).isZero();
        assertThat(apiUsageLogMapper.countByTenant("t_1002")).isZero();
    }

    private void printNoModelCoveringStatsExplain(
            String tenantId,
            LocalDateTime start,
            LocalDateTime end,
            long elapsedMs,
            long requestCount,
            long totalTokens) throws Exception {
        String explainSql = """
                EXPLAIN SELECT ?, NULL, COUNT(*), COALESCE(SUM(total_tokens), 0)
                FROM api_usage_log USE INDEX (idx_tenant_time_tokens)
                WHERE tenant_id = ? AND create_time >= ? AND create_time <= ?""";
        List<ExplainRow> rows = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(explainSql)) {
            ps.setString(1, tenantId);
            ps.setString(2, tenantId);
            ps.setTimestamp(3, Timestamp.valueOf(start));
            ps.setTimestamp(4, Timestamp.valueOf(end));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(ExplainRow.builder()
                            .id(rs.getObject("id") != null ? rs.getLong("id") : null)
                            .selectType(rs.getString("select_type"))
                            .table(rs.getString("table"))
                            .type(rs.getString("type"))
                            .possibleKeys(rs.getString("possible_keys"))
                            .key(rs.getString("key"))
                            .keyLen(rs.getString("key_len"))
                            .ref(rs.getString("ref"))
                            .rows(rs.getObject("rows") != null ? rs.getLong("rows") : null)
                            .filtered(rs.getObject("filtered") != null ? rs.getDouble("filtered") : null)
                            .extra(rs.getString("Extra"))
                            .build());
                }
            }
        }
        assertThat(rows).isNotEmpty();
        assertThat(rows.get(0).getKey()).isEqualTo("idx_tenant_time_tokens");
        assertThat(rows.get(0).getExtra()).contains("Using index");
        assertThat(rows.get(0).getExtra()).doesNotContain("Using filesort");
        System.out.printf("[BENCHMARK-STATS][ALL_MODELS(no modelName)] elapsed=%d ms, count=%d, totalTokens=%d%n",
                elapsedMs, requestCount, totalTokens);
        printExplainTable("EXPLAIN CoveringStats (ALL_MODELS -> idx_tenant_time_tokens)", rows);
    }

    private void printExplainDetails(String tag, UsageBenchmarkResult result) {
        System.out.println("================================================================================");
        System.out.printf("[BENCHMARK-REPORT][%s] totalRows=%d, offset=%d, pageSize=%d, resultsConsistent=%s%n",
                tag, result.getTotalRows(), result.getOffset(), result.getPageSize(), result.getResultsConsistent());
        System.out.printf("  CoveringStats    : %d ms (count=%d, totalTokens=%d)%n",
                result.getCoveringStatsMs(), result.getCoveringStatsRequestCount(), result.getCoveringStatsTotalTokens());
        System.out.printf("  TraditionalLimit : %d ms%n", result.getTraditionalOffsetMs());
        System.out.printf("  CompositeCursor  : %d ms (speedup=%.2fx)%n",
                result.getCursorSeekMs(), result.getSpeedupRatio());
        printExplainTable("EXPLAIN CoveringStats", result.getExplainCoveringRows());
        printExplainTable("EXPLAIN TraditionalLimit", result.getExplainTraditionalRows());
        printExplainTable("EXPLAIN CompositeCursor", result.getExplainCursorRows());
        System.out.println("================================================================================");
    }

    private void printExplainTable(String title, List<ExplainRow> rows) {
        System.out.println("  -- " + title + " --");
        if (rows == null || rows.isEmpty()) {
            System.out.println("     (empty)");
            return;
        }
        for (ExplainRow r : rows) {
            System.out.printf("     id=%s | select_type=%s | table=%s | type=%s | possible_keys=%s | key=%s | key_len=%s | ref=%s | rows=%s | filtered=%s | Extra=%s%n",
                    r.getId(), r.getSelectType(), r.getTable(), r.getType(),
                    r.getPossibleKeys(), r.getKey(), r.getKeyLen(), r.getRef(),
                    r.getRows(), r.getFiltered(), r.getExtra());
        }
    }

    private void insertTempTenant(String tenantId, String apiKey) {
        TenantAccount account = TenantAccount.builder()
                .tenantId(tenantId)
                .tenantName("Analytics Test " + tenantId)
                .apiKey(apiKey)
                .balance(100_000L)
                .frozenTokens(0L)
                .rpmLimit(1000)
                .tpmLimit(1_000_000)
                .status(1)
                .version(0)
                .build();
        tenantAccountMapper.insert(account);
        tempTenantIds.add(tenantId);
        tempApiKeys.add(apiKey);
    }

    private String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }
}
