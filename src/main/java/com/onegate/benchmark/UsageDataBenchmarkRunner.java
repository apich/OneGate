package com.onegate.benchmark;

import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.dto.UsageBenchmarkResult;
import com.onegate.domain.dto.UsageBenchmarkResult.ExplainRow;
import com.onegate.domain.dto.UsageBenchmarkResult.SeedSummary;
import com.onegate.domain.dto.UsageStatsResponse;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 模块 4：200 万级 {@code api_usage_log} 压测数据生成器与慢 SQL / 深分页治理基准验证器。
 * <p>
 * 1. 基于原生 JDBC {@code PreparedStatement} 批处理 ({@code rewriteBatchedStatements=true}、5000/batch、25000/commit)
 *    幂等写入 200 万条仿真调用流水，并刻意制造 7 条一组的同毫秒 {@code create_time} 记录群验证游标不漏读、不重复；<br>
 * 2. 获取真实 MySQL {@code EXPLAIN} 执行计划并预热对比覆盖索引聚合、传统 {@code LIMIT offset, size} 与复合游标分页耗时。
 */
@Slf4j
@Component
public class UsageDataBenchmarkRunner {

    private static final int BATCH_SIZE = 5_000;
    private static final int COMMIT_INTERVAL = 25_000;
    private static final int SAME_MILLIS_CLUSTER_SIZE = 7;
    private static final long CLUSTER_STEP_MS = 9_000L;
    private static final int BENCHMARK_ROUNDS = 3;
    private static final int MAX_SEED_ROWS = 5_000_000;

    private static final String[] MODELS = {"deepseek-chat", "qwen-plus", "gpt-4o-mini"};
    private static final String[] PROVIDERS = {"DEEPSEEK", "QWEN", "OPENAI"};
    private static final long[] CHANNEL_IDS = {1L, 3L, 5L};
    private static final String[] FINISH_REASONS = {"stop", "stop", "stop", "length", "stop"};

    private static final String INSERT_SQL = """
            INSERT IGNORE INTO api_usage_log
                (request_id, tenant_id, model_name, provider, channel_id,
                 prompt_tokens, completion_tokens, total_tokens, cost_ms, finish_reason, estimated, create_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final DataSource dataSource;
    private final ApiUsageLogMapper apiUsageLogMapper;
    private final AtomicBoolean seedingInProgress = new AtomicBoolean(false);

    public UsageDataBenchmarkRunner(DataSource dataSource, ApiUsageLogMapper apiUsageLogMapper) {
        this.dataSource = dataSource;
        this.apiUsageLogMapper = apiUsageLogMapper;
    }

    /**
     * 幂等写入目标数量的压测流水数据（若该租户已达目标行数则直接跳过，绝不污染默认演示租户数据）。
     */
    public SeedSummary seedTwoMillionRowsIfNeeded(String tenantId, int targetTotalRows) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "tenantId must not be blank.");
        }
        if (targetTotalRows <= 0 || targetTotalRows > MAX_SEED_ROWS) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "targetTotalRows must be between 1 and " + MAX_SEED_ROWS + ".");
        }
        if (!seedingInProgress.compareAndSet(false, true)) {
            throw new GatewayException(ErrorCode.SERVICE_UNAVAILABLE,
                    "Benchmark seeding is already in progress.");
        }

        try {
            String normalizedTenant = tenantId.trim();
            long existing = apiUsageLogMapper.countByTenant(normalizedTenant);
            if (existing >= targetTotalRows) {
                log.info("[SEED] Tenant '{}' already has {} rows (target={}), skipping batch seed.",
                        normalizedTenant, existing, targetTotalRows);
                return SeedSummary.builder()
                        .tenantId(normalizedTenant)
                        .targetTotalRows(targetTotalRows)
                        .existingRowsBefore(existing)
                        .insertedRows(0L)
                        .totalRowsAfter(existing)
                        .skipped(true)
                        .elapsedMs(0L)
                        .build();
            }

            long rowsToInsert = targetTotalRows - existing;
            String batchTag = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
            // 固定基准时间向前分布约 30 天，保证同毫秒群组在 200 万数据中均匀出现
            long baseTimeMs = LocalDateTime.of(2026, 10, 1, 0, 0, 0, 0)
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();

            long startNanos = System.nanoTime();
            log.info("[SEED] Starting JDBC batch insert for tenant='{}': existing={}, toInsert={}, target={}",
                    normalizedTenant, existing, rowsToInsert, targetTotalRows);

            try (Connection conn = dataSource.getConnection()) {
                boolean originalAutoCommit = conn.getAutoCommit();
                conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                    int inBatch = 0;
                    int sinceCommit = 0;

                    for (long i = 0; i < rowsToInsert; i++) {
                        long globalSeq = existing + i;
                        int modelIdx = (int) (globalSeq % MODELS.length);
                        int promptTokens = 80 + (int) ((globalSeq * 17L) % 420);
                        int completionTokens = 40 + (int) ((globalSeq * 31L) % 560);
                        int totalTokens = promptTokens + completionTokens;
                        int costMs = 120 + (int) ((globalSeq * 13L) % 1800);
                        String finishReason = FINISH_REASONS[(int) (globalSeq % FINISH_REASONS.length)];

                        // 每 7 条共享完全相同的毫秒级时间戳，用以验证复合游标 (create_time, id) 无漏读无重复
                        long clusterIndex = globalSeq / SAME_MILLIS_CLUSTER_SIZE;
                        long tsMs = baseTimeMs + clusterIndex * CLUSTER_STEP_MS;

                        ps.setString(1, "b-" + batchTag + "-" + globalSeq);
                        ps.setString(2, normalizedTenant);
                        ps.setString(3, MODELS[modelIdx]);
                        ps.setString(4, PROVIDERS[modelIdx]);
                        ps.setLong(5, CHANNEL_IDS[modelIdx]);
                        ps.setInt(6, promptTokens);
                        ps.setInt(7, completionTokens);
                        ps.setInt(8, totalTokens);
                        ps.setInt(9, costMs);
                        ps.setString(10, finishReason);
                        ps.setInt(11, (globalSeq % 10 == 0) ? 1 : 0);
                        ps.setTimestamp(12, new Timestamp(tsMs));

                        ps.addBatch();
                        inBatch++;
                        sinceCommit++;

                        if (inBatch >= BATCH_SIZE) {
                            ps.executeBatch();
                            ps.clearBatch();
                            inBatch = 0;
                        }
                        if (sinceCommit >= COMMIT_INTERVAL) {
                            conn.commit();
                            sinceCommit = 0;
                            if ((i + 1) % 250_000 == 0) {
                                long elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000L;
                                log.info("[SEED] Progress tenant='{}': {}/{} rows committed ({}s elapsed)",
                                        normalizedTenant, i + 1, rowsToInsert, elapsedSec);
                            }
                        }
                    }

                    if (inBatch > 0) {
                        ps.executeBatch();
                        ps.clearBatch();
                    }
                    if (sinceCommit > 0) {
                        conn.commit();
                    }
                } catch (Exception ex) {
                    try {
                        conn.rollback();
                    } catch (SQLException rollbackEx) {
                        ex.addSuppressed(rollbackEx);
                    }
                    throw ex;
                } finally {
                    try {
                        conn.setAutoCommit(originalAutoCommit);
                    } catch (SQLException ignored) {
                        // ignore cleanup failure on broken connection
                    }
                }

                // 刷新 InnoDB 索引基数统计，确保后续 EXPLAIN 选取最准确执行计划
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ANALYZE TABLE api_usage_log");
                }
            } catch (SQLException ex) {
                throw new GatewayException(ErrorCode.INTERNAL_ERROR,
                        "Failed to batch seed api_usage_log rows for tenant " + normalizedTenant, ex);
            }

            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            long totalAfter = apiUsageLogMapper.countByTenant(normalizedTenant);
            log.info("[SEED] Completed tenant='{}': inserted={}, totalAfter={}, elapsedMs={}",
                    normalizedTenant, totalAfter - existing, totalAfter, elapsedMs);

            return SeedSummary.builder()
                    .tenantId(normalizedTenant)
                    .targetTotalRows(targetTotalRows)
                    .existingRowsBefore(existing)
                    .insertedRows(Math.max(0L, totalAfter - existing))
                    .totalRowsAfter(totalAfter)
                    .skipped(false)
                    .elapsedMs(elapsedMs)
                    .build();
        } finally {
            seedingInProgress.set(false);
        }
    }

    /**
     * 执行覆盖索引聚合、传统深分页与复合游标深分页的真实 EXPLAIN 解析与预热对比压测。
     */
    public UsageBenchmarkResult runDeepPaginationAndCoveringBenchmark(
            String tenantId,
            String modelName,
            long offset,
            int pageSize) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "tenantId must not be blank.");
        }
        if (offset < 0) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "offset must be >= 0.");
        }
        if (pageSize <= 0 || pageSize > 200) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "pageSize must be between 1 and 200.");
        }

        String normalizedTenant = tenantId.trim();
        String normalizedModel = (modelName != null && !modelName.isBlank()) ? modelName.trim() : null;
        String statsModel = normalizedModel != null ? normalizedModel : MODELS[0];
        long totalRows = apiUsageLogMapper.countByTenant(normalizedTenant);

        // 定位 offset 之前的最后一条边界游标，确保复合游标与 LIMIT offset 返回完全一致的目标页数据
        LocalDateTime cursorCreateTime = null;
        Long cursorId = null;
        if (offset > 0 && totalRows > offset) {
            CursorPoint boundary = findBoundaryCursor(normalizedTenant, normalizedModel, offset);
            if (boundary != null) {
                cursorCreateTime = boundary.createTime;
                cursorId = boundary.id;
            }
        }

        // 1. 获取三条 SQL 的真实 MySQL EXPLAIN 输出
        LocalDateTime statsStart = LocalDateTime.of(2026, 9, 1, 0, 0, 0);
        LocalDateTime statsEnd = LocalDateTime.of(2026, 12, 31, 23, 59, 59);
        List<ExplainRow> explainCovering = explainCoveringStats(normalizedTenant, statsModel, statsStart, statsEnd);
        List<ExplainRow> explainTraditional = explainTraditionalOffset(normalizedTenant, normalizedModel, offset, pageSize);
        List<ExplainRow> explainCursor = explainCursorSeek(
                normalizedTenant, normalizedModel, cursorCreateTime, cursorId, pageSize);

        // 2. 预热 InnoDB Buffer Pool 与 MyBatis / Hikari 链路
        apiUsageLogMapper.selectCoveringStats(normalizedTenant, statsModel, statsStart, statsEnd);
        apiUsageLogMapper.selectByTraditionalOffset(normalizedTenant, normalizedModel, offset, pageSize);
        apiUsageLogMapper.selectByCursor(normalizedTenant, normalizedModel, cursorCreateTime, cursorId, pageSize);

        // 3. 实测对比：快查询 (coveringStats / cursorSeek) 多轮取中位数，慢查询 (traditionalOffset) 预热后实测
        long[] coveringSamplesNanos = new long[BENCHMARK_ROUNDS];
        long[] cursorSamplesNanos = new long[BENCHMARK_ROUNDS];
        int traditionalRounds = (offset >= 100_000L) ? 1 : BENCHMARK_ROUNDS;
        long[] traditionalSamplesNanos = new long[traditionalRounds];

        UsageStatsResponse lastStats = null;
        List<ApiUsageLog> lastTraditional = Collections.emptyList();
        List<ApiUsageLog> lastCursor = Collections.emptyList();

        for (int round = 0; round < BENCHMARK_ROUNDS; round++) {
            long s1 = System.nanoTime();
            lastStats = apiUsageLogMapper.selectCoveringStats(normalizedTenant, statsModel, statsStart, statsEnd);
            coveringSamplesNanos[round] = System.nanoTime() - s1;

            if (round < traditionalRounds) {
                long s2 = System.nanoTime();
                lastTraditional = apiUsageLogMapper.selectByTraditionalOffset(
                        normalizedTenant, normalizedModel, offset, pageSize);
                traditionalSamplesNanos[round] = System.nanoTime() - s2;
            }

            long s3 = System.nanoTime();
            lastCursor = apiUsageLogMapper.selectByCursor(
                    normalizedTenant, normalizedModel, cursorCreateTime, cursorId, pageSize);
            cursorSamplesNanos[round] = System.nanoTime() - s3;
        }

        long coveringMedianNanos = median(coveringSamplesNanos);
        long traditionalMedianNanos = median(traditionalSamplesNanos);
        long cursorMedianNanos = median(cursorSamplesNanos);

        long coveringMs = nanosToRoundedMs(coveringMedianNanos);
        long traditionalMs = nanosToRoundedMs(traditionalMedianNanos);
        long cursorMs = nanosToRoundedMs(cursorMedianNanos);

        double speedupRatio = traditionalMedianNanos / (double) Math.max(1L, cursorMedianNanos);
        boolean resultsConsistent = extractIds(lastTraditional).equals(extractIds(lastCursor));

        String coveringExplainSummary = formatExplainSummary(explainCovering);
        String traditionalExplainSummary = formatExplainSummary(explainTraditional);
        String cursorExplainSummary = formatExplainSummary(explainCursor);

        log.info("""
                        [BENCHMARK][tenant={}, model={}, offset={}, pageSize={}, totalRows={}]
                          CoveringStats   : median={} ms ({} us), count={}, totalTokens={}, EXPLAIN=[{}]
                          TraditionalLimit: median={} ms ({} us), rows={}, EXPLAIN=[{}]
                          CompositeCursor : median={} ms ({} us), rows={}, resultsMatch={}, EXPLAIN=[{}]
                          SpeedupRatio    : {}x""",
                normalizedTenant, normalizedModel == null ? "ALL" : normalizedModel, offset, pageSize, totalRows,
                coveringMs, coveringMedianNanos / 1_000L,
                lastStats != null ? lastStats.getRequestCount() : 0L,
                lastStats != null ? lastStats.getTotalTokens() : 0L,
                coveringExplainSummary,
                traditionalMs, traditionalMedianNanos / 1_000L, lastTraditional.size(), traditionalExplainSummary,
                cursorMs, cursorMedianNanos / 1_000L, lastCursor.size(), resultsConsistent, cursorExplainSummary,
                String.format("%.2f", speedupRatio));

        return UsageBenchmarkResult.builder()
                .tenantId(normalizedTenant)
                .modelName(normalizedModel)
                .offset(offset)
                .pageSize(pageSize)
                .totalRows(totalRows)
                .coveringStatsMs(coveringMs)
                .coveringStatsRequestCount(lastStats != null ? lastStats.getRequestCount() : 0L)
                .coveringStatsTotalTokens(lastStats != null ? lastStats.getTotalTokens() : 0L)
                .traditionalOffsetMs(traditionalMs)
                .cursorSeekMs(cursorMs)
                .speedupRatio(Math.round(speedupRatio * 100.0) / 100.0)
                .traditionalResultCount(lastTraditional.size())
                .cursorResultCount(lastCursor.size())
                .resultsConsistent(resultsConsistent)
                .explainCoveringStats(coveringExplainSummary)
                .explainTraditionalOffset(traditionalExplainSummary)
                .explainCursorSeek(cursorExplainSummary)
                .explainCoveringRows(explainCovering)
                .explainTraditionalRows(explainTraditional)
                .explainCursorRows(explainCursor)
                .build();
    }

    private CursorPoint findBoundaryCursor(String tenantId, String modelName, long offset) {
        boolean hasModel = modelName != null && !modelName.isBlank();
        String indexName = hasModel ? "idx_tenant_model_time" : "idx_tenant_create_time";
        StringBuilder sql = new StringBuilder("SELECT create_time, id FROM api_usage_log USE INDEX (")
                .append(indexName)
                .append(") WHERE tenant_id = ?");
        List<Object> params = new ArrayList<>();
        params.add(tenantId);
        if (hasModel) {
            sql.append(" AND model_name = ?");
            params.add(modelName);
        }
        sql.append(" ORDER BY create_time DESC, id DESC LIMIT ?, 1");
        params.add(offset - 1);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp("create_time");
                    long id = rs.getLong("id");
                    return new CursorPoint(ts != null ? ts.toLocalDateTime() : null, id);
                }
                return null;
            }
        } catch (SQLException ex) {
            throw new GatewayException(ErrorCode.INTERNAL_ERROR, "Failed to resolve cursor boundary.", ex);
        }
    }

    private List<ExplainRow> explainCoveringStats(
            String tenantId, String modelName, LocalDateTime startTime, LocalDateTime endTime) {
        boolean hasModel = modelName != null && !modelName.isBlank();
        String indexName = hasModel ? "idx_tenant_model_time_tokens" : "idx_tenant_time_tokens";
        StringBuilder sql = new StringBuilder("EXPLAIN SELECT ?, ")
                .append(hasModel ? "?" : "NULL")
                .append(", COUNT(*), COALESCE(SUM(total_tokens), 0) FROM api_usage_log USE INDEX (")
                .append(indexName)
                .append(") WHERE tenant_id = ?");
        List<Object> params = new ArrayList<>();
        params.add(tenantId);
        if (hasModel) {
            params.add(modelName);
        }
        params.add(tenantId);
        if (hasModel) {
            sql.append(" AND model_name = ?");
            params.add(modelName);
        }
        if (startTime != null) {
            sql.append(" AND create_time >= ?");
            params.add(Timestamp.valueOf(startTime));
        }
        if (endTime != null) {
            sql.append(" AND create_time <= ?");
            params.add(Timestamp.valueOf(endTime));
        }
        return runExplain(sql.toString(), params);
    }

    private List<ExplainRow> explainTraditionalOffset(
            String tenantId, String modelName, long offset, int pageSize) {
        boolean hasModel = modelName != null && !modelName.isBlank();
        String indexName = hasModel ? "idx_tenant_model_time" : "idx_tenant_create_time";
        StringBuilder sql = new StringBuilder("""
                EXPLAIN SELECT id, request_id, tenant_id, model_name, provider, channel_id,
                       prompt_tokens, completion_tokens, total_tokens, cost_ms, finish_reason, estimated, create_time
                FROM api_usage_log
                USE INDEX (""")
                .append(indexName)
                .append(") WHERE tenant_id = ?");
        List<Object> params = new ArrayList<>();
        params.add(tenantId);
        if (hasModel) {
            sql.append(" AND model_name = ?");
            params.add(modelName);
        }
        sql.append(" ORDER BY create_time DESC, id DESC LIMIT ?, ?");
        params.add(offset);
        params.add(pageSize);
        return runExplain(sql.toString(), params);
    }

    private List<ExplainRow> explainCursorSeek(
            String tenantId, String modelName, LocalDateTime lastCreateTime, Long lastId, int pageSize) {
        boolean hasModel = modelName != null && !modelName.isBlank();
        String indexName = hasModel ? "idx_tenant_model_time" : "idx_tenant_create_time";
        StringBuilder sql = new StringBuilder("""
                EXPLAIN SELECT id, request_id, tenant_id, model_name, provider, channel_id,
                       prompt_tokens, completion_tokens, total_tokens, cost_ms, finish_reason, estimated, create_time
                FROM api_usage_log
                USE INDEX (""")
                .append(indexName)
                .append(") WHERE tenant_id = ?");
        List<Object> params = new ArrayList<>();
        params.add(tenantId);
        if (hasModel) {
            sql.append(" AND model_name = ?");
            params.add(modelName);
        }
        if (lastCreateTime != null && lastId != null) {
            Timestamp ts = Timestamp.valueOf(lastCreateTime);
            sql.append(" AND create_time <= ? AND (create_time, id) < (?, ?)");
            params.add(ts);
            params.add(ts);
            params.add(lastId);
        }
        sql.append(" ORDER BY create_time DESC, id DESC LIMIT ?");
        params.add(pageSize);
        return runExplain(sql.toString(), params);
    }

    private List<ExplainRow> runExplain(String sql, List<Object> params) {
        List<ExplainRow> rows = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(ExplainRow.builder()
                            .id(getNullableLong(rs, "id"))
                            .selectType(rs.getString("select_type"))
                            .table(rs.getString("table"))
                            .partitions(rs.getString("partitions"))
                            .type(rs.getString("type"))
                            .possibleKeys(rs.getString("possible_keys"))
                            .key(rs.getString("key"))
                            .keyLen(rs.getString("key_len"))
                            .ref(rs.getString("ref"))
                            .rows(getNullableLong(rs, "rows"))
                            .filtered(getNullableDouble(rs, "filtered"))
                            .extra(rs.getString("Extra"))
                            .build());
                }
            }
        } catch (SQLException ex) {
            throw new GatewayException(ErrorCode.INTERNAL_ERROR, "Failed to execute EXPLAIN plan.", ex);
        }
        return rows;
    }

    private Long getNullableLong(ResultSet rs, String col) throws SQLException {
        long val = rs.getLong(col);
        return rs.wasNull() ? null : val;
    }

    private Double getNullableDouble(ResultSet rs, String col) throws SQLException {
        double val = rs.getDouble(col);
        return rs.wasNull() ? null : val;
    }

    private String formatExplainSummary(List<ExplainRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return "NO_PLAN";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sb.append(" ; ");
            }
            ExplainRow r = rows.get(i);
            sb.append("type=").append(r.getType())
                    .append(", key=").append(r.getKey())
                    .append(", keyLen=").append(r.getKeyLen())
                    .append(", rows=").append(r.getRows())
                    .append(", filtered=").append(r.getFiltered())
                    .append(", Extra=").append(r.getExtra());
        }
        return sb.toString();
    }

    private List<Long> extractIds(List<ApiUsageLog> logs) {
        List<Long> ids = new ArrayList<>(logs.size());
        for (ApiUsageLog item : logs) {
            ids.add(item.getId());
        }
        return ids;
    }

    private long median(long[] samples) {
        long[] copy = Arrays.copyOf(samples, samples.length);
        Arrays.sort(copy);
        return copy[copy.length / 2];
    }

    private long nanosToRoundedMs(long nanos) {
        return Math.round(nanos / 1_000_000.0);
    }

    private record CursorPoint(LocalDateTime createTime, long id) {
    }
}
