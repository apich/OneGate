package com.onegate.controller;

import com.onegate.auth.AdminTokenVerifier;
import com.onegate.auth.TenantContext;
import com.onegate.benchmark.UsageDataBenchmarkRunner;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.common.model.CursorPageResult;
import com.onegate.common.model.Result;
import com.onegate.domain.dto.UsageBenchmarkResult;
import com.onegate.domain.dto.UsageBenchmarkResult.SeedSummary;
import com.onegate.domain.dto.UsageStatsResponse;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * 模块 4：租户侧覆盖索引统计与复合游标深分页查询接口 + 管理侧 200 万级基准触发接口。
 */
@RestController
public class UsageAnalyticsController {

    private static final int MIN_PAGE_SIZE = 1;
    private static final int MAX_PAGE_SIZE = 200;
    private static final int MAX_SEED_ROWS = 5_000_000;

    private static final List<DateTimeFormatter> SUPPORTED_DATE_TIME_FORMATTERS = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    );

    private final ApiUsageLogMapper apiUsageLogMapper;
    private final UsageDataBenchmarkRunner benchmarkRunner;
    private final AdminTokenVerifier adminTokenVerifier;

    public UsageAnalyticsController(
            ApiUsageLogMapper apiUsageLogMapper,
            UsageDataBenchmarkRunner benchmarkRunner,
            AdminTokenVerifier adminTokenVerifier) {
        this.apiUsageLogMapper = apiUsageLogMapper;
        this.benchmarkRunner = benchmarkRunner;
        this.adminTokenVerifier = adminTokenVerifier;
    }

    /**
     * 租户侧调用量聚合统计（走覆盖索引 idx_tenant_model_time_tokens，零回表）。
     */
    @GetMapping("/v1/usage/stats")
    public Result<UsageStatsResponse> getUsageStats(
            HttpServletRequest servletRequest,
            @RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "startTime", required = false) String startTime,
            @RequestParam(value = "endTime", required = false) String endTime) {
        String tenantId = resolveTenantId(servletRequest);
        String normalizedModel = normalizeBlankToNull(modelName);
        LocalDateTime parsedStart = parseFlexibleDateTime(startTime, "startTime");
        LocalDateTime parsedEnd = parseFlexibleDateTime(endTime, "endTime");

        if (parsedStart != null && parsedEnd != null && parsedStart.isAfter(parsedEnd)) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "startTime must not be after endTime.");
        }

        long startNanos = System.nanoTime();
        UsageStatsResponse stats = apiUsageLogMapper.selectCoveringStats(
                tenantId, normalizedModel, parsedStart, parsedEnd);
        long elapsedMs = Math.round((System.nanoTime() - startNanos) / 1_000_000.0);

        if (stats == null) {
            stats = UsageStatsResponse.builder()
                    .tenantId(tenantId)
                    .modelName(normalizedModel)
                    .requestCount(0L)
                    .totalTokens(0L)
                    .build();
        }
        stats.setQueryElapsedMs(elapsedMs);
        return Result.ok(stats);
    }

    /**
     * 租户侧账单流水复合游标深分页查询（基于 (create_time, id) 无偏移高效定位）。
     */
    @GetMapping("/v1/usage/logs")
    public Result<CursorPageResult<ApiUsageLog>> getUsageLogs(
            HttpServletRequest servletRequest,
            @RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "lastCreateTime", required = false) String lastCreateTime,
            @RequestParam(value = "lastId", required = false) Long lastId,
            @RequestParam(value = "pageSize", defaultValue = "20") int pageSize) {
        String tenantId = resolveTenantId(servletRequest);
        if (pageSize < MIN_PAGE_SIZE || pageSize > MAX_PAGE_SIZE) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "pageSize must be between " + MIN_PAGE_SIZE + " and " + MAX_PAGE_SIZE + ".");
        }

        String normalizedModel = normalizeBlankToNull(modelName);
        LocalDateTime parsedLastCreateTime = parseFlexibleDateTime(lastCreateTime, "lastCreateTime");
        if ((parsedLastCreateTime == null) != (lastId == null)) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "lastCreateTime and lastId must either both be provided or both be omitted.");
        }

        long startNanos = System.nanoTime();
        List<ApiUsageLog> fetched = apiUsageLogMapper.selectByCursor(
                tenantId, normalizedModel, parsedLastCreateTime, lastId, pageSize + 1);
        long costMs = Math.round((System.nanoTime() - startNanos) / 1_000_000.0);

        if (fetched == null || fetched.isEmpty()) {
            return Result.ok(CursorPageResult.empty(costMs));
        }

        boolean hasMore = fetched.size() > pageSize;
        List<ApiUsageLog> pageRecords = hasMore
                ? List.copyOf(fetched.subList(0, pageSize))
                : List.copyOf(fetched);
        ApiUsageLog tail = pageRecords.get(pageRecords.size() - 1);

        CursorPageResult<ApiUsageLog> result = CursorPageResult.<ApiUsageLog>builder()
                .records(pageRecords)
                .nextCreateTime(tail.getCreateTime())
                .nextId(tail.getId())
                .hasMore(hasMore)
                .size(pageRecords.size())
                .costMs(costMs)
                .build();
        return Result.ok(result);
    }

    /**
     * 管理侧触发/校验 200 万级压测数据批处理写入。
     */
    @PostMapping("/admin/benchmark/seed-2m")
    public Result<SeedSummary> seedTwoMillionRows(
            @RequestHeader(value = HeaderConstants.ADMIN_TOKEN_HEADER, required = false) String adminToken,
            @RequestParam(value = "tenantId", defaultValue = "t_bench_2m") String tenantId,
            @RequestParam(value = "targetRows", defaultValue = "2000000") int targetRows) {
        adminTokenVerifier.verifyOrThrow(adminToken);
        if (tenantId == null || tenantId.isBlank() || tenantId.trim().length() > 64) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "tenantId must be between 1 and 64 characters.");
        }
        if (targetRows < 1 || targetRows > MAX_SEED_ROWS) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "targetRows must be between 1 and " + MAX_SEED_ROWS + ".");
        }
        SeedSummary summary = benchmarkRunner.seedTwoMillionRowsIfNeeded(tenantId, targetRows);
        return Result.ok(summary);
    }

    /**
     * 管理侧触发覆盖索引与复合游标深分页真实 EXPLAIN + 耗时基准对比。
     */
    @GetMapping("/admin/benchmark/compare")
    public Result<UsageBenchmarkResult> compareDeepPagination(
            @RequestHeader(value = HeaderConstants.ADMIN_TOKEN_HEADER, required = false) String adminToken,
            @RequestParam(value = "tenantId", defaultValue = "t_bench_2m") String tenantId,
            @RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "offset", defaultValue = "1000000") long offset,
            @RequestParam(value = "pageSize", defaultValue = "20") int pageSize) {
        adminTokenVerifier.verifyOrThrow(adminToken);
        if (tenantId == null || tenantId.isBlank() || tenantId.trim().length() > 64) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "tenantId must be between 1 and 64 characters.");
        }
        if (offset < 0) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "offset must be >= 0.");
        }
        if (pageSize < MIN_PAGE_SIZE || pageSize > MAX_PAGE_SIZE) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "pageSize must be between " + MIN_PAGE_SIZE + " and " + MAX_PAGE_SIZE + ".");
        }
        UsageBenchmarkResult benchmarkResult = benchmarkRunner.runDeepPaginationAndCoveringBenchmark(
                tenantId, normalizeBlankToNull(modelName), offset, pageSize);
        return Result.ok(benchmarkResult);
    }

    private String resolveTenantId(HttpServletRequest request) {
        Object attr = request.getAttribute(HeaderConstants.TENANT_ID_ATTR);
        if (attr instanceof String tenantId && !tenantId.isBlank()) {
            return tenantId;
        }
        String ctxTenantId = TenantContext.getTenantId();
        if (ctxTenantId != null && !ctxTenantId.isBlank()) {
            return ctxTenantId;
        }
        throw new GatewayException(ErrorCode.INVALID_API_KEY, "Missing authenticated tenant attribute.");
    }

    private String normalizeBlankToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private LocalDateTime parseFlexibleDateTime(String raw, String fieldName) {
        String normalized = normalizeBlankToNull(raw);
        if (normalized == null) {
            return null;
        }
        for (DateTimeFormatter formatter : SUPPORTED_DATE_TIME_FORMATTERS) {
            try {
                return LocalDateTime.parse(normalized, formatter);
            } catch (DateTimeParseException ignored) {
                // try next pattern
            }
        }
        try {
            return LocalDate.parse(normalized, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay();
        } catch (DateTimeParseException ex) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "Invalid datetime format for " + fieldName + ": " + normalized);
        }
    }
}
