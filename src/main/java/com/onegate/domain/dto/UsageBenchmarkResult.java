package com.onegate.domain.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 深分页与覆盖索引性能压测对比结果：传统 {@code LIMIT offset, size} vs 复合游标 Seek + 覆盖索引统计。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UsageBenchmarkResult {

    private String tenantId;

    private String modelName;

    private Long offset;

    private Integer pageSize;

    private Long totalRows;

    /** 覆盖索引聚合查询耗时 (毫秒) */
    private Long coveringStatsMs;

    /** 覆盖索引统计命中的请求总条数 */
    private Long coveringStatsRequestCount;

    /** 覆盖索引统计命中的总 Token 数 */
    private Long coveringStatsTotalTokens;

    /** 传统 LIMIT offset, pageSize 查询耗时 (毫秒) */
    private Long traditionalOffsetMs;

    /** 复合游标 (create_time, id) < (?, ?) 查询耗时 (毫秒) */
    private Long cursorSeekMs;

    /** 加速比 (traditionalOffsetMs / max(cursorSeekMs, 1)) */
    private Double speedupRatio;

    private Integer traditionalResultCount;

    private Integer cursorResultCount;

    /** 传统深分页与复合游标分页返回的记录 ID 集合是否完全一致 */
    private Boolean resultsConsistent;

    /** 覆盖索引统计 SQL EXPLAIN 摘要 */
    private String explainCoveringStats;

    /** 传统深分页 SQL EXPLAIN 摘要 */
    private String explainTraditionalOffset;

    /** 复合游标深分页 SQL EXPLAIN 摘要 */
    private String explainCursorSeek;

    private List<ExplainRow> explainCoveringRows;

    private List<ExplainRow> explainTraditionalRows;

    private List<ExplainRow> explainCursorRows;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExplainRow {
        private Long id;
        private String selectType;
        private String table;
        private String partitions;
        private String type;
        private String possibleKeys;
        private String key;
        private String keyLen;
        private String ref;
        private Long rows;
        private Double filtered;
        private String extra;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SeedSummary {
        private String tenantId;
        private long targetTotalRows;
        private long existingRowsBefore;
        private long insertedRows;
        private long totalRowsAfter;
        private boolean skipped;
        private long elapsedMs;
    }
}
