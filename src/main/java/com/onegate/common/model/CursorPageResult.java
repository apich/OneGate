package com.onegate.common.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

/**
 * 基于复合游标 (create_time, id) 的无偏移深分页响应模型。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CursorPageResult<T> {

    private List<T> records;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private LocalDateTime nextCreateTime;

    private Long nextId;

    private boolean hasMore;

    private int size;

    /** 本次查询数据库执行耗时 (毫秒), 便于性能对比展示 */
    private long costMs;

    public static <T> CursorPageResult<T> empty(long costMs) {
        return CursorPageResult.<T>builder()
                .records(Collections.emptyList())
                .hasMore(false)
                .size(0)
                .costMs(costMs)
                .build();
    }
}
