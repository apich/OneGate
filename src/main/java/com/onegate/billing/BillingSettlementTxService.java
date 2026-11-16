package com.onegate.billing;

import com.onegate.domain.dto.BillingMessage;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import com.onegate.domain.mapper.TenantAccountMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * 独立事务计费落库服务。
 * <p>
 * 与消费者 {@link BillingSettleConsumer} 分离，确保 Spring {@code @Transactional} AOP 代理生效且异常不会在事务内部被吞掉。
 * 单事务内先写流水（依赖 {@code uk_request_id} 唯一索引防重幂等），再执行 {@code balance >= actual_cost} 条件扣减；
 * 若扣减影响行数为 0 则抛出异常触发整事务回滚，保证流水与余额严格一致。
 */
@Slf4j
@Service
public class BillingSettlementTxService {

    private static final int MAX_ID_LENGTH = 64;
    private static final int MAX_MODEL_NAME_LENGTH = 64;
    private static final int MAX_PROVIDER_LENGTH = 32;
    private static final int MAX_FINISH_REASON_LENGTH = 32;

    private final ApiUsageLogMapper apiUsageLogMapper;
    private final TenantAccountMapper tenantAccountMapper;

    public BillingSettlementTxService(
            ApiUsageLogMapper apiUsageLogMapper,
            TenantAccountMapper tenantAccountMapper) {
        this.apiUsageLogMapper = apiUsageLogMapper;
        this.tenantAccountMapper = tenantAccountMapper;
    }

    /**
     * 在单事务内原子写入调用流水并扣减租户 MySQL 余额。
     *
     * @return {@code true} 本次新结算并扣款成功；{@code false} 检测到 {@code uk_request_id} 重复投递，幂等跳过
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean settleAtomically(BillingMessage msg) {
        if (msg == null || msg.getRequestId() == null || msg.getRequestId().isBlank()
                || msg.getTenantId() == null || msg.getTenantId().isBlank()) {
            throw new IllegalArgumentException("BillingMessage requestId and tenantId must not be blank.");
        }

        String safeRequestId = truncateRequired(msg.getRequestId(), MAX_ID_LENGTH);
        String safeTenantId = truncateRequired(msg.getTenantId(), MAX_ID_LENGTH);
        String safeModelName = truncateWithDefault(msg.getModelName(), MAX_MODEL_NAME_LENGTH, "unknown");
        String safeProvider = truncateWithDefault(msg.getProvider(), MAX_PROVIDER_LENGTH, "UNKNOWN");
        String safeFinishReason = truncateNullable(msg.getFinishReason(), MAX_FINISH_REASON_LENGTH);

        int promptTokens = msg.getPromptTokens() != null ? Math.max(0, msg.getPromptTokens()) : 0;
        int completionTokens = msg.getCompletionTokens() != null ? Math.max(0, msg.getCompletionTokens()) : 0;
        int totalTokens = msg.getTotalTokens() != null
                ? Math.max(0, msg.getTotalTokens())
                : (int) Math.min(Integer.MAX_VALUE, (long) promptTokens + completionTokens);

        ApiUsageLog logEntry = ApiUsageLog.builder()
                .requestId(safeRequestId)
                .tenantId(safeTenantId)
                .modelName(safeModelName)
                .provider(safeProvider)
                .channelId(msg.getChannelId() != null ? msg.getChannelId() : 0L)
                .promptTokens(promptTokens)
                .completionTokens(completionTokens)
                .totalTokens(totalTokens)
                .costMs(msg.getCostMs() != null ? Math.max(0, msg.getCostMs()) : 0)
                .finishReason(safeFinishReason)
                .estimated(Boolean.TRUE.equals(msg.getEstimated()) ? 1 : 0)
                .createTime(msg.getRequestTime() != null ? msg.getRequestTime() : LocalDateTime.now())
                .build();

        // 1. 插入计费流水: 依赖 uk_request_id 唯一键约束实现消费幂等防重
        try {
            apiUsageLogMapper.insert(logEntry);
        } catch (DuplicateKeyException ex) {
            log.info("Duplicate billing message detected via uk_request_id, skipping idempotently: requestId={}",
                    safeRequestId);
            return false;
        }

        // 2. 条件原子扣减余额: WHERE tenant_id = ? AND balance >= actual_cost
        int affectedRows = tenantAccountMapper.deductBalanceAtomically(safeTenantId, totalTokens);
        if (affectedRows == 0) {
            throw new IllegalStateException(String.format(
                    "Atomic balance deduction failed (insufficient balance or tenant missing), rolling back usage log: "
                            + "tenantId=%s, requestId=%s, actualCost=%d",
                    safeTenantId, safeRequestId, totalTokens));
        }

        log.debug("Atomic billing settled in MySQL: tenantId={}, requestId={}, totalTokens={}",
                safeTenantId, safeRequestId, totalTokens);
        return true;
    }

    private static String truncateRequired(String value, int maxLen) {
        String trimmed = value.trim();
        return trimmed.length() <= maxLen ? trimmed : trimmed.substring(0, maxLen);
    }

    private static String truncateWithDefault(String value, int maxLen, String defaultVal) {
        if (value == null || value.isBlank()) {
            return defaultVal;
        }
        String trimmed = value.trim();
        return trimmed.length() <= maxLen ? trimmed : trimmed.substring(0, maxLen);
    }

    private static String truncateNullable(String value, int maxLen) {
        if (value == null) {
            return null;
        }
        return value.length() <= maxLen ? value : value.substring(0, maxLen);
    }
}
