package com.onegate.router;

import com.onegate.common.config.OneGateProperties;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.dto.ChatCompletionRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 轻量级本地 Token 估算器。
 * <p>
 * 用于两处关键场景：<br>
 * 1. 请求准入阶段：根据消息角色与内容长度快速估算 {@code Prompt Tokens}，结合 {@code max_tokens} 完成 TPM 限流与额度预占冻结；<br>
 * 2. 断连/异常兜底阶段：当客户端在收到全部流式尾包 {@code usage} 前主动断开 SSE，按已推送分片字符数估算输出 Token（至少 1）。
 */
@Component
public class TokenEstimator {

    public static final int DEFAULT_MAX_TOKENS = 512;
    public static final int DEFAULT_MAX_TOKENS_LIMIT = 65_536;

    private final OneGateProperties properties;

    public TokenEstimator() {
        this.properties = null;
    }

    @Autowired
    public TokenEstimator(OneGateProperties properties) {
        this.properties = properties;
    }

    /**
     * 估算输入消息列表对应的 Prompt Tokens (至少为 1)。
     */
    public int estimatePromptTokens(ChatCompletionRequest request) {
        if (request == null || request.getMessages() == null || request.getMessages().isEmpty()) {
            return 1;
        }
        long total = 3L; // 对话回复引导头开销
        for (ChatCompletionRequest.ChatMessage message : request.getMessages()) {
            if (message == null) {
                continue;
            }
            total += 4L; // 每条消息角色与分隔符固定开销
            total += countTextTokensLong(message.getRole());
            total += countTextTokensLong(message.getContent());
            if (total > Integer.MAX_VALUE) {
                throw new GatewayException(ErrorCode.INVALID_REQUEST, "Prompt exceeds maximum supported token bounds.");
            }
        }
        return Math.max(1, (int) total);
    }

    /**
     * 解析请求 max_tokens，未传递或非正数时默认按 512 估算预冻上限，并受限于配置的最大安全上界。
     */
    public int resolveMaxTokens(ChatCompletionRequest request) {
        int limit = getEffectiveMaxTokensLimit();
        if (request != null && request.getMaxTokens() != null && request.getMaxTokens() > 0) {
            return Math.min(request.getMaxTokens(), limit);
        }
        return Math.min(DEFAULT_MAX_TOKENS, limit);
    }

    /**
     * 计算入站安全预冻总 Token ({@code promptTokens + maxTokens})，防止整型溢出。
     */
    public int estimateTotalFrozenTokens(ChatCompletionRequest request) {
        int promptTokens = estimatePromptTokens(request);
        int maxTokens = resolveMaxTokens(request);
        long total = (long) promptTokens + (long) maxTokens;
        if (total <= 0 || total > Integer.MAX_VALUE) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "Estimated total tokens exceed integer bounds.");
        }
        return (int) total;
    }

    /**
     * 计算入站预估总 Token ({@code promptTokens + maxTokens})。
     */
    public int estimateTotalRequestedTokens(ChatCompletionRequest request) {
        return estimateTotalFrozenTokens(request);
    }

    private int getEffectiveMaxTokensLimit() {
        if (properties != null && properties.getMaxTokensLimit() > 0) {
            return properties.getMaxTokensLimit();
        }
        return DEFAULT_MAX_TOKENS_LIMIT;
    }

    /**
     * 根据已成功推送给客户端的流式分片累计文本估算 Completion Tokens（兜底至少返回 1）。
     */
    public int estimateCompletionTokens(CharSequence pushedContent) {
        return Math.max(1, countTextTokens(pushedContent));
    }

    /**
     * 混合中英文文本 Token 近似计算：CJK 字符约 1 char ≈ 1 token，ASCII 字符约 4 chars ≈ 1 token。
     */
    public int countTextTokens(CharSequence text) {
        long tokens = countTextTokensLong(text);
        return tokens >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) tokens;
    }

    private long countTextTokensLong(CharSequence text) {
        if (text == null || text.isEmpty()) {
            return 0L;
        }
        long cjkChars = 0L;
        long asciiChars = 0L;
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch <= 0x7F) {
                asciiChars++;
            } else {
                cjkChars++;
            }
        }
        long asciiTokens = (asciiChars + 3L) / 4L;
        return cjkChars + asciiTokens;
    }
}
