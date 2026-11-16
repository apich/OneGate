package com.onegate.common.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * 网关标准错误码与 OpenAI 兼容 HTTP 状态码映射。
 */
@Getter
public enum ErrorCode {

    INVALID_REQUEST(HttpStatus.BAD_REQUEST, "invalid_request", "invalid_request_error",
            "Invalid request payload or parameters."),
    INVALID_API_KEY(HttpStatus.UNAUTHORIZED, "invalid_api_key", "authentication_error",
            "Invalid or missing API key."),
    INSUFFICIENT_QUOTA(HttpStatus.PAYMENT_REQUIRED, "insufficient_quota", "insufficient_quota",
            "Insufficient token balance for this request."),
    TENANT_DISABLED(HttpStatus.FORBIDDEN, "tenant_disabled", "permission_error",
            "Tenant account has been disabled."),
    ADMIN_UNAUTHORIZED(HttpStatus.FORBIDDEN, "admin_unauthorized", "permission_error",
            "Invalid or missing admin token."),
    MODEL_NOT_FOUND(HttpStatus.NOT_FOUND, "model_not_found", "invalid_request_error",
            "Requested model has no available upstream channels."),
    RATE_LIMIT_EXCEEDED(HttpStatus.TOO_MANY_REQUESTS, "rate_limit_exceeded", "rate_limit_error",
            "Rate limit (RPM/TPM) exceeded. Please retry later."),
    UPSTREAM_ERROR(HttpStatus.BAD_GATEWAY, "upstream_error", "upstream_error",
            "Upstream LLM provider returned an error or timed out."),
    SERVICE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "service_unavailable", "service_unavailable",
            "All upstream channels are unavailable or in cooldown."),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", "server_error",
            "Internal gateway error.");

    private final HttpStatus httpStatus;
    private final String code;
    private final String type;
    private final String defaultMessage;

    ErrorCode(HttpStatus httpStatus, String code, String type, String defaultMessage) {
        this.httpStatus = httpStatus;
        this.code = code;
        this.type = type;
        this.defaultMessage = defaultMessage;
    }

    public int getStatusCode() {
        return httpStatus.value();
    }
}
