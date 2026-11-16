package com.onegate.common.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.ConstraintViolationException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.stream.Collectors;

/**
 * 全局异常处理器：统一输出 OpenAI 兼容格式错误响应与标准 HTTP 状态码。
 * <pre>
 * {
 *   "error": {
 *     "message": "...",
 *     "type": "rate_limit_error",
 *     "param": null,
 *     "code": "rate_limit_exceeded"
 *   }
 * }
 * </pre>
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(GatewayException.class)
    public ResponseEntity<OpenAiErrorResponse> handleGatewayException(GatewayException ex) {
        ErrorCode errorCode = ex.getErrorCode();
        if (errorCode.getHttpStatus().is5xxServerError()) {
            log.error("Gateway server error [{}]: {}", errorCode.getCode(), ex.getMessage(), ex);
        } else {
            log.warn("Gateway rejected request [{}]: {}", errorCode.getCode(), ex.getMessage());
        }
        return buildResponse(errorCode, ex.getMessage(), null);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<OpenAiErrorResponse> handleValidationException(MethodArgumentNotValidException ex) {
        String param = ex.getBindingResult().getFieldErrors().stream()
                .map(FieldError::getField)
                .findFirst()
                .orElse(null);
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .map(err -> err.getField() + ": " + (err.isBindingFailure() ? "invalid value type" : err.getDefaultMessage()))
                .collect(Collectors.joining("; "));
        return buildResponse(ErrorCode.INVALID_REQUEST,
                detail.isEmpty() ? ErrorCode.INVALID_REQUEST.getDefaultMessage() : detail, param);
    }

    @ExceptionHandler({
            ConstraintViolationException.class,
            HttpMessageNotReadableException.class,
            MissingServletRequestParameterException.class,
            MethodArgumentTypeMismatchException.class
    })
    public ResponseEntity<OpenAiErrorResponse> handleBadRequest(Exception ex) {
        log.warn("Rejected malformed request ({}): {}", ex.getClass().getSimpleName(), ex.getMessage());
        String sanitizedMessage;
        String param = null;
        if (ex instanceof HttpMessageNotReadableException) {
            sanitizedMessage = "Malformed JSON request body or invalid field format.";
        } else if (ex instanceof MissingServletRequestParameterException mspe) {
            param = mspe.getParameterName();
            sanitizedMessage = "Missing required request parameter: " + param;
        } else if (ex instanceof MethodArgumentTypeMismatchException matme) {
            param = matme.getName();
            sanitizedMessage = "Invalid value for parameter: " + param;
        } else if (ex instanceof ConstraintViolationException cve) {
            String detail = cve.getConstraintViolations().stream()
                    .map(v -> {
                        String path = v.getPropertyPath() != null ? v.getPropertyPath().toString() : "";
                        int dotIdx = path.lastIndexOf('.');
                        String leaf = (dotIdx >= 0 && dotIdx + 1 < path.length()) ? path.substring(dotIdx + 1) : path;
                        return (leaf.isEmpty() ? "param" : leaf) + ": " + v.getMessage();
                    })
                    .collect(Collectors.joining("; "));
            sanitizedMessage = detail.isEmpty() ? ErrorCode.INVALID_REQUEST.getDefaultMessage() : detail;
        } else {
            sanitizedMessage = ErrorCode.INVALID_REQUEST.getDefaultMessage();
        }
        return buildResponse(ErrorCode.INVALID_REQUEST, sanitizedMessage, param);
    }

    @ExceptionHandler({NoHandlerFoundException.class, NoResourceFoundException.class})
    public ResponseEntity<OpenAiErrorResponse> handleNotFound(Exception ex) {
        log.warn("Endpoint not found: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new OpenAiErrorResponse(
                new OpenAiErrorDetail("Endpoint not found", "invalid_request_error", null, "not_found")
        ));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<OpenAiErrorResponse> handleUnexpectedException(Exception ex) {
        log.error("Unhandled exception in OneGate gateway", ex);
        return buildResponse(ErrorCode.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR.getDefaultMessage(), null);
    }

    private ResponseEntity<OpenAiErrorResponse> buildResponse(ErrorCode errorCode, String message, String param) {
        OpenAiErrorResponse body = new OpenAiErrorResponse(
                new OpenAiErrorDetail(message, errorCode.getType(), param, errorCode.getCode())
        );
        return ResponseEntity.status(errorCode.getHttpStatus()).body(body);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OpenAiErrorResponse {
        private OpenAiErrorDetail error;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public static class OpenAiErrorDetail {
        private String message;
        private String type;
        private String param;
        private String code;
    }
}
