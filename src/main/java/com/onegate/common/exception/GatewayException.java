package com.onegate.common.exception;

import lombok.Getter;

/**
 * 网关业务与流控统一运行时异常。
 */
@Getter
public class GatewayException extends RuntimeException {

    private final ErrorCode errorCode;

    public GatewayException(ErrorCode errorCode) {
        super(errorCode.getDefaultMessage());
        this.errorCode = errorCode;
    }

    public GatewayException(ErrorCode errorCode, String message) {
        super(message != null ? message : errorCode.getDefaultMessage());
        this.errorCode = errorCode;
    }

    public GatewayException(ErrorCode errorCode, String message, Throwable cause) {
        super(message != null ? message : errorCode.getDefaultMessage(), cause);
        this.errorCode = errorCode;
    }
}
