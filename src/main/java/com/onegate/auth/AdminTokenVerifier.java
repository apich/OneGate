package com.onegate.auth;

import com.onegate.common.config.OneGateProperties;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * 管理后台接口统一 Token 校验组件，采用常量时间摘要比较防止时序侧信道攻击。
 */
@Component
public class AdminTokenVerifier {

    private final OneGateProperties properties;

    public AdminTokenVerifier(OneGateProperties properties) {
        this.properties = properties;
    }

    public void verifyOrThrow(String providedToken) {
        String expectedToken = properties.getAdminToken();
        if (providedToken == null || providedToken.isBlank()
                || expectedToken == null || expectedToken.isBlank()) {
            throw new GatewayException(ErrorCode.ADMIN_UNAUTHORIZED);
        }
        boolean matched = MessageDigest.isEqual(
                providedToken.getBytes(StandardCharsets.UTF_8),
                expectedToken.getBytes(StandardCharsets.UTF_8)
        );
        if (!matched) {
            throw new GatewayException(ErrorCode.ADMIN_UNAUTHORIZED);
        }
    }
}
