package com.onegate.auth;

import com.onegate.domain.dto.TenantAuthInfo;

/**
 * 请求级租户认证上下文 (基于 ThreadLocal)。
 * <p>
 * 由 {@link ApiKeyAuthInterceptor#preHandle} 在主请求阶段写入，并在
 * {@link ApiKeyAuthInterceptor#afterCompletion} 中强制清理，防止 Tomcat 线程池复用引发内存泄漏与租户串号。
 */
public final class TenantContext {

    private static final ThreadLocal<TenantAuthInfo> CONTEXT = new ThreadLocal<>();

    private TenantContext() {
    }

    public static void set(TenantAuthInfo authInfo) {
        if (authInfo == null) {
            CONTEXT.remove();
        } else {
            CONTEXT.set(authInfo);
        }
    }

    public static TenantAuthInfo get() {
        return CONTEXT.get();
    }

    public static String getTenantId() {
        TenantAuthInfo authInfo = CONTEXT.get();
        return authInfo != null ? authInfo.getTenantId() : null;
    }

    public static void clear() {
        CONTEXT.remove();
    }
}
