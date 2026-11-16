package com.onegate.common.constant;

/**
 * HTTP 请求/响应头及 Servlet Request 上下文属性常量。
 */
public final class HeaderConstants {

    private HeaderConstants() {
    }

    /** 标准认证头 */
    public static final String AUTHORIZATION = "Authorization";

    /** Bearer Token 前缀 */
    public static final String BEARER_PREFIX = "Bearer ";

    /** 网关全局请求追踪与幂等ID响应头 */
    public static final String REQUEST_ID_HEADER = "X-OneGate-Request-Id";

    /** 实际命中的上游模型厂商响应头 */
    public static final String PROVIDER_HEADER = "X-OneGate-Provider";

    /** 是否触发主备故障转移标识响应头 ("true"/"false") */
    public static final String FAILOVER_HEADER = "X-OneGate-Failover";

    /** 认证校验耗时(微秒)响应头，用于展示 L1/L2 缓存性能 */
    public static final String AUTH_LATENCY_US_HEADER = "X-OneGate-Auth-Micros";

    /** 管理端鉴权请求头 */
    public static final String ADMIN_TOKEN_HEADER = "X-OneGate-Admin-Token";

    /** Servlet Request 属性键：当前认证通过的租户上下文 (TenantAuthInfo) */
    public static final String TENANT_AUTH_ATTR = "onegate.tenantAuth";

    /** Servlet Request 属性键：当前租户ID */
    public static final String TENANT_ID_ATTR = "onegate.tenantId";

    /** Servlet Request 属性键：当前请求ID */
    public static final String REQUEST_ID_ATTR = "onegate.requestId";
}
