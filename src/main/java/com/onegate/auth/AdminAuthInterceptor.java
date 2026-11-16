package com.onegate.auth;

import com.onegate.common.constant.HeaderConstants;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * 管理后台全局鉴权拦截器 (Bean 名称固定为 {@code adminAuthInterceptor}，由 {@code WebMvcConfig} 优先挂载至 {@code /admin/**})。
 */
@Component("adminAuthInterceptor")
public class AdminAuthInterceptor implements HandlerInterceptor {

    private final AdminTokenVerifier adminTokenVerifier;

    public AdminAuthInterceptor(AdminTokenVerifier adminTokenVerifier) {
        this.adminTokenVerifier = adminTokenVerifier;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (request.getDispatcherType() != DispatcherType.REQUEST) {
            return true;
        }
        adminTokenVerifier.verifyOrThrow(request.getHeader(HeaderConstants.ADMIN_TOKEN_HEADER));
        return true;
    }
}
