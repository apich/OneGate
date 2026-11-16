package com.onegate.controller;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.onegate.auth.AdminTokenVerifier;
import com.onegate.auth.TenantAuthCacheService;
import com.onegate.billing.QuotaFreezeLuaService;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.common.model.Result;
import com.onegate.domain.dto.AdminKeyUpdateRequest;
import com.onegate.domain.dto.AdminRechargeRequest;
import com.onegate.domain.dto.AdminTenantResponse;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.TenantAccountMapper;
import jakarta.validation.Valid;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * 租户配额与 API Key 管理后台接口。
 * <p>
 * 全量接口受 {@code AdminAuthInterceptor} 统一拦截并在方法内保留 {@link AdminTokenVerifier} 双重校验，
 * 查询与充值接口仅返回脱敏密钥 {@code apiKeyMasked}。
 */
@RestController
@RequestMapping("/admin/tenants")
public class AdminTenantController {

    private final TenantAccountMapper tenantAccountMapper;
    private final TenantAuthCacheService tenantAuthCacheService;
    private final QuotaFreezeLuaService quotaFreezeLuaService;
    private final AdminTokenVerifier adminTokenVerifier;

    public AdminTenantController(
            TenantAccountMapper tenantAccountMapper,
            TenantAuthCacheService tenantAuthCacheService,
            QuotaFreezeLuaService quotaFreezeLuaService,
            AdminTokenVerifier adminTokenVerifier) {
        this.tenantAccountMapper = tenantAccountMapper;
        this.tenantAuthCacheService = tenantAuthCacheService;
        this.quotaFreezeLuaService = quotaFreezeLuaService;
        this.adminTokenVerifier = adminTokenVerifier;
    }

    /**
     * 查询租户当前 MySQL 余额、状态、限流阈值与脱敏密钥快照。
     */
    @GetMapping("/{tenantId}")
    public Result<AdminTenantResponse> getTenant(
            @RequestHeader(value = HeaderConstants.ADMIN_TOKEN_HEADER, required = false) String adminToken,
            @PathVariable("tenantId") String tenantId) {
        adminTokenVerifier.verifyOrThrow(adminToken);
        TenantAccount account = requireTenant(tenantId);
        return Result.ok(AdminTenantResponse.fromEntityMasked(account));
    }

    /**
     * 通过 {@link LambdaUpdateWrapper} 局部更新租户状态 / RPM / TPM / 密钥，
     * 严禁覆盖 {@code balance} / {@code frozenTokens} / {@code version} 等并发记账字段。
     */
    @PostMapping("/{tenantId}/api-key")
    public Result<AdminTenantResponse> updateApiKeyAndQuota(
            @RequestHeader(value = HeaderConstants.ADMIN_TOKEN_HEADER, required = false) String adminToken,
            @PathVariable("tenantId") String tenantId,
            @Valid @RequestBody AdminKeyUpdateRequest request) {
        adminTokenVerifier.verifyOrThrow(adminToken);

        TenantAccount existing = requireTenant(tenantId);
        String oldApiKey = existing.getApiKey();
        String newApiKey = resolveNewApiKey(request, oldApiKey);
        boolean keyRequested = Boolean.TRUE.equals(request.getRotateApiKey())
                || (request.getApiKey() != null && !request.getApiKey().isBlank());
        boolean keyChanged = !Objects.equals(oldApiKey, newApiKey);

        LambdaUpdateWrapper<TenantAccount> updateWrapper = new LambdaUpdateWrapper<TenantAccount>()
                .eq(TenantAccount::getTenantId, tenantId)
                .set(TenantAccount::getUpdateTime, LocalDateTime.now());
        if (request.getTenantName() != null && !request.getTenantName().isBlank()) {
            updateWrapper.set(TenantAccount::getTenantName, request.getTenantName().trim());
        }
        if (request.getStatus() != null) {
            updateWrapper.set(TenantAccount::getStatus, request.getStatus());
        }
        if (request.getRpmLimit() != null) {
            updateWrapper.set(TenantAccount::getRpmLimit, request.getRpmLimit());
        }
        if (request.getTpmLimit() != null) {
            updateWrapper.set(TenantAccount::getTpmLimit, request.getTpmLimit());
        }
        if (keyChanged) {
            updateWrapper.set(TenantAccount::getApiKey, newApiKey);
        }

        try {
            int updatedRows = tenantAccountMapper.update(null, updateWrapper);
            if (updatedRows == 0) {
                throw new GatewayException(ErrorCode.INVALID_REQUEST, "Tenant not found: " + tenantId);
            }
        } catch (DuplicateKeyException ex) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST,
                    "API key already exists for another tenant.", ex);
        }

        // 先提交 DB 局部更新，再精确失效 Redis L2 + 广播清除全节点 L1 Caffeine
        if (keyChanged) {
            tenantAuthCacheService.invalidateApiKey(oldApiKey, newApiKey);
        } else {
            tenantAuthCacheService.invalidateApiKey(oldApiKey);
        }

        TenantAccount latest = requireTenant(tenantId);
        AdminTenantResponse response = keyRequested
                ? AdminTenantResponse.fromEntityWithPlaintextKey(latest)
                : AdminTenantResponse.fromEntityMasked(latest);
        return Result.ok("Tenant configuration updated", response);
    }

    /**
     * 在租户配额初始化互斥锁保护下执行 DB 充值与 Redis 可用余额同步 INCRBY，消除冷启动并发竞态。
     */
    @PostMapping("/{tenantId}/recharge")
    public Result<AdminTenantResponse> recharge(
            @RequestHeader(value = HeaderConstants.ADMIN_TOKEN_HEADER, required = false) String adminToken,
            @PathVariable("tenantId") String tenantId,
            @Valid @RequestBody AdminRechargeRequest request) {
        adminTokenVerifier.verifyOrThrow(adminToken);

        quotaFreezeLuaService.executeUnderInitLock(tenantId, () -> {
            int affected = tenantAccountMapper.rechargeBalance(tenantId, request.getAmount());
            if (affected == 0) {
                throw new GatewayException(ErrorCode.INVALID_REQUEST, "Tenant not found: " + tenantId);
            }
            quotaFreezeLuaService.incrementRedisBalanceIfExists(tenantId, request.getAmount());
        });

        TenantAccount latest = requireTenant(tenantId);
        return Result.ok("Recharge succeeded", AdminTenantResponse.fromEntityMasked(latest));
    }

    private TenantAccount requireTenant(String tenantId) {
        TenantAccount account = tenantAccountMapper.selectByTenantId(tenantId);
        if (account == null) {
            throw new GatewayException(ErrorCode.INVALID_REQUEST, "Tenant not found: " + tenantId);
        }
        return account;
    }

    private String resolveNewApiKey(AdminKeyUpdateRequest request, String currentApiKey) {
        if (Boolean.TRUE.equals(request.getRotateApiKey())) {
            return "sk-onegate-" + UUID.randomUUID().toString().replace("-", "").substring(0, 24);
        }
        if (request.getApiKey() != null && !request.getApiKey().isBlank()) {
            return request.getApiKey().trim();
        }
        return currentApiKey;
    }
}
