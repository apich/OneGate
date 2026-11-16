package com.onegate.billing;

import com.onegate.common.constant.MqConstants;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.dto.BillingMessage;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.SQLTransientException;

/**
 * 预占冻结超时补偿消费者 (监听原生 RabbitMQ TTL+DLX 到期转发的 {@code onegate.freeze.timeout.queue})。
 * <p>
 * 若请求已正常核销或异常已回滚，{@code settle_or_rollback_quota.lua} 检测到 {@code freezeKey} 不存在会返回 {@code {0, 0}} 幂等跳过；
 * 若因节点宕机等极端原因遗留了冻结额度，则在此原子退还全额冻结 Token 并删除冻结 Key，防止死账。
 */
@Slf4j
@Component
public class FreezeTimeoutConsumer {

    private final QuotaFreezeLuaService quotaFreezeLuaService;

    public FreezeTimeoutConsumer(QuotaFreezeLuaService quotaFreezeLuaService) {
        this.quotaFreezeLuaService = quotaFreezeLuaService;
    }

    @RabbitListener(queues = MqConstants.FREEZE_TIMEOUT_QUEUE)
    public void onFreezeTimeout(
            BillingMessage message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        if (message == null || message.getTenantId() == null || message.getRequestId() == null) {
            log.warn("Received invalid freeze timeout message, ACKing to discard");
            channel.basicAck(deliveryTag, false);
            return;
        }

        try {
            QuotaFreezeLuaService.SettleResult result =
                    quotaFreezeLuaService.rollbackFrozen(message.getTenantId(), message.getRequestId());
            if (result.isApplied()) {
                log.warn("Freeze timeout compensation executed: tenantId={}, requestId={}, refundedTokens={}",
                        message.getTenantId(), message.getRequestId(), result.getRefundedDelta());
            } else {
                log.debug("Freeze timeout message ignored (already settled/rolled back): tenantId={}, requestId={}",
                        message.getTenantId(), message.getRequestId());
            }
            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            if (!isDeterministicBusinessError(ex) && isTransientInfrastructureError(ex)) {
                log.error("Transient infrastructure error during freeze timeout compensation, rethrowing for Spring retry: tenantId={}, requestId={}, error={}",
                        message.getTenantId(), message.getRequestId(), ex.getMessage(), ex);
                if (ex instanceof RuntimeException runtimeEx) {
                    throw runtimeEx;
                }
                throw new IllegalStateException("Transient infrastructure failure during freeze timeout compensation", ex);
            }
            log.error("Failed to process freeze timeout compensation, NACKing non-retryable message: tenantId={}, requestId={}, error={}",
                    message.getTenantId(), message.getRequestId(), ex.getMessage(), ex);
            channel.basicNack(deliveryTag, false, false);
        }
    }

    private boolean isDeterministicBusinessError(Throwable ex) {
        return ex instanceof IllegalStateException
                || ex instanceof IllegalArgumentException
                || ex instanceof GatewayException;
    }

    private boolean isTransientInfrastructureError(Throwable ex) {
        Throwable cur = ex;
        for (int depth = 0; cur != null && depth < 16; depth++) {
            if (cur instanceof DataAccessException
                    || cur instanceof RedisConnectionFailureException
                    || cur instanceof RedisSystemException
                    || cur instanceof SQLTransientException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }
}
