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
import org.springframework.transaction.TransactionException;

import java.io.IOException;
import java.sql.SQLTransientException;

/**
 * 异步计费主队列消费者 (手动 ACK/NACK)。
 * <p>
 * 故意不在本类加 {@code @Transactional}，确保调用 {@link BillingSettlementTxService#settleAtomically}
 * 时任何异常都能在事务服务边界完整回滚后抛到本类，再执行 {@code basicNack(requeue=false)} 路由至死信队列 {@code onegate.billing.dlq}。
 */
@Slf4j
@Component
public class BillingSettleConsumer {

    private final BillingSettlementTxService billingSettlementTxService;

    public BillingSettleConsumer(BillingSettlementTxService billingSettlementTxService) {
        this.billingSettlementTxService = billingSettlementTxService;
    }

    @RabbitListener(queues = MqConstants.BILLING_QUEUE)
    public void onBillingMessage(
            BillingMessage message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        if (message == null) {
            log.warn("Received null BillingMessage on {}, rejecting to DLQ", MqConstants.BILLING_QUEUE);
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        try {
            boolean newlySettled = billingSettlementTxService.settleAtomically(message);
            if (newlySettled) {
                log.debug("Billing message settled and ACKed: requestId={}, tenantId={}",
                        message.getRequestId(), message.getTenantId());
            } else {
                log.info("Duplicate billing message ACKed idempotently: requestId={}", message.getRequestId());
            }
            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            if (!isDeterministicBusinessError(ex) && isTransientInfrastructureError(ex)) {
                log.error("Transient infrastructure error during billing settlement, rethrowing for broker/Spring retry: requestId={}, tenantId={}, error={}",
                        message.getRequestId(), message.getTenantId(), ex.getMessage(), ex);
                if (ex instanceof RuntimeException runtimeEx) {
                    throw runtimeEx;
                }
                throw new IllegalStateException("Transient infrastructure failure during billing settlement", ex);
            }
            log.error("Billing settlement failed after transaction rollback, NACKing to DLX: requestId={}, tenantId={}, error={}",
                    message.getRequestId(), message.getTenantId(), ex.getMessage(), ex);
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
                    || cur instanceof TransactionException
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
