package com.onegate.billing;

import com.onegate.common.config.OneGateProperties;
import com.onegate.common.constant.MqConstants;
import com.onegate.domain.dto.BillingMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

/**
 * RabbitMQ 异步计费与预占冻结超时延迟消息生产者。
 * <p>
 * 发送时携带 {@link CorrelationData} 以触发 Publisher Confirm；
 * 预占延迟消息通过原生 RabbitMQ TTL + DLX 机制超时投递至补偿消费者，不依赖第三方延迟插件。
 */
@Slf4j
@Service
public class BillingMessageProducer {

    private final RabbitTemplate rabbitTemplate;
    private final OneGateProperties properties;

    @Autowired
    public BillingMessageProducer(@Nullable RabbitTemplate rabbitTemplate, OneGateProperties properties) {
        this.rabbitTemplate = rabbitTemplate;
        this.properties = properties;
    }

    /**
     * 投递预占冻结延迟检查消息 (TTL 到期后自动进入 FREEZE_TIMEOUT_QUEUE 执行互斥回滚)。
     */
    public void sendFreezeDelayMessage(BillingMessage message) {
        RabbitTemplate template = requireTemplate();
        long ttlMs = Math.max(1000L, properties.getFreezeDelayMs());
        CorrelationData correlationData = new CorrelationData("freeze:" + message.getRequestId());
        MessagePostProcessor postProcessor = amqpMsg -> {
            amqpMsg.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            amqpMsg.getMessageProperties().setExpiration(String.valueOf(ttlMs));
            return amqpMsg;
        };
        try {
            template.convertAndSend(
                    MqConstants.FREEZE_DELAY_EXCHANGE,
                    MqConstants.FREEZE_DELAY_ROUTING_KEY,
                    message,
                    postProcessor,
                    correlationData
            );
            log.debug("Sent freeze delay message: tenantId={}, requestId={}, ttlMs={}",
                    message.getTenantId(), message.getRequestId(), ttlMs);
        } catch (RuntimeException ex) {
            log.error("[MQ_AUDIT] Failed to publish freeze delay message: tenantId={}, requestId={}, exchange={}, routingKey={}, error={}",
                    message.getTenantId(), message.getRequestId(),
                    MqConstants.FREEZE_DELAY_EXCHANGE, MqConstants.FREEZE_DELAY_ROUTING_KEY,
                    ex.getMessage(), ex);
            throw ex;
        }
    }

    /**
     * 投递最终计费账单消息至主计费交换机 (经 Publisher Confirm 确认到达 Broker)。
     */
    public void sendBillingMessage(BillingMessage message) {
        RabbitTemplate template = requireTemplate();
        CorrelationData correlationData = new CorrelationData("billing:" + message.getRequestId());
        MessagePostProcessor postProcessor = amqpMsg -> {
            amqpMsg.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            return amqpMsg;
        };
        try {
            template.convertAndSend(
                    MqConstants.BILLING_EXCHANGE,
                    MqConstants.BILLING_ROUTING_KEY,
                    message,
                    postProcessor,
                    correlationData
            );
            log.debug("Sent billing settlement message: tenantId={}, requestId={}, totalTokens={}, estimated={}",
                    message.getTenantId(), message.getRequestId(), message.getTotalTokens(), message.getEstimated());
        } catch (RuntimeException ex) {
            log.error("[MQ_AUDIT] Failed to publish billing settlement message: tenantId={}, requestId={}, exchange={}, routingKey={}, totalTokens={}, error={}",
                    message.getTenantId(), message.getRequestId(),
                    MqConstants.BILLING_EXCHANGE, MqConstants.BILLING_ROUTING_KEY,
                    message.getTotalTokens(), ex.getMessage(), ex);
            throw ex;
        }
    }

    private RabbitTemplate requireTemplate() {
        if (rabbitTemplate == null) {
            throw new IllegalStateException("RabbitTemplate is not configured; cannot dispatch RabbitMQ billing message.");
        }
        return rabbitTemplate;
    }
}
