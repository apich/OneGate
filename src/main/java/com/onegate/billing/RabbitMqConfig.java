package com.onegate.billing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.onegate.common.config.OneGateProperties;
import com.onegate.common.constant.MqConstants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicLong;

/**
 * RabbitMQ 异步计费与预占冻结超时回滚拓扑配置。
 * <p>
 * 1. 主计费链路：{@code BILLING_EXCHANGE -> BILLING_QUEUE -> (NACK requeue=false) -> BILLING_DLX -> BILLING_DLQ}<br>
 * 2. 原生 TTL+DLX 延迟回滚链路：{@code FREEZE_DELAY_EXCHANGE -> FREEZE_DELAY_QUEUE (TTL) -> FREEZE_TIMEOUT_EXCHANGE -> FREEZE_TIMEOUT_QUEUE}<br>
 * 3. 通过 {@link RabbitTemplateCustomizer} 注册 Publisher Confirm 与 Returns 回调，
 *    同时保持拓扑 Bean 与定制器在排除 {@code RabbitAutoConfiguration} 的测试环境下无感知兼容。
 */
@Slf4j
@Configuration
public class RabbitMqConfig {

    @Getter
    private final AtomicLong confirmAckCount = new AtomicLong(0L);

    @Getter
    private final AtomicLong confirmNackCount = new AtomicLong(0L);

    @Getter
    private final AtomicLong returnedMessageCount = new AtomicLong(0L);

    @Bean
    public MessageConverter jackson2JsonMessageConverter(ObjectMapper springObjectMapper) {
        ObjectMapper mqMapper = springObjectMapper.copy();
        mqMapper.registerModule(new JavaTimeModule());
        mqMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(mqMapper);
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.addTrustedPackages("com.onegate.domain.dto", "com.onegate.domain");
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }

    /**
     * With acknowledge-mode=manual, Spring AMQP's default RejectAndDontRequeueRecoverer throws
     * ListenerExecutionFailedException(rejectManual=false), which skips channel.basicReject and leaves
     * retry-exhausted messages stuck unacked. Throwing AmqpRejectAndDontRequeueException(rejectManual=true)
     * ensures BlockingQueueConsumer immediately basicReject(requeue=false) to route into the configured DLX.
     */
    @Bean
    public MessageRecoverer oneGateRejectAndDontRequeueRecoverer() {
        return (message, cause) -> {
            String queue = message != null && message.getMessageProperties() != null
                    ? message.getMessageProperties().getConsumerQueue()
                    : "unknown";
            Throwable root = cause != null && cause.getCause() != null ? cause.getCause() : cause;
            log.warn("RabbitMQ retries exhausted; rejecting message without requeue to trigger DLX: queue={}, error={}",
                    queue, root != null ? root.getMessage() : "unknown", cause);
            throw new AmqpRejectAndDontRequeueException("Retry policy exhausted for queue " + queue, true, cause);
        };
    }

    @Bean
    public RabbitTemplateCustomizer oneGateRabbitTemplateCustomizer(MessageConverter jackson2JsonMessageConverter) {
        return template -> {
            template.setMessageConverter(jackson2JsonMessageConverter);
            template.setMandatory(true);
            template.setConfirmCallback((correlationData, ack, cause) -> {
                String id = correlationData != null ? correlationData.getId() : "unknown";
                if (ack) {
                    confirmAckCount.incrementAndGet();
                    log.debug("RabbitMQ publisher confirm ACK: correlationId={}", id);
                } else {
                    confirmNackCount.incrementAndGet();
                    String returnedReply = (correlationData != null && correlationData.getReturned() != null)
                            ? correlationData.getReturned().getReplyText()
                            : "none";
                    log.error("[MQ_AUDIT] RabbitMQ publisher confirm NACK (message NOT persisted by broker): correlationId={}, cause={}, returnedReply={}",
                            id, cause, returnedReply);
                }
            });
            template.setReturnsCallback(returned -> {
                returnedMessageCount.incrementAndGet();
                String messageId = null;
                String payloadPreview = "";
                if (returned.getMessage() != null) {
                    if (returned.getMessage().getMessageProperties() != null) {
                        messageId = returned.getMessage().getMessageProperties().getMessageId();
                    }
                    byte[] body = returned.getMessage().getBody();
                    if (body != null && body.length > 0) {
                        String raw = new String(body, java.nio.charset.StandardCharsets.UTF_8);
                        payloadPreview = raw.length() > 256 ? raw.substring(0, 256) + "..." : raw;
                    }
                }
                log.error("[MQ_AUDIT] RabbitMQ message returned (unroutable): exchange={}, routingKey={}, replyCode={}, replyText={}, messageId={}, payload={}",
                        returned.getExchange(),
                        returned.getRoutingKey(),
                        returned.getReplyCode(),
                        returned.getReplyText(),
                        messageId,
                        payloadPreview);
            });
        };
    }

    // ==================== 1. 主计费链路 + 死信兜底队列 ====================

    @Bean
    public DirectExchange billingExchange() {
        return ExchangeBuilder.directExchange(MqConstants.BILLING_EXCHANGE)
                .durable(true)
                .build();
    }

    @Bean
    public Queue billingQueue() {
        return QueueBuilder.durable(MqConstants.BILLING_QUEUE)
                .withArgument("x-dead-letter-exchange", MqConstants.BILLING_DLX)
                .withArgument("x-dead-letter-routing-key", MqConstants.BILLING_DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding billingBinding(Queue billingQueue, DirectExchange billingExchange) {
        return BindingBuilder.bind(billingQueue)
                .to(billingExchange)
                .with(MqConstants.BILLING_ROUTING_KEY);
    }

    @Bean
    public DirectExchange billingDlxExchange() {
        return ExchangeBuilder.directExchange(MqConstants.BILLING_DLX)
                .durable(true)
                .build();
    }

    @Bean
    public Queue billingDlq() {
        return QueueBuilder.durable(MqConstants.BILLING_DLQ).build();
    }

    @Bean
    public Binding billingDlqBinding(Queue billingDlq, DirectExchange billingDlxExchange) {
        return BindingBuilder.bind(billingDlq)
                .to(billingDlxExchange)
                .with(MqConstants.BILLING_DLQ_ROUTING_KEY);
    }

    // ==================== 2. 预占冻结原生 TTL + DLX 延迟补偿链路 ====================

    @Bean
    public DirectExchange freezeDelayExchange() {
        return ExchangeBuilder.directExchange(MqConstants.FREEZE_DELAY_EXCHANGE)
                .durable(true)
                .build();
    }

    @Bean
    public Queue freezeDelayQueue(OneGateProperties properties) {
        return QueueBuilder.durable(MqConstants.FREEZE_DELAY_QUEUE)
                .withArgument("x-dead-letter-exchange", MqConstants.FREEZE_TIMEOUT_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", MqConstants.FREEZE_TIMEOUT_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding freezeDelayBinding(Queue freezeDelayQueue, DirectExchange freezeDelayExchange) {
        return BindingBuilder.bind(freezeDelayQueue)
                .to(freezeDelayExchange)
                .with(MqConstants.FREEZE_DELAY_ROUTING_KEY);
    }

    @Bean
    public DirectExchange freezeTimeoutExchange() {
        return ExchangeBuilder.directExchange(MqConstants.FREEZE_TIMEOUT_EXCHANGE)
                .durable(true)
                .build();
    }

    @Bean
    public Queue freezeTimeoutQueue() {
        return QueueBuilder.durable(MqConstants.FREEZE_TIMEOUT_QUEUE).build();
    }

    @Bean
    public Binding freezeTimeoutBinding(Queue freezeTimeoutQueue, DirectExchange freezeTimeoutExchange) {
        return BindingBuilder.bind(freezeTimeoutQueue)
                .to(freezeTimeoutExchange)
                .with(MqConstants.FREEZE_TIMEOUT_ROUTING_KEY);
    }
}
