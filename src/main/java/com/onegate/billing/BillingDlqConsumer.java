package com.onegate.billing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.common.constant.MqConstants;
import com.onegate.domain.dto.BillingMessage;
import com.onegate.domain.entity.BillingDeadLetterRecord;
import com.onegate.domain.mapper.BillingDeadLetterMapper;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 计费死信队列消费者：消费路由至 {@code onegate.billing.dlq} 的异常账单，
 * 将原始 JSON 载荷与失败原因持久化至 {@code billing_dead_letter_record} 表等待人工核对或重放补偿。
 */
@Slf4j
@Component
public class BillingDlqConsumer {

    private static final int MAX_ID_LENGTH = 64;
    private static final int MAX_REASON_LENGTH = 512;

    private final BillingDeadLetterMapper billingDeadLetterMapper;
    private final ObjectMapper objectMapper;

    public BillingDlqConsumer(BillingDeadLetterMapper billingDeadLetterMapper, ObjectMapper objectMapper) {
        this.billingDeadLetterMapper = billingDeadLetterMapper;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = MqConstants.BILLING_DLQ)
    public void onDeadLetterMessage(
            Message amqpMessage,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        if (amqpMessage == null) {
            channel.basicAck(deliveryTag, false);
            return;
        }

        String payload = new String(amqpMessage.getBody(), StandardCharsets.UTF_8);
        String requestId = "unknown";
        String tenantId = "unknown";

        try {
            BillingMessage parsed = objectMapper.readValue(payload, BillingMessage.class);
            if (parsed.getRequestId() != null && !parsed.getRequestId().isBlank()) {
                requestId = truncate(parsed.getRequestId(), MAX_ID_LENGTH);
            }
            if (parsed.getTenantId() != null && !parsed.getTenantId().isBlank()) {
                tenantId = truncate(parsed.getTenantId(), MAX_ID_LENGTH);
            }
        } catch (Exception parseEx) {
            log.warn("Unable to parse DLQ payload as BillingMessage, recording raw payload: {}", parseEx.getMessage());
        }

        String errorReason = resolveErrorReason(amqpMessage);

        BillingDeadLetterRecord record = BillingDeadLetterRecord.builder()
                .requestId(requestId)
                .tenantId(tenantId)
                .payload(payload.isEmpty() ? "{}" : payload)
                .errorReason(truncate(errorReason, MAX_REASON_LENGTH))
                .status("PENDING")
                .build();

        try {
            billingDeadLetterMapper.insert(record);
            log.warn("Persisted billing DLQ record: id={}, requestId={}, tenantId={}, reason={}",
                    record.getId(), requestId, tenantId, record.getErrorReason());
            channel.basicAck(deliveryTag, false);
        } catch (Exception dbEx) {
            log.error("[DLQ_ALERT] CRITICAL: Failed to persist billing DLQ record, requeuing message to prevent data loss! requestId={}, tenantId={}, reason={}, payload={}, error={}",
                    requestId, tenantId, record.getErrorReason(), record.getPayload(), dbEx.getMessage(), dbEx);
            channel.basicNack(deliveryTag, false, true);
        }
    }

    private String resolveErrorReason(Message amqpMessage) {
        if (amqpMessage.getMessageProperties() == null) {
            return "Billing settlement failed and routed to DLQ";
        }
        Map<String, Object> headers = amqpMessage.getMessageProperties().getHeaders();
        if (headers != null) {
            Object explicitReason = headers.get("x-error-reason");
            if (explicitReason != null && !String.valueOf(explicitReason).isBlank()) {
                return String.valueOf(explicitReason);
            }
            Object exceptionMsg = headers.get("x-exception-message");
            if (exceptionMsg != null && !String.valueOf(exceptionMsg).isBlank()) {
                return String.valueOf(exceptionMsg);
            }
            Object xDeath = headers.get("x-death");
            if (xDeath != null) {
                return "Dead-lettered from billing queue: " + xDeath;
            }
        }
        return "Billing settlement NACKed and routed to DLQ";
    }

    private String truncate(String val, int maxLen) {
        if (val == null) {
            return "";
        }
        return val.length() <= maxLen ? val : val.substring(0, maxLen);
    }
}
