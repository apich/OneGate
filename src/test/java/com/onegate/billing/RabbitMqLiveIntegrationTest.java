package com.onegate.billing;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.auth.TenantAuthCacheService;
import com.onegate.common.constant.HeaderConstants;
import com.onegate.common.constant.MqConstants;
import com.onegate.common.constant.RedisKeys;
import com.onegate.domain.dto.BillingMessage;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.entity.BillingDeadLetterRecord;
import com.onegate.domain.entity.LlmChannelKey;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import com.onegate.domain.mapper.BillingDeadLetterMapper;
import com.onegate.domain.mapper.LlmChannelKeyMapper;
import com.onegate.domain.mapper.TenantAccountMapper;
import com.onegate.router.LlmChannelRouter;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.mockito.Mockito;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;

/**
 * 基于真实 RabbitMQ 3.12 容器的全链路 AMQP 端到端集成测试。
 * <p>
 * 仅通过 {@code Socket("127.0.0.1", 5672)} 在线探活，未启动本地 RabbitMQ 时由 JUnit 5 {@link EnabledIf}
 * 优雅跳过，不影响离线构建；Broker 在线时验证真实拓扑声明、Confirm/Returns、异步落库、死信持久化、
 * 瞬时故障指数退避重试及原生 TTL+DLX 预占冻结延迟回滚。
 */
@Tag("mq")
@EnabledIf("isRabbitMqReachable")
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "onegate.mock-enabled=true",
                "onegate.admin-token=onegate-admin-secret",
                "onegate.freeze-delay-ms=1500",
                "onegate.freeze-ttl-seconds=600",
                "spring.rabbitmq.host=localhost",
                "spring.rabbitmq.port=5672",
                "spring.rabbitmq.username=guest",
                "spring.rabbitmq.password=guest"
        }
)
class RabbitMqLiveIntegrationTest {

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    @LocalServerPort
    private int port;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitMqConfig rabbitMqConfig;

    @Autowired
    private BillingMessageProducer billingMessageProducer;

    @MockitoSpyBean
    private BillingSettlementTxService billingSettlementTxService;

    @Autowired
    private QuotaFreezeLuaService quotaFreezeLuaService;

    @Autowired
    private TenantAccountMapper tenantAccountMapper;

    @Autowired
    private LlmChannelKeyMapper channelKeyMapper;

    @Autowired
    private ApiUsageLogMapper apiUsageLogMapper;

    @Autowired
    private BillingDeadLetterMapper billingDeadLetterMapper;

    @Autowired
    private TenantAuthCacheService tenantAuthCacheService;

    @Autowired
    private LlmChannelRouter llmChannelRouter;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(15, TimeUnit.SECONDS)
            .writeTimeout(5, TimeUnit.SECONDS)
            .build();

    private final List<String> createdTenantIds = new ArrayList<>();
    private final List<String> createdApiKeys = new ArrayList<>();
    private final List<Long> createdChannelIds = new ArrayList<>();
    private final List<String> createdRequestIds = new ArrayList<>();

    static boolean isRabbitMqReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", 5672), 400);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    @AfterEach
    void cleanup() {
        Mockito.reset(billingSettlementTxService);

        if (!createdRequestIds.isEmpty()) {
            billingDeadLetterMapper.delete(new LambdaQueryWrapper<BillingDeadLetterRecord>()
                    .in(BillingDeadLetterRecord::getRequestId, createdRequestIds));
        }
        for (Long channelId : createdChannelIds) {
            stringRedisTemplate.delete(RedisKeys.channelFailKey(channelId));
            stringRedisTemplate.delete(RedisKeys.channelCooldownKey(channelId));
            channelKeyMapper.deleteById(channelId);
        }
        for (String apiKey : createdApiKeys) {
            tenantAuthCacheService.evictLocalCache(apiKey);
            stringRedisTemplate.delete(RedisKeys.authKey(apiKey));
            stringRedisTemplate.delete("onegate:lock:auth:" + apiKey);
        }
        for (String tenantId : createdTenantIds) {
            apiUsageLogMapper.delete(new LambdaQueryWrapper<ApiUsageLog>()
                    .eq(ApiUsageLog::getTenantId, tenantId));
            billingDeadLetterMapper.delete(new LambdaQueryWrapper<BillingDeadLetterRecord>()
                    .eq(BillingDeadLetterRecord::getTenantId, tenantId));
            for (String reqId : createdRequestIds) {
                stringRedisTemplate.delete(RedisKeys.quotaFreezeKey(tenantId, reqId));
                stringRedisTemplate.delete(RedisKeys.quotaSeenKey(tenantId, reqId));
            }
            stringRedisTemplate.delete(RedisKeys.quotaAvailKey(tenantId));
            stringRedisTemplate.delete(RedisKeys.rateLimitRpmKey(tenantId));
            stringRedisTemplate.delete(RedisKeys.rateLimitTpmKey(tenantId));
            stringRedisTemplate.delete("onegate:lock:quota:init:" + tenantId);
            TenantAccount account = tenantAccountMapper.selectByTenantId(tenantId);
            if (account != null) {
                tenantAccountMapper.deleteById(account.getId());
            }
        }
        createdChannelIds.clear();
        createdApiKeys.clear();
        createdTenantIds.clear();
        createdRequestIds.clear();
        llmChannelRouter.evictAll();
    }

    @Test
    void shouldDeclareRealTopologyAndTriggerPublisherConfirmsAndReturns() throws Exception {
        String suffix = shortId();
        String[] exchanges = {
                MqConstants.BILLING_EXCHANGE,
                MqConstants.BILLING_DLX,
                MqConstants.FREEZE_DELAY_EXCHANGE,
                MqConstants.FREEZE_TIMEOUT_EXCHANGE
        };
        String[] queues = {
                MqConstants.BILLING_QUEUE,
                MqConstants.BILLING_DLQ,
                MqConstants.FREEZE_DELAY_QUEUE,
                MqConstants.FREEZE_TIMEOUT_QUEUE
        };

        rabbitTemplate.execute(channel -> {
            for (String ex : exchanges) {
                channel.exchangeDeclarePassive(ex);
            }
            for (String q : queues) {
                channel.queueDeclarePassive(q);
            }
            return null;
        });

        for (String q : queues) {
            QueueInformation info = amqpAdmin.getQueueInfo(q);
            assertThat(info).as("Queue %s should exist in RabbitMQ", q).isNotNull();
            assertThat(info.getName()).isEqualTo(q);
        }

        // 发送一条向不存在 routingKey 的 mandatory 消息，验证 Confirm ACK 与 Return 回调双递增
        long ackBefore = rabbitMqConfig.getConfirmAckCount().get();
        long returnBefore = rabbitMqConfig.getReturnedMessageCount().get();
        String probeReqId = "req-mq-probe-" + suffix;
        BillingMessage probeMsg = buildBillingMessage("t_mq_probe_" + suffix, probeReqId, 1, "probe");

        rabbitTemplate.convertAndSend(
                MqConstants.BILLING_EXCHANGE,
                "unroutable.routing.key." + suffix,
                probeMsg,
                new CorrelationData("probe:" + suffix)
        );

        awaitUntil(() -> rabbitMqConfig.getConfirmAckCount().get() >= ackBefore + 1
                && rabbitMqConfig.getReturnedMessageCount().get() >= returnBefore + 1, 5_000L);

        long ackAfter = rabbitMqConfig.getConfirmAckCount().get();
        long returnAfter = rabbitMqConfig.getReturnedMessageCount().get();
        assertThat(ackAfter).isGreaterThanOrEqualTo(ackBefore + 1);
        assertThat(returnAfter).isGreaterThanOrEqualTo(returnBefore + 1);

        System.out.printf("[MQ-1][Topology+Confirm/Returns] declaredExchanges=%d, declaredQueues=%d, "
                        + "confirmAck=%d->%d, returned=%d->%d%n",
                exchanges.length, queues.length, ackBefore, ackAfter, returnBefore, returnAfter);
    }

    @Test
    void shouldSettleGatewayChatViaRealRabbitMqAndIgnoreDuplicateReplayIdempotently() throws Exception {
        String suffix = shortId();
        String tenantId = "t_mq_e2e_" + suffix;
        String apiKey = "sk-mq-e2e-" + suffix;
        String model = "mq-e2e-ds-" + suffix;
        long initialBalance = 10_000L;
        insertTenant(tenantId, apiKey, initialBalance);
        LlmChannelKey channel = insertMockChannel(model, "mq-ds-primary-" + suffix, 1);

        String reqId = "req-mq-e2e-" + suffix;
        createdRequestIds.add(reqId);

        long ackBefore = rabbitMqConfig.getConfirmAckCount().get();
        Request request = new Request.Builder()
                .url("http://127.0.0.1:" + port + "/v1/chat/completions")
                .header(HeaderConstants.AUTHORIZATION, "Bearer " + apiKey)
                .header(HeaderConstants.REQUEST_ID_HEADER, reqId)
                .post(RequestBody.create(chatBody(model, "Test RabbitMQ async settlement", false, 128), JSON))
                .build();

        int actualTotalTokens;
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(200);
            assertThat(response.header(HeaderConstants.PROVIDER_HEADER)).isEqualTo("DEEPSEEK");
            JsonNode root = objectMapper.readTree(response.body().string());
            actualTotalTokens = root.at("/usage/total_tokens").asInt();
            assertThat(actualTotalTokens).isGreaterThan(0);
        }

        long expectedBalance = initialBalance - actualTotalTokens;
        awaitUntil(() -> {
            Long cnt = apiUsageLogMapper.selectCount(new LambdaQueryWrapper<ApiUsageLog>()
                    .eq(ApiUsageLog::getTenantId, tenantId)
                    .eq(ApiUsageLog::getRequestId, reqId));
            return cnt != null && cnt == 1L;
        }, 8_000L);

        assertThat(rabbitMqConfig.getConfirmAckCount().get()).isGreaterThanOrEqualTo(ackBefore + 1);
        Long redisAvail = quotaFreezeLuaService.getAvailableQuota(tenantId);
        TenantAccount settledAccount = tenantAccountMapper.selectByTenantId(tenantId);
        assertThat(redisAvail).isEqualTo(expectedBalance);
        assertThat(settledAccount.getBalance()).isEqualTo(expectedBalance);

        // 重放完全相同的 BillingMessage 到真实交换机，验证 uk_tenant_request 幂等跳过、余额不再扣减且不进 DLQ
        BillingMessage replayMsg = BillingMessage.builder()
                .requestId(reqId)
                .tenantId(tenantId)
                .modelName(model)
                .provider("DEEPSEEK")
                .channelId(channel.getId())
                .estimatedTokens(actualTotalTokens)
                .promptTokens(actualTotalTokens / 2)
                .completionTokens(actualTotalTokens - actualTotalTokens / 2)
                .totalTokens(actualTotalTokens)
                .costMs(20)
                .finishReason("stop")
                .estimated(false)
                .requestTime(LocalDateTime.now())
                .build();

        long ackBeforeReplay = rabbitMqConfig.getConfirmAckCount().get();
        billingMessageProducer.sendBillingMessage(replayMsg);
        awaitUntil(() -> rabbitMqConfig.getConfirmAckCount().get() >= ackBeforeReplay + 1, 5_000L);
        Thread.sleep(400L);

        Long afterReplayLogCount = apiUsageLogMapper.selectCount(new LambdaQueryWrapper<ApiUsageLog>()
                .eq(ApiUsageLog::getTenantId, tenantId)
                .eq(ApiUsageLog::getRequestId, reqId));
        Long dlqCount = billingDeadLetterMapper.selectCount(new LambdaQueryWrapper<BillingDeadLetterRecord>()
                .eq(BillingDeadLetterRecord::getTenantId, tenantId)
                .eq(BillingDeadLetterRecord::getRequestId, reqId));
        assertThat(afterReplayLogCount).isEqualTo(1L);
        assertThat(tenantAccountMapper.selectByTenantId(tenantId).getBalance()).isEqualTo(expectedBalance);
        assertThat(dlqCount).isEqualTo(0L);

        // 等待 1.5s freeze-delay 到期确保 FREEZE_TIMEOUT_QUEUE 也幂等消化
        Thread.sleep(1_700L);
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isEqualTo(expectedBalance);

        System.out.printf("[MQ-2][Gateway->RabbitMQ->MySQL + Replay Idempotency] totalTokens=%d, "
                        + "redisAvail=%d, mysqlBalance=%d, logsAfterReplay=%d, dlqCount=%d%n",
                actualTotalTokens, redisAvail, settledAccount.getBalance(), afterReplayLogCount, dlqCount);
    }

    @Test
    void shouldRouteDeterministicInsufficientBalanceFailureToDlqAndPersistRecord() throws Exception {
        String suffix = shortId();
        String tenantId = "t_mq_det_dlq_" + suffix;
        String apiKey = "sk-mq-det-dlq-" + suffix;
        long initialBalance = 10L;
        insertTenant(tenantId, apiKey, initialBalance);

        String reqId = "req-mq-det-dlq-" + suffix;
        createdRequestIds.add(reqId);
        BillingMessage overCostMsg = buildBillingMessage(tenantId, reqId, 500, "stop");

        long ackBefore = rabbitMqConfig.getConfirmAckCount().get();
        billingMessageProducer.sendBillingMessage(overCostMsg);
        awaitUntil(() -> rabbitMqConfig.getConfirmAckCount().get() >= ackBefore + 1, 5_000L);

        awaitUntil(() -> {
            Long cnt = billingDeadLetterMapper.selectCount(new LambdaQueryWrapper<BillingDeadLetterRecord>()
                    .eq(BillingDeadLetterRecord::getRequestId, reqId));
            return cnt != null && cnt == 1L;
        }, 8_000L);

        List<BillingDeadLetterRecord> dlqRecords = billingDeadLetterMapper.selectList(
                new LambdaQueryWrapper<BillingDeadLetterRecord>()
                        .eq(BillingDeadLetterRecord::getRequestId, reqId));
        assertThat(dlqRecords).hasSize(1);
        BillingDeadLetterRecord record = dlqRecords.get(0);
        assertThat(record.getTenantId()).isEqualTo(tenantId);
        assertThat(record.getStatus()).isEqualTo("PENDING");
        assertThat(record.getPayload()).contains(reqId).contains("\"totalTokens\":500");
        assertThat(record.getErrorReason()).isNotBlank();

        Long usageCount = apiUsageLogMapper.selectCount(new LambdaQueryWrapper<ApiUsageLog>()
                .eq(ApiUsageLog::getTenantId, tenantId)
                .eq(ApiUsageLog::getRequestId, reqId));
        assertThat(usageCount).isEqualTo(0L);
        assertThat(tenantAccountMapper.selectByTenantId(tenantId).getBalance()).isEqualTo(initialBalance);

        System.out.printf("[MQ-3][Deterministic NACK->BILLING_DLQ->DB] requestId=%s, status=%s, "
                        + "mysqlBalance=%d, usageLogs=%d, reason=%s%n",
                record.getRequestId(), record.getStatus(), tenantAccountMapper.selectByTenantId(tenantId).getBalance(),
                usageCount, abbreviate(record.getErrorReason(), 120));
    }

    @Test
    void shouldRetryTransientErrorsWithBackoffAndRecoverOrRouteExhaustedToDlq() throws Exception {
        String suffix = shortId();

        // 子场景 A: 前 2 次抛瞬时异常，第 3 次放行成功结算（对应 max-attempts=3）
        String tenantA = "t_mq_retry_ok_" + suffix;
        String apiKeyA = "sk-mq-retry-ok-" + suffix;
        String reqA = "req-mq-retry-ok-" + suffix;
        insertTenant(tenantA, apiKeyA, 2_000L);
        createdRequestIds.add(reqA);
        BillingMessage msgA = buildBillingMessage(tenantA, reqA, 150, "stop");

        AtomicInteger attemptsA = new AtomicInteger(0);
        doAnswer(invocation -> {
            int attempt = attemptsA.incrementAndGet();
            if (attempt <= 2) {
                throw new TransientDataAccessResourceException("Simulated transient DB glitch #" + attempt);
            }
            return invocation.callRealMethod();
        }).when(billingSettlementTxService)
                .settleAtomically(argThat(m -> m != null && reqA.equals(m.getRequestId())));

        // 子场景 B: 连续 3+ 次抛瞬时异常，重试耗尽后自动 NACK 不重回队列并路由至 DLQ 落表
        String tenantB = "t_mq_retry_ex_" + suffix;
        String apiKeyB = "sk-mq-retry-ex-" + suffix;
        String reqB = "req-mq-retry-ex-" + suffix;
        insertTenant(tenantB, apiKeyB, 2_000L);
        createdRequestIds.add(reqB);
        BillingMessage msgB = buildBillingMessage(tenantB, reqB, 180, "stop");

        AtomicInteger attemptsB = new AtomicInteger(0);
        doAnswer(invocation -> {
            int attempt = attemptsB.incrementAndGet();
            throw new TransientDataAccessResourceException("Simulated persistent DB outage #" + attempt);
        }).when(billingSettlementTxService)
                .settleAtomically(argThat(m -> m != null && reqB.equals(m.getRequestId())));

        try {
            long startNanos = System.nanoTime();
            billingMessageProducer.sendBillingMessage(msgA);

            awaitUntil(() -> {
                Long cnt = apiUsageLogMapper.selectCount(new LambdaQueryWrapper<ApiUsageLog>()
                        .eq(ApiUsageLog::getTenantId, tenantA)
                        .eq(ApiUsageLog::getRequestId, reqA));
                return cnt != null && cnt == 1L;
            }, 10_000L);
            long recoverElapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

            assertThat(attemptsA.get()).isEqualTo(3);
            assertThat(recoverElapsedMs).isGreaterThanOrEqualTo(1_200L); // 500ms + 1000ms backoff
            assertThat(tenantAccountMapper.selectByTenantId(tenantA).getBalance()).isEqualTo(1_850L);
            assertThat(billingDeadLetterMapper.selectCount(new LambdaQueryWrapper<BillingDeadLetterRecord>()
                    .eq(BillingDeadLetterRecord::getRequestId, reqA))).isEqualTo(0L);

            long exhaustStartNanos = System.nanoTime();
            billingMessageProducer.sendBillingMessage(msgB);

            awaitUntil(() -> {
                Long cnt = billingDeadLetterMapper.selectCount(new LambdaQueryWrapper<BillingDeadLetterRecord>()
                        .eq(BillingDeadLetterRecord::getRequestId, reqB));
                return cnt != null && cnt == 1L;
            }, 10_000L);
            long exhaustElapsedMs = (System.nanoTime() - exhaustStartNanos) / 1_000_000L;

            assertThat(attemptsB.get()).isEqualTo(3);
            assertThat(exhaustElapsedMs).isGreaterThanOrEqualTo(1_200L);
            assertThat(apiUsageLogMapper.selectCount(new LambdaQueryWrapper<ApiUsageLog>()
                    .eq(ApiUsageLog::getTenantId, tenantB)
                    .eq(ApiUsageLog::getRequestId, reqB))).isEqualTo(0L);
            assertThat(tenantAccountMapper.selectByTenantId(tenantB).getBalance()).isEqualTo(2_000L);

            List<BillingDeadLetterRecord> dlqB = billingDeadLetterMapper.selectList(
                    new LambdaQueryWrapper<BillingDeadLetterRecord>()
                            .eq(BillingDeadLetterRecord::getRequestId, reqB));
            assertThat(dlqB).hasSize(1);
            assertThat(dlqB.get(0).getStatus()).isEqualTo("PENDING");
            assertThat(dlqB.get(0).getTenantId()).isEqualTo(tenantB);

            System.out.printf("[MQ-4][Spring Retry Backoff] recoverAttempts=%d (elapsed=%d ms, balanceA=%d), "
                            + "exhaustAttempts=%d (elapsed=%d ms, dlqStatus=%s, reasonB=%s)%n",
                    attemptsA.get(), recoverElapsedMs, tenantAccountMapper.selectByTenantId(tenantA).getBalance(),
                    attemptsB.get(), exhaustElapsedMs, dlqB.get(0).getStatus(),
                    abbreviate(dlqB.get(0).getErrorReason(), 100));
        } finally {
            Mockito.reset(billingSettlementTxService);
        }
    }

    @Test
    void shouldRollbackOrphanFreezeAndSkipAlreadySettledOnNativeTtlDlxExpiration() throws Exception {
        String suffix = shortId();

        // 子场景 A: 孤儿冻结补偿 — 冻结 300 后发送 1500ms TTL 延迟消息且不结算，到期自动回滚至 1000
        String tenantOrphan = "t_mq_ttl_orphan_" + suffix;
        String apiKeyOrphan = "sk-mq-ttl-orphan-" + suffix;
        String reqOrphan = "req-mq-ttl-orphan-" + suffix;
        insertTenant(tenantOrphan, apiKeyOrphan, 1_000L);
        createdRequestIds.add(reqOrphan);

        long afterFreezeOrphan = quotaFreezeLuaService.freezeQuotaOrThrow(tenantOrphan, reqOrphan, 300);
        assertThat(afterFreezeOrphan).isEqualTo(700L);
        assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantOrphan, reqOrphan))).isTrue();

        long orphanStartNanos = System.nanoTime();
        billingMessageProducer.sendFreezeDelayMessage(buildBillingMessage(tenantOrphan, reqOrphan, 300, "freeze_init"));

        awaitUntil(() -> {
            Long avail = quotaFreezeLuaService.getAvailableQuota(tenantOrphan);
            boolean freezeExists = Boolean.TRUE.equals(
                    stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantOrphan, reqOrphan)));
            return avail != null && avail == 1_000L && !freezeExists;
        }, 8_000L);
        long orphanRollbackMs = (System.nanoTime() - orphanStartNanos) / 1_000_000L;
        assertThat(orphanRollbackMs).isGreaterThanOrEqualTo(1_200L);

        // 子场景 B: 已正常核销扣费 — 延迟消息到期触发 FreezeTimeoutConsumer 应幂等跳过，余额维持 880
        String tenantSettled = "t_mq_ttl_settled_" + suffix;
        String apiKeySettled = "sk-mq-ttl-settled-" + suffix;
        String reqSettled = "req-mq-ttl-settled-" + suffix;
        insertTenant(tenantSettled, apiKeySettled, 1_000L);
        createdRequestIds.add(reqSettled);

        long afterFreezeSettled = quotaFreezeLuaService.freezeQuotaOrThrow(tenantSettled, reqSettled, 300);
        assertThat(afterFreezeSettled).isEqualTo(700L);
        billingMessageProducer.sendFreezeDelayMessage(
                buildBillingMessage(tenantSettled, reqSettled, 300, "freeze_init"));

        QuotaFreezeLuaService.SettleResult settleResult =
                quotaFreezeLuaService.settleQuota(tenantSettled, reqSettled, 120);
        assertThat(settleResult.isApplied()).isTrue();
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantSettled)).isEqualTo(880L);

        // 等待 1500ms TTL 到期路由并经 FreezeTimeoutConsumer 消费完毕
        Thread.sleep(2_200L);
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantSettled)).isEqualTo(880L);
        assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantSettled, reqSettled))).isFalse();

        awaitUntil(() -> {
            QueueInformation delayInfo = amqpAdmin.getQueueInfo(MqConstants.FREEZE_DELAY_QUEUE);
            QueueInformation timeoutInfo = amqpAdmin.getQueueInfo(MqConstants.FREEZE_TIMEOUT_QUEUE);
            return delayInfo != null && delayInfo.getMessageCount() == 0
                    && timeoutInfo != null && timeoutInfo.getMessageCount() == 0;
        }, 5_000L);

        System.out.printf("[MQ-5][Native TTL+DLX Freeze Compensation] orphanFrozen=300 -> refundedTo=%d in %d ms; "
                        + "settledAfterTtl=%d (idempotent skip verified)%n",
                quotaFreezeLuaService.getAvailableQuota(tenantOrphan),
                orphanRollbackMs,
                quotaFreezeLuaService.getAvailableQuota(tenantSettled));
    }

    private void insertTenant(String tenantId, String apiKey, long balance) {
        TenantAccount account = TenantAccount.builder()
                .tenantId(tenantId)
                .tenantName("RabbitMQ Live Test " + tenantId)
                .apiKey(apiKey)
                .balance(balance)
                .frozenTokens(0L)
                .rpmLimit(5000)
                .tpmLimit(5_000_000)
                .status(1)
                .version(0)
                .build();
        tenantAccountMapper.insert(account);
        createdTenantIds.add(tenantId);
        createdApiKeys.add(apiKey);
    }

    private LlmChannelKey insertMockChannel(String modelName, String channelName, int priority) {
        LlmChannelKey channel = LlmChannelKey.builder()
                .channelName(channelName)
                .provider("DEEPSEEK")
                .modelName(modelName)
                .baseUrl("http://127.0.0.1:" + port + "/mock-llm/primary")
                .secretKey("mock-mq-secret")
                .weight(100)
                .priority(priority)
                .status(1)
                .build();
        channelKeyMapper.insert(channel);
        createdChannelIds.add(channel.getId());
        llmChannelRouter.evictModelCache(modelName);
        return channel;
    }

    private String chatBody(String model, String content, boolean stream, int maxTokens) throws Exception {
        Map<String, Object> body = Map.of(
                "model", model,
                "stream", stream,
                "max_tokens", maxTokens,
                "messages", List.of(Map.of("role", "user", "content", content))
        );
        return objectMapper.writeValueAsString(body);
    }

    private BillingMessage buildBillingMessage(String tenantId, String requestId, int totalTokens, String finishReason) {
        int prompt = totalTokens / 2;
        int completion = totalTokens - prompt;
        return BillingMessage.builder()
                .requestId(requestId)
                .tenantId(tenantId)
                .modelName("deepseek-chat")
                .provider("DEEPSEEK")
                .channelId(1L)
                .estimatedTokens(totalTokens + 50)
                .promptTokens(prompt)
                .completionTokens(completion)
                .totalTokens(totalTokens)
                .costMs(100)
                .finishReason(finishReason)
                .estimated(false)
                .requestTime(LocalDateTime.now())
                .build();
    }

    private void awaitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50L);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private String abbreviate(String text, int maxLen) {
        if (text == null) {
            return "";
        }
        String clean = text.replace('\r', ' ').replace('\n', ' ').trim();
        return clean.length() <= maxLen ? clean : clean.substring(0, maxLen) + "...";
    }

    private String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }
}
