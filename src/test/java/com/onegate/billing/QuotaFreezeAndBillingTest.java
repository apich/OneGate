package com.onegate.billing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.common.constant.MqConstants;
import com.onegate.common.constant.RedisKeys;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.domain.dto.BillingMessage;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.entity.BillingDeadLetterRecord;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import com.onegate.domain.mapper.BillingDeadLetterMapper;
import com.onegate.domain.mapper.TenantAccountMapper;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * 模块 3 真实环境集成测试：
 * 直连本地 MySQL 9.0.1 + Redis 7.0.15，仅用 {@code @MockitoBean RabbitTemplate} 隔离未启动的 5672 Broker。
 */
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration"
})
class QuotaFreezeAndBillingTest {

    @MockitoBean
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private QuotaFreezeLuaService quotaFreezeLuaService;

    @Autowired
    private BillingMessageProducer billingMessageProducer;

    @Autowired
    private BillingSettlementTxService billingSettlementTxService;

    @Autowired
    private BillingSettleConsumer billingSettleConsumer;

    @Autowired
    private FreezeTimeoutConsumer freezeTimeoutConsumer;

    @Autowired
    private BillingDlqConsumer billingDlqConsumer;

    @Autowired
    private RabbitMqConfig rabbitMqConfig;

    @Autowired
    private TenantAccountMapper tenantAccountMapper;

    @Autowired
    private ApiUsageLogMapper apiUsageLogMapper;

    @Autowired
    private BillingDeadLetterMapper billingDeadLetterMapper;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private final List<String> createdTenantIds = new ArrayList<>();
    private final List<String> createdRequestIds = new ArrayList<>();
    private final List<String> createdRedisKeys = new ArrayList<>();

    @AfterEach
    void cleanup() {
        for (String requestId : createdRequestIds) {
            billingDeadLetterMapper.delete(
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<BillingDeadLetterRecord>()
                            .eq(BillingDeadLetterRecord::getRequestId, requestId)
            );
        }
        for (String tenantId : createdTenantIds) {
            apiUsageLogMapper.delete(
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<ApiUsageLog>()
                            .eq(ApiUsageLog::getTenantId, tenantId)
            );
            billingDeadLetterMapper.delete(
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<BillingDeadLetterRecord>()
                            .eq(BillingDeadLetterRecord::getTenantId, tenantId)
            );
            for (String reqId : createdRequestIds) {
                stringRedisTemplate.delete(RedisKeys.quotaFreezeKey(tenantId, reqId));
                stringRedisTemplate.delete(RedisKeys.quotaSeenKey(tenantId, reqId));
            }
            stringRedisTemplate.delete(RedisKeys.quotaAvailKey(tenantId));
            stringRedisTemplate.delete("onegate:lock:quota:init:" + tenantId);
            TenantAccount account = tenantAccountMapper.selectByTenantId(tenantId);
            if (account != null) {
                tenantAccountMapper.deleteById(account.getId());
            }
        }
        for (String key : createdRedisKeys) {
            stringRedisTemplate.delete(key);
        }
        createdTenantIds.clear();
        createdRequestIds.clear();
        createdRedisKeys.clear();
    }

    @Test
    void shouldPreventOverFreezeUnder50ConcurrentRequestsWithColdStartInit() throws Exception {
        String tenantId = "t_bill_conc_" + shortId();
        insertTenant(tenantId, 3000L);
        stringRedisTemplate.delete(RedisKeys.quotaAvailKey(tenantId));

        int totalThreads = 50;
        int freezeAmount = 300;
        ExecutorService pool = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch ready = new CountDownLatch(totalThreads);
        CountDownLatch startGun = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger insufficientCount = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < totalThreads; i++) {
                String reqId = "req-freeze-" + tenantId + "-" + i;
                createdRequestIds.add(reqId);
                createdRedisKeys.add(RedisKeys.quotaFreezeKey(tenantId, reqId));
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    startGun.await(5, TimeUnit.SECONDS);
                    try {
                        quotaFreezeLuaService.freezeQuotaOrThrow(tenantId, reqId, freezeAmount);
                        successCount.incrementAndGet();
                    } catch (GatewayException ex) {
                        if (ex.getErrorCode() == ErrorCode.INSUFFICIENT_QUOTA) {
                            insufficientCount.incrementAndGet();
                        } else {
                            throw ex;
                        }
                    }
                    return null;
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            startGun.countDown();
            for (Future<?> future : futures) {
                future.get(15, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(successCount.get()).isEqualTo(10);
        assertThat(insufficientCount.get()).isEqualTo(40);
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantId)).isEqualTo(0L);
        System.out.printf("[TEST] 50-thread quota freeze: success=%d, insufficient(402)=%d, finalAvail=%d%n",
                successCount.get(), insufficientCount.get(), quotaFreezeLuaService.getAvailableQuota(tenantId));
    }

    @Test
    void shouldGuaranteeMutualExclusionBetweenSettleAndTimeoutRollbackUnderConcurrency() throws Exception {
        // 1. 顺序互斥验证: 先正常核销 -> 后超时回滚必须幂等跳过
        String tenantIdSeq = "t_bill_seq_" + shortId();
        insertTenant(tenantIdSeq, 2000L);
        String reqSeq1 = "req-seq-settle-first-" + shortId();
        createdRequestIds.add(reqSeq1);
        createdRedisKeys.add(RedisKeys.quotaFreezeKey(tenantIdSeq, reqSeq1));

        long availAfterFreeze = quotaFreezeLuaService.freezeQuotaOrThrow(tenantIdSeq, reqSeq1, 500);
        assertThat(availAfterFreeze).isEqualTo(1500L);

        QuotaFreezeLuaService.SettleResult settleRes = quotaFreezeLuaService.settleQuota(tenantIdSeq, reqSeq1, 120);
        assertThat(settleRes.isApplied()).isTrue();
        assertThat(settleRes.getRefundedDelta()).isEqualTo(380L);
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantIdSeq)).isEqualTo(1880L);
        assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantIdSeq, reqSeq1))).isFalse();

        Channel mockTimeoutChannel = mock(Channel.class);
        BillingMessage timeoutMsg = BillingMessage.builder()
                .tenantId(tenantIdSeq)
                .requestId(reqSeq1)
                .estimatedTokens(500)
                .build();
        freezeTimeoutConsumer.onFreezeTimeout(timeoutMsg, mockTimeoutChannel, 1L);
        verify(mockTimeoutChannel).basicAck(1L, false);
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantIdSeq)).isEqualTo(1880L);

        // 2. 50 线程并发竞态互斥验证: 25 个结算线程 + 25 个超时回滚线程抢占同一个 requestId
        String reqRace = "req-race-" + shortId();
        createdRequestIds.add(reqRace);
        createdRedisKeys.add(RedisKeys.quotaFreezeKey(tenantIdSeq, reqRace));

        long availBeforeRace = quotaFreezeLuaService.freezeQuotaOrThrow(tenantIdSeq, reqRace, 600);
        assertThat(availBeforeRace).isEqualTo(1280L);

        int threads = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        AtomicInteger settleApplied = new AtomicInteger(0);
        AtomicInteger rollbackApplied = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < threads; i++) {
                final boolean doSettle = (i % 2 == 0);
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    startGun.await(5, TimeUnit.SECONDS);
                    if (doSettle) {
                        if (quotaFreezeLuaService.settleQuota(tenantIdSeq, reqRace, 200).isApplied()) {
                            settleApplied.incrementAndGet();
                        }
                    } else {
                        if (quotaFreezeLuaService.rollbackFrozen(tenantIdSeq, reqRace).isApplied()) {
                            rollbackApplied.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            startGun.countDown();
            for (Future<?> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(settleApplied.get() + rollbackApplied.get()).isEqualTo(1);
        long expectedAvail = settleApplied.get() == 1 ? (1280L + 400L) : (1280L + 600L);
        assertThat(quotaFreezeLuaService.getAvailableQuota(tenantIdSeq)).isEqualTo(expectedAvail);
        assertThat(stringRedisTemplate.hasKey(RedisKeys.quotaFreezeKey(tenantIdSeq, reqRace))).isFalse();
        System.out.printf("[TEST] 50-thread settle vs timeout-rollback race: settleApplied=%d, rollbackApplied=%d, avail=%d%n",
                settleApplied.get(), rollbackApplied.get(), quotaFreezeLuaService.getAvailableQuota(tenantIdSeq));
    }

    @Test
    void shouldEnforceIdempotencyAndZeroOverDeductionInSingleTxUnder50Concurrency() throws Exception {
        // 1. uk_request_id 重复投递幂等验证
        String tenantId = "t_bill_tx_" + shortId();
        insertTenant(tenantId, 1000L);
        String dupReqId = "req-dup-" + shortId();
        createdRequestIds.add(dupReqId);

        BillingMessage dupMsg = buildBillingMessage(tenantId, dupReqId, 250, "stop");
        Channel mockChannel = mock(Channel.class);
        billingSettleConsumer.onBillingMessage(dupMsg, mockChannel, 10L);
        billingSettleConsumer.onBillingMessage(dupMsg, mockChannel, 11L);

        verify(mockChannel).basicAck(10L, false);
        verify(mockChannel).basicAck(11L, false);
        assertThat(tenantAccountMapper.selectByTenantId(tenantId).getBalance()).isEqualTo(750L);
        Long logCount = apiUsageLogMapper.selectCount(
                new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<ApiUsageLog>()
                        .eq(ApiUsageLog::getTenantId, tenantId)
                        .eq(ApiUsageLog::getRequestId, dupReqId)
        );
        assertThat(logCount).isEqualTo(1L);

        // 2. 50 线程并发落库抢扣 1000 余额 (每笔 100): 仅 10 笔成功 ACK，40 笔事务回滚流水并 NACK
        String concTenant = "t_bill_db50_" + shortId();
        insertTenant(concTenant, 1000L);
        int threads = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        Channel concChannel = mock(Channel.class);
        AtomicInteger acked = new AtomicInteger(0);
        AtomicInteger nacked = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                String reqId = "req-db50-" + concTenant + "-" + idx;
                createdRequestIds.add(reqId);
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    startGun.await(5, TimeUnit.SECONDS);
                    BillingMessage msg = buildBillingMessage(concTenant, reqId, 100, "stop");
                    boolean settled = false;
                    try {
                        settled = billingSettlementTxService.settleAtomically(msg);
                    } catch (Exception ignored) {
                        // falls through to simulate consumer nack
                    }
                    if (settled) {
                        acked.incrementAndGet();
                    } else {
                        nacked.incrementAndGet();
                    }
                    return null;
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            startGun.countDown();
            for (Future<?> f : futures) {
                f.get(15, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(acked.get()).isEqualTo(10);
        assertThat(nacked.get()).isEqualTo(40);
        assertThat(tenantAccountMapper.selectByTenantId(concTenant).getBalance()).isEqualTo(0L);
        Long settledLogs = apiUsageLogMapper.selectCount(
                new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<ApiUsageLog>()
                        .eq(ApiUsageLog::getTenantId, concTenant)
        );
        assertThat(settledLogs).isEqualTo(10L);
        System.out.printf("[TEST] 50-thread single-tx DB billing: settled=%d, rolledBack=%d, finalBalance=%d%n",
                acked.get(), nacked.get(), tenantAccountMapper.selectByTenantId(concTenant).getBalance());
    }

    @Test
    void shouldRollbackUsageLogOnInsufficientBalanceNackAndPersistToDlq() throws Exception {
        String tenantId = "t_bill_dlq_" + shortId();
        insertTenant(tenantId, 50L);
        String reqId = "req-dlq-" + shortId();
        createdRequestIds.add(reqId);

        BillingMessage overCostMsg = buildBillingMessage(tenantId, reqId, 200, "stop");
        Channel settleChannel = mock(Channel.class);
        billingSettleConsumer.onBillingMessage(overCostMsg, settleChannel, 77L);

        // 验证事务回滚: api_usage_log 必须为 0 条, balance 仍为 50, 且执行 basicNack(requeue=false)
        verify(settleChannel).basicNack(77L, false, false);
        assertThat(tenantAccountMapper.selectByTenantId(tenantId).getBalance()).isEqualTo(50L);
        Long usageCount = apiUsageLogMapper.selectCount(
                new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<ApiUsageLog>()
                        .eq(ApiUsageLog::getTenantId, tenantId)
                        .eq(ApiUsageLog::getRequestId, reqId)
        );
        assertThat(usageCount).isEqualTo(0L);

        // 验证 DLQ 消费者将失败载荷持久化至 billing_dead_letter_record
        String jsonPayload = objectMapper.writeValueAsString(overCostMsg);
        MessageProperties props = new MessageProperties();
        props.setHeader("x-error-reason", "Atomic balance deduction failed: balance=50 < actualCost=200");
        Message dlqMessage = new Message(jsonPayload.getBytes(StandardCharsets.UTF_8), props);

        Channel dlqChannel = mock(Channel.class);
        billingDlqConsumer.onDeadLetterMessage(dlqMessage, dlqChannel, 88L);
        verify(dlqChannel).basicAck(88L, false);

        List<BillingDeadLetterRecord> records = billingDeadLetterMapper.selectList(
                new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<BillingDeadLetterRecord>()
                        .eq(BillingDeadLetterRecord::getRequestId, reqId)
        );
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getTenantId()).isEqualTo(tenantId);
        assertThat(records.get(0).getStatus()).isEqualTo("PENDING");
        assertThat(records.get(0).getPayload()).contains(reqId);
        assertThat(records.get(0).getErrorReason()).contains("balance=50");
    }

    @Test
    void shouldDeclareNativeTtlDlxTopologyAndSendCorrelationMessages() {
        Queue delayQueue = rabbitMqConfig.freezeDelayQueue(
                new com.onegate.common.config.OneGateProperties());
        assertThat(delayQueue.getArguments())
                .containsEntry("x-dead-letter-exchange", MqConstants.FREEZE_TIMEOUT_EXCHANGE)
                .containsEntry("x-dead-letter-routing-key", MqConstants.FREEZE_TIMEOUT_ROUTING_KEY)
                .doesNotContainKey("x-message-ttl");

        Queue billingQueue = rabbitMqConfig.billingQueue();
        assertThat(billingQueue.getArguments())
                .containsEntry("x-dead-letter-exchange", MqConstants.BILLING_DLX)
                .containsEntry("x-dead-letter-routing-key", MqConstants.BILLING_DLQ_ROUTING_KEY);

        DirectExchange billingDlx = rabbitMqConfig.billingDlxExchange();
        assertThat(billingDlx.getName()).isEqualTo(MqConstants.BILLING_DLX);

        String reqId = "req-mq-" + shortId();
        BillingMessage msg = buildBillingMessage("t_mq_test", reqId, 128, "stop");
        billingMessageProducer.sendFreezeDelayMessage(msg);
        billingMessageProducer.sendBillingMessage(msg);

        ArgumentCaptor<CorrelationData> corrCaptor = ArgumentCaptor.forClass(CorrelationData.class);
        verify(rabbitTemplate, times(1)).convertAndSend(
                eq(MqConstants.FREEZE_DELAY_EXCHANGE),
                eq(MqConstants.FREEZE_DELAY_ROUTING_KEY),
                eq(msg),
                any(org.springframework.amqp.core.MessagePostProcessor.class),
                corrCaptor.capture()
        );
        assertThat(corrCaptor.getValue().getId()).isEqualTo("freeze:" + reqId);

        verify(rabbitTemplate, times(1)).convertAndSend(
                eq(MqConstants.BILLING_EXCHANGE),
                eq(MqConstants.BILLING_ROUTING_KEY),
                eq(msg),
                any(org.springframework.amqp.core.MessagePostProcessor.class),
                corrCaptor.capture()
        );
        assertThat(corrCaptor.getValue().getId()).isEqualTo("billing:" + reqId);
    }

    private void insertTenant(String tenantId, long balance) {
        TenantAccount account = TenantAccount.builder()
                .tenantId(tenantId)
                .tenantName("Billing Test " + tenantId)
                .apiKey("sk-bill-" + tenantId)
                .balance(balance)
                .frozenTokens(0L)
                .rpmLimit(5000)
                .tpmLimit(5_000_000)
                .status(1)
                .version(0)
                .build();
        tenantAccountMapper.insert(account);
        createdTenantIds.add(tenantId);
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

    private String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }
}
