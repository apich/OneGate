package com.onegate;

import com.onegate.common.config.OneGateProperties;
import com.onegate.common.constant.RedisKeys;
import com.onegate.common.exception.ErrorCode;
import com.onegate.common.exception.GatewayException;
import com.onegate.common.exception.GlobalExceptionHandler;
import com.onegate.domain.dto.UsageStatsResponse;
import com.onegate.domain.entity.ApiUsageLog;
import com.onegate.domain.entity.LlmChannelKey;
import com.onegate.domain.entity.TenantAccount;
import com.onegate.domain.mapper.ApiUsageLogMapper;
import com.onegate.domain.mapper.BillingDeadLetterMapper;
import com.onegate.domain.mapper.LlmChannelKeyMapper;
import com.onegate.domain.mapper.TenantAccountMapper;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration"
})
class Phase1FoundationTest {

    @MockitoBean
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private OneGateProperties properties;

    @Autowired
    private OkHttpClient okHttpClient;

    @Autowired
    private GlobalExceptionHandler exceptionHandler;

    @Autowired
    private TenantAccountMapper tenantAccountMapper;

    @Autowired
    private LlmChannelKeyMapper channelKeyMapper;

    @Autowired
    private ApiUsageLogMapper apiUsageLogMapper;

    @Autowired
    private BillingDeadLetterMapper deadLetterMapper;

    @Test
    void shouldLoadConfigAndRedisHashTagContracts() {
        assertThat(properties.getTtftTimeoutMs()).isEqualTo(3000L);
        assertThat(properties.getPoolSpec("DEEPSEEK").getMaxSize()).isEqualTo(64);
        assertThat(properties.getPoolSpec("UNKNOWN").getCoreSize()).isEqualTo(8);
        assertThat(okHttpClient.dispatcher().getMaxRequestsPerHost()).isEqualTo(64);

        assertThat(RedisKeys.rateLimitRpmKey("t_1001")).isEqualTo("onegate:rl:rpm:{t_1001}");
        assertThat(RedisKeys.quotaAvailKey("t_1001")).isEqualTo("onegate:quota:avail:{t_1001}");
        assertThat(RedisKeys.quotaFreezeKey("t_1001", "req-9")).isEqualTo("onegate:quota:freeze:{t_1001}:req-9");
    }

    @Test
    void shouldFormatOpenAiStyleErrorResponses() {
        ResponseEntity<GlobalExceptionHandler.OpenAiErrorResponse> resp =
                exceptionHandler.handleGatewayException(new GatewayException(ErrorCode.RATE_LIMIT_EXCEEDED));
        assertThat(resp.getStatusCode().value()).isEqualTo(429);
        assertThat(resp.getBody()).isNotNull();
        assertThat(resp.getBody().getError().getCode()).isEqualTo("rate_limit_exceeded");
        assertThat(resp.getBody().getError().getType()).isEqualTo("rate_limit_error");
        assertThat(resp.getBody().getError().getParam()).isNull();
    }

    @Test
    @Transactional
    void shouldVerifyMappersAgainstOneGateSchema() {
        TenantAccount demo = tenantAccountMapper.selectByApiKey("sk-onegate-demo-1001");
        assertThat(demo).isNotNull();
        assertThat(demo.getTenantId()).isEqualTo("t_1001");
        assertThat(demo.getBalance()).isEqualTo(1_000_000L);

        int deducted = tenantAccountMapper.deductBalanceAtomically("t_1001", 250L);
        assertThat(deducted).isEqualTo(1);
        assertThat(tenantAccountMapper.selectByTenantId("t_1001").getBalance()).isEqualTo(999_750L);

        int overDeducted = tenantAccountMapper.deductBalanceAtomically("t_1001", 99_999_999L);
        assertThat(overDeducted).isEqualTo(0);

        List<LlmChannelKey> channels = channelKeyMapper.selectActiveByModel("deepseek-chat");
        assertThat(channels).hasSize(2);
        assertThat(channels.get(0).getPriority()).isEqualTo(1);

        LocalDateTime t1 = LocalDateTime.of(2026, 11, 13, 12, 0, 0);
        ApiUsageLog logEntry = ApiUsageLog.builder()
                .requestId("req-phase1-test-1")
                .tenantId("t_1001")
                .modelName("deepseek-chat")
                .provider("DEEPSEEK")
                .channelId(channels.get(0).getId())
                .promptTokens(100)
                .completionTokens(50)
                .totalTokens(150)
                .costMs(120)
                .finishReason("stop")
                .estimated(0)
                .createTime(t1)
                .build();
        assertThat(apiUsageLogMapper.insertIgnore(logEntry)).isEqualTo(1);
        assertThat(apiUsageLogMapper.insertIgnore(logEntry)).isEqualTo(0);

        UsageStatsResponse stats = apiUsageLogMapper.selectCoveringStats(
                "t_1001", "deepseek-chat", t1.minusMinutes(1), t1.plusMinutes(1));
        assertThat(stats.getRequestCount()).isEqualTo(1L);
        assertThat(stats.getTotalTokens()).isEqualTo(150L);

        List<ApiUsageLog> cursorPage = apiUsageLogMapper.selectByCursor(
                "t_1001", null, t1.plusSeconds(1), Long.MAX_VALUE, 10);
        assertThat(cursorPage).hasSize(1);
        assertThat(cursorPage.get(0).getRequestId()).isEqualTo("req-phase1-test-1");

        assertThat(deadLetterMapper.selectPending(10)).isNotNull();
    }
}
