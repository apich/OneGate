package com.onegate.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onegate.domain.dto.ChatCompletionRequest;
import com.onegate.domain.dto.ChatCompletionResponse;
import com.onegate.domain.dto.ChatStreamChunk;
import com.onegate.router.TokenEstimator;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内置高仿真 OpenAI 协议模拟上游控制器，支撑零成本本地压测、TTFT 超时容灾复现与 SSE 断连中断测试。
 * <p>
 * 路径 {@code /mock-llm/**} 已在 {@code WebMvcConfig} 中排除鉴权拦截。
 * 支持通过请求头 {@code X-Mock-Mode} / {@code X-Mock-Primary-Mode} / {@code X-Mock-Backup-Mode}
 * 以及查询参数 {@code ?mode=...&delayMs=...&chunkCount=...&chunkDelayMs=...} 精确控制上游行为。
 */
@Slf4j
@RestController
@RequestMapping("/mock-llm")
@ConditionalOnProperty(name = "onegate.mock-enabled", havingValue = "true")
public class MockUpstreamController {

    public static final String HEADER_MOCK_MODE = "X-Mock-Mode";
    public static final String HEADER_MOCK_PRIMARY_MODE = "X-Mock-Primary-Mode";
    public static final String HEADER_MOCK_BACKUP_MODE = "X-Mock-Backup-Mode";
    public static final String HEADER_MOCK_DELAY_MS = "X-Mock-Delay-Ms";
    public static final String HEADER_MOCK_PRIMARY_DELAY_MS = "X-Mock-Primary-Delay-Ms";
    public static final String HEADER_MOCK_CHUNK_COUNT = "X-Mock-Chunk-Count";
    public static final String HEADER_MOCK_CHUNK_DELAY_MS = "X-Mock-Chunk-Delay-Ms";

    private static final long DEFAULT_TTFT_TIMEOUT_DELAY_MS = 5000L;
    private static final long MAX_INITIAL_DELAY_MS = 10_000L;
    private static final int DEFAULT_CHUNK_COUNT = 5;
    private static final int DEFAULT_SLOW_CHUNK_COUNT = 12;
    private static final int MAX_CHUNK_COUNT = 100;
    private static final long DEFAULT_NORMAL_CHUNK_DELAY_MS = 10L;
    private static final long DEFAULT_SLOW_CHUNK_DELAY_MS = 180L;
    private static final long MAX_CHUNK_DELAY_MS = 2_000L;

    private final ObjectMapper objectMapper;
    private final TokenEstimator tokenEstimator;

    @Getter
    private final AtomicLong completedStreamCount = new AtomicLong(0L);

    @Getter
    private final AtomicLong abortedStreamCount = new AtomicLong(0L);

    @Getter
    private final AtomicInteger lastStreamChunksWritten = new AtomicInteger(0);

    public MockUpstreamController(ObjectMapper objectMapper, TokenEstimator tokenEstimator) {
        this.objectMapper = objectMapper;
        this.tokenEstimator = tokenEstimator;
    }

    public void resetCounters() {
        completedStreamCount.set(0L);
        abortedStreamCount.set(0L);
        lastStreamChunksWritten.set(0);
    }

    @PostMapping("/{tier}/v1/chat/completions")
    public void chatCompletions(
            @PathVariable("tier") String tier,
            @RequestParam(value = "mode", required = false) String modeParam,
            @RequestParam(value = "delayMs", required = false) Long delayMsParam,
            @RequestParam(value = "chunkCount", required = false) Integer chunkCountParam,
            @RequestParam(value = "chunkDelayMs", required = false) Long chunkDelayMsParam,
            HttpServletRequest servletRequest,
            HttpServletResponse servletResponse,
            @RequestBody ChatCompletionRequest request) throws IOException {

        String mode = resolveMode(tier, modeParam, servletRequest);
        long initialDelayMs = clampLong(
                resolveInitialDelayMs(tier, mode, delayMsParam, servletRequest),
                0L,
                MAX_INITIAL_DELAY_MS);

        int defaultChunks = "slow-stream".equalsIgnoreCase(mode) ? DEFAULT_SLOW_CHUNK_COUNT : DEFAULT_CHUNK_COUNT;
        int chunkCount = (int) clampLong(resolveLongParam(
                chunkCountParam != null ? chunkCountParam.longValue() : null,
                servletRequest.getHeader(HEADER_MOCK_CHUNK_COUNT),
                defaultChunks), 1L, MAX_CHUNK_COUNT);
        long defaultChunkDelay = "slow-stream".equalsIgnoreCase(mode)
                ? DEFAULT_SLOW_CHUNK_DELAY_MS
                : DEFAULT_NORMAL_CHUNK_DELAY_MS;
        long chunkDelayMs = clampLong(resolveLongParam(
                chunkDelayMsParam, servletRequest.getHeader(HEADER_MOCK_CHUNK_DELAY_MS), defaultChunkDelay),
                0L,
                MAX_CHUNK_DELAY_MS);

        if ("error-500".equalsIgnoreCase(mode)) {
            writeOpenAiError(servletResponse, HttpStatus.INTERNAL_SERVER_ERROR,
                    "upstream_error", "upstream_500", "Mock upstream internal error on tier=" + tier);
            return;
        }
        if ("rate-429".equalsIgnoreCase(mode)) {
            writeOpenAiError(servletResponse, HttpStatus.TOO_MANY_REQUESTS,
                    "rate_limit_error", "rate_limit_exceeded", "Mock upstream rate limited on tier=" + tier);
            return;
        }

        int promptTokens = tokenEstimator.estimatePromptTokens(request);
        String model = request.getModel() != null ? request.getModel() : "mock-model";
        boolean stream = Boolean.TRUE.equals(request.getStream());

        if (!stream) {
            if (initialDelayMs > 0L && !sleepQuietly(initialDelayMs)) {
                servletResponse.setStatus(HttpStatus.REQUEST_TIMEOUT.value());
                return;
            }
            String replyText = String.format("Mock [%s] response for model %s.", tier, model);
            int completionTokens = Math.max(8, tokenEstimator.estimateCompletionTokens(replyText));
            ChatCompletionResponse response = ChatCompletionResponse.builder()
                    .id("chatcmpl-mock-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12))
                    .object("chat.completion")
                    .created(Instant.now().getEpochSecond())
                    .model(model)
                    .choices(Collections.singletonList(
                            ChatCompletionResponse.Choice.builder()
                                    .index(0)
                                    .message(ChatCompletionRequest.ChatMessage.builder()
                                            .role("assistant")
                                            .content(replyText)
                                            .build())
                                    .finishReason("stop")
                                    .build()))
                    .usage(ChatCompletionResponse.Usage.builder()
                            .promptTokens(promptTokens)
                            .completionTokens(completionTokens)
                            .totalTokens(promptTokens + completionTokens)
                            .build())
                    .build();

            servletResponse.setStatus(HttpStatus.OK.value());
            servletResponse.setContentType(MediaType.APPLICATION_JSON_VALUE);
            servletResponse.setCharacterEncoding(StandardCharsets.UTF_8.name());
            OutputStream out = servletResponse.getOutputStream();
            out.write(objectMapper.writeValueAsBytes(response));
            out.flush();
            return;
        }

        servletResponse.setStatus(HttpStatus.OK.value());
        servletResponse.setContentType(MediaType.TEXT_EVENT_STREAM_VALUE);
        servletResponse.setCharacterEncoding(StandardCharsets.UTF_8.name());
        servletResponse.setHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        servletResponse.flushBuffer();

        if (initialDelayMs > 0L && !sleepQuietly(initialDelayMs)) {
            abortedStreamCount.incrementAndGet();
            return;
        }

        String chatId = "chatcmpl-mock-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        long created = Instant.now().getEpochSecond();
        OutputStream outputStream = servletResponse.getOutputStream();
        int chunksWritten = 0;
        StringBuilder totalGenerated = new StringBuilder();

        try {
            for (int i = 0; i < chunkCount; i++) {
                String piece = (i == 0)
                        ? String.format("[%s] chunk-0 ", tier)
                        : String.format("token-%d ", i);
                totalGenerated.append(piece);

                ChatStreamChunk.Delta delta = ChatStreamChunk.Delta.builder()
                        .role(i == 0 ? "assistant" : null)
                        .content(piece)
                        .build();
                ChatStreamChunk chunk = ChatStreamChunk.builder()
                        .id(chatId)
                        .object("chat.completion.chunk")
                        .created(created)
                        .model(model)
                        .choices(Collections.singletonList(
                                ChatStreamChunk.StreamChoice.builder()
                                        .index(0)
                                        .delta(delta)
                                        .finishReason(null)
                                        .build()))
                        .build();

                writeSseData(outputStream, objectMapper.writeValueAsString(chunk));
                chunksWritten++;
                lastStreamChunksWritten.set(chunksWritten);

                if (chunkDelayMs > 0L && !sleepQuietly(chunkDelayMs)) {
                    abortedStreamCount.incrementAndGet();
                    return;
                }
            }

            int completionTokens = Math.max(chunkCount, tokenEstimator.estimateCompletionTokens(totalGenerated));
            ChatStreamChunk tailChunk = ChatStreamChunk.builder()
                    .id(chatId)
                    .object("chat.completion.chunk")
                    .created(created)
                    .model(model)
                    .choices(Collections.singletonList(
                            ChatStreamChunk.StreamChoice.builder()
                                    .index(0)
                                    .delta(ChatStreamChunk.Delta.builder().build())
                                    .finishReason("stop")
                                    .build()))
                    .usage(ChatCompletionResponse.Usage.builder()
                            .promptTokens(promptTokens)
                            .completionTokens(completionTokens)
                            .totalTokens(promptTokens + completionTokens)
                            .build())
                    .build();

            writeSseData(outputStream, objectMapper.writeValueAsString(tailChunk));
            writeSseData(outputStream, "[DONE]");
            completedStreamCount.incrementAndGet();
        } catch (IOException ioEx) {
            abortedStreamCount.incrementAndGet();
            log.info("Mock upstream SSE stream aborted by client/gateway on tier={} after {} chunks: {}",
                    tier, chunksWritten, ioEx.getMessage());
        }
    }

    private void writeSseData(OutputStream out, String data) throws IOException {
        byte[] bytes = ("data: " + data + "\n\n").getBytes(StandardCharsets.UTF_8);
        out.write(bytes);
        out.flush();
    }

    private String resolveMode(String tier, String modeParam, HttpServletRequest request) {
        if (modeParam != null && !modeParam.isBlank()) {
            return modeParam.trim();
        }
        if ("primary".equalsIgnoreCase(tier)) {
            String primaryHeader = request.getHeader(HEADER_MOCK_PRIMARY_MODE);
            if (primaryHeader != null && !primaryHeader.isBlank()) {
                return primaryHeader.trim();
            }
        }
        if ("backup".equalsIgnoreCase(tier)) {
            String backupHeader = request.getHeader(HEADER_MOCK_BACKUP_MODE);
            if (backupHeader != null && !backupHeader.isBlank()) {
                return backupHeader.trim();
            }
        }
        String modeHeader = request.getHeader(HEADER_MOCK_MODE);
        if (modeHeader != null && !modeHeader.isBlank()) {
            return modeHeader.trim();
        }
        if ("slow".equalsIgnoreCase(tier)) {
            return "slow-stream";
        }
        return "normal";
    }

    private long resolveInitialDelayMs(String tier, String mode, Long delayMsParam, HttpServletRequest request) {
        if (delayMsParam != null) {
            return Math.max(0L, delayMsParam);
        }
        if ("primary".equalsIgnoreCase(tier)) {
            String primaryDelay = request.getHeader(HEADER_MOCK_PRIMARY_DELAY_MS);
            if (primaryDelay != null && !primaryDelay.isBlank()) {
                return Math.max(0L, parseLongOrDefault(primaryDelay, 0L));
            }
        }
        if ("backup".equalsIgnoreCase(tier)
                && "normal".equalsIgnoreCase(mode)
                && request.getHeader(HEADER_MOCK_PRIMARY_MODE) != null) {
            return 0L;
        }
        long headerDelay = parseLongOrDefault(request.getHeader(HEADER_MOCK_DELAY_MS), -1L);
        if (headerDelay >= 0L) {
            return headerDelay;
        }
        return "ttft-timeout".equalsIgnoreCase(mode) ? DEFAULT_TTFT_TIMEOUT_DELAY_MS : 0L;
    }

    private long clampLong(long value, long min, long max) {
        return Math.min(max, Math.max(min, value));
    }

    private long resolveLongParam(Long queryParam, String headerVal, long defaultVal) {
        if (queryParam != null) {
            return queryParam;
        }
        return parseLongOrDefault(headerVal, defaultVal);
    }

    private long parseLongOrDefault(String val, long defaultVal) {
        if (val != null && !val.isBlank()) {
            try {
                return Long.parseLong(val.trim());
            } catch (NumberFormatException ignored) {
                return defaultVal;
            }
        }
        return defaultVal;
    }

    private boolean sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void writeOpenAiError(
            HttpServletResponse response, HttpStatus status, String type, String code, String message)
            throws IOException {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("message", message);
        error.put("type", type);
        error.put("param", null);
        error.put("code", code);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", error);

        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        OutputStream out = response.getOutputStream();
        out.write(objectMapper.writeValueAsBytes(body));
        out.flush();
    }
}
