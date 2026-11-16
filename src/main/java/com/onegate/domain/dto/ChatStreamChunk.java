package com.onegate.domain.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * OpenAI 兼容 SSE 流式响应增量分片 DTO ({@code chat.completion.chunk})。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChatStreamChunk {

    private String id;

    @Builder.Default
    private String object = "chat.completion.chunk";

    private Long created;

    private String model;

    private List<StreamChoice> choices;

    private ChatCompletionResponse.Usage usage;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StreamChoice {
        private Integer index;
        private Delta delta;
        @JsonProperty("finish_reason")
        private String finishReason;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Delta {
        private String role;
        private String content;
    }
}
