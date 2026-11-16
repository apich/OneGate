package com.onegate.domain.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * OpenAI 兼容 {@code /v1/chat/completions} 请求体 DTO。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ChatCompletionRequest {

    @NotBlank(message = "model must not be blank")
    private String model;

    @NotEmpty(message = "messages must not be empty")
    @Valid
    private List<ChatMessage> messages;

    @Builder.Default
    private Boolean stream = Boolean.FALSE;

    @Min(value = 1, message = "max_tokens must be at least 1")
    @Max(value = 65536, message = "max_tokens must not exceed 65536")
    @JsonProperty("max_tokens")
    private Integer maxTokens;

    private Double temperature;

    @JsonProperty("top_p")
    private Double topP;

    private String user;

    @JsonProperty("stream_options")
    private StreamOptions streamOptions;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChatMessage {
        @NotBlank(message = "message role must not be blank")
        private String role;

        @NotNull(message = "message content must not be null")
        private String content;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StreamOptions {
        @JsonProperty("include_usage")
        private Boolean includeUsage;
    }
}
