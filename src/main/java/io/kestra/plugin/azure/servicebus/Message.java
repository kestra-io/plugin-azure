package io.kestra.plugin.azure.servicebus;

import java.time.Duration;
import java.util.Map;

import io.swagger.v3.oas.annotations.media.Schema;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class Message implements io.kestra.core.models.tasks.Output {
    @Schema(title = "Message id")
    private final String messageId;
    @Schema(title = "Subject")
    private final String subject;
    @Schema(title = "Body")
    private final Object body;
    @Schema(title = "Time to live")
    private final Duration timeToLive;
    @Schema(title = "Application properties")
    private final Map<String, Object> applicationProperties;
}
