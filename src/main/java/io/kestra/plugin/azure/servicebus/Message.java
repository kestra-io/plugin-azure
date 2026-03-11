package io.kestra.plugin.azure.servicebus;

import java.time.Duration;
import java.util.Map;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

@Builder
@Getter
@Jacksonized
public class Message implements io.kestra.core.models.tasks.Output {
    private final String messageId;
    private final String subject;
    private final Object body;
    private final Duration timeToLive;
    private final Map<String, Object> applicationProperties;
}
