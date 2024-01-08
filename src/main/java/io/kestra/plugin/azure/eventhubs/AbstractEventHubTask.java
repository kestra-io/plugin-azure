package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.tasks.Task;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor
@SuperBuilder
@Getter
public class AbstractEventHubTask extends Task implements EventHubClientInterface {

    private String connectionString;

    private String sharedKeyAccountName;

    private String sharedKeyAccountAccessKey;

    private String sasToken;

    @Builder.Default
    private Integer clientMaxRetries = 5;

    @Builder.Default
    private Long clientRetryDelay = 500L;

    private String namespace;

    private String eventHubName;

    private String customEndpointAddress;
}
