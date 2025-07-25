package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor
@SuperBuilder
@Getter
public abstract class AbstractEventHubTask extends Task implements EventHubClientInterface {

    private Property<String> connectionString;

    private Property<String> sharedKeyAccountName;

    private Property<String> sharedKeyAccountAccessKey;

    private Property<String> sasToken;

    @Builder.Default
    private Property<Integer> clientMaxRetries = Property.ofValue(5);

    @Builder.Default
    private Property<Long> clientRetryDelay = Property.ofValue(500L);

    private Property<String> namespace;

    private Property<String> eventHubName;

    private Property<String> customEndpointAddress;
}
