package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor
@SuperBuilder
@Getter
public abstract class AbstractEventHubTask extends Task implements EventHubClientInterface {

    @Schema(title = "Event Hubs connection string", description = "Namespace or Event Hub–level connection string; overrides key/sas fields when set")
    private Property<String> connectionString;

    @Schema(title = "Shared key account name", description = "Event Hubs namespace name used with sharedKeyAccountAccessKey when no connection string is provided")
    private Property<String> sharedKeyAccountName;

    @Schema(title = "Shared key", description = "Access key paired with sharedKeyAccountName; ignored if connectionString is provided")
    private Property<String> sharedKeyAccountAccessKey;

    @Schema(title = "SAS token", description = "Precomputed SAS token for Event Hubs; optional alternative to shared key/connection string")
    private Property<String> sasToken;

    @Builder.Default
    @Schema(title = "Client max retries", description = "Max retry attempts for Event Hubs client operations; default 5")
    private Property<Integer> clientMaxRetries = Property.ofValue(5);

    @Builder.Default
    @Schema(title = "Retry delay (ms)", description = "Delay between client retries in milliseconds; default 500")
    private Property<Long> clientRetryDelay = Property.ofValue(500L);

    @Schema(title = "Namespace", description = "Event Hubs namespace when using AAD or shared key auth")
    private Property<String> namespace;

    @Schema(title = "Event Hub name", description = "Target Event Hub entity")
    private Property<String> eventHubName;

    @Schema(title = "Custom endpoint address", description = "Custom endpoint for Event Hubs (e.g., for private link); optional")
    private Property<String> customEndpointAddress;
}
