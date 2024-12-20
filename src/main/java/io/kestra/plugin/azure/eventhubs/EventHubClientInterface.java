package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.azure.AzureClientWithSasInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface EventHubClientInterface extends AzureClientWithSasInterface {

    @Schema(
        title = "Custom endpoint address when connecting to the Event Hubs service."
    )
    Property<String> getCustomEndpointAddress();

    @NotNull
    @Schema(
        title = "Namespace name of the event hub to connect to."
    )
    Property<String> getNamespace();

    @NotNull
    @Schema(
        title = "The event hub to read from."
    )
    Property<String> getEventHubName();

    @Schema(
        title = "The maximum number of retry attempts before considering a client operation to have failed."
    )
    Property<Integer> getClientMaxRetries();

    @Schema(
        title = "The maximum permissible delay between retry attempts in milliseconds."
    )
    Property<Long> getClientRetryDelay();
}

