package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.azure.AzureClientInterface;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface EventHubClientInterface extends AzureClientInterface {

    @Schema(
        title = "Custom endpoint address when connecting to the Event Hubs service."
    )
    @PluginProperty(dynamic = true)
    String getCustomEndpointAddress();

    @NotNull
    @Schema(
        title = "Namespace name of the event hub to connect to."
    )
    @PluginProperty(dynamic = true)
    String getNamespace();

    @NotNull
    @Schema(
        title = "The event hub to read from."
    )
    @PluginProperty(dynamic = true)
    String getEventHubName();

    @Schema(
        title = "The maximum number of retry attempts before considering a client operation to have failed."
    )
    @PluginProperty
    Integer getClientMaxRetries();

    @Schema(
        title = "The maximum permissible delay between retry attempts in milliseconds."
    )
    @PluginProperty
    Long getClientRetryDelay();
}

