package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.azure.shared.AzureClientWithSasInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;

public interface EventHubClientInterface extends AzureClientWithSasInterface {

    @Schema(
        title = "Custom endpoint address when connecting to the Event Hubs service."
    )
    @PluginProperty(group = "advanced")
    Property<String> getCustomEndpointAddress();

    @NotNull
    @Schema(
        title = "Namespace name of the event hub to connect to."
    )
    @PluginProperty(group = "main")
    Property<String> getNamespace();

    @NotNull
    @Schema(
        title = "The event hub to read from."
    )
    @PluginProperty(group = "main")
    Property<String> getEventHubName();

    @Schema(
        title = "The maximum number of retry attempts before considering a client operation to have failed."
    )
    @PluginProperty(group = "execution")
    Property<Integer> getClientMaxRetries();

    @Schema(
        title = "The maximum permissible delay between retry attempts in milliseconds."
    )
    @PluginProperty(group = "advanced")
    Property<Long> getClientRetryDelay();
}
