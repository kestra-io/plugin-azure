package io.kestra.plugin.azure.eventhubs;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.azure.shared.AzureClientWithSasInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

/**
 * This class is suffixed 'Interface' as it used to capture parameters from task properties.
 */
@SuperBuilder
@Getter
public final class BlobContainerClientInterface implements AzureClientWithSasInterface {

    private Property<String> connectionString;
    private Property<String> sharedKeyAccountName;
    private Property<String> sharedKeyAccountAccessKey;
    private Property<String> sasToken;
    @Schema(
        title = "The blob container name."
    )
    @PluginProperty(group = "advanced")
    private Property<String> containerName;
}
