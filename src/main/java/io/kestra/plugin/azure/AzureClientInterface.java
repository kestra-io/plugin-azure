package io.kestra.plugin.azure;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Top-level interface that can be used by plugins to retrieve
 * required configuration properties in order to establish connection to Azure services.
 */
public interface AzureClientInterface {
    @Schema(
        title = "Connection string of the Storage Account."
    )
    Property<String> getConnectionString();

    @Schema(
        title = "Shared Key account name for authenticating requests."
    )
    Property<String> getSharedKeyAccountName();

    @Schema(
        title = "Shared Key access key for authenticating requests."
    )
    Property<String> getSharedKeyAccountAccessKey();
}