package io.kestra.plugin.azure;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Top-level interface that can be used by plugins to retrieve
 * required configuration properties in order to establish connection to Azure services.
 */
public interface AzureSasTokenInterface {
    @Schema(
        title = "The SAS token to use for authenticating requests.",
        description = "This string should only be the query parameters (with or without a leading '?') and not a full URL."
    )
    Property<String> getSasToken();
}
