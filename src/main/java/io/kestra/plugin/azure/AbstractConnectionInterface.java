package io.kestra.plugin.azure;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface AbstractConnectionInterface {
    @Schema(
        title = "The blob service endpoint."
    )
    Property<String> getEndpoint();
}
