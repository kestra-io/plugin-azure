package io.kestra.plugin.azure;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface AbstractConnectionInterface {
    @Schema(
        title = "The blob service endpoint."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getEndpoint();
}
