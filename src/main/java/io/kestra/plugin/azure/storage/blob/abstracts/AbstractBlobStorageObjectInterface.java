package io.kestra.plugin.azure.storage.blob.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface AbstractBlobStorageObjectInterface {
    @Schema(
        title = "The blob container."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getContainer();

    @Schema(
        title = "The full blob path on the container."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getName();
}
