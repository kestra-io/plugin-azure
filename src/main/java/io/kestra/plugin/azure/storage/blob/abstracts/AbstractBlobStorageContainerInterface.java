package io.kestra.plugin.azure.storage.blob.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface AbstractBlobStorageContainerInterface {
    @Schema(
        title = "The blob container"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getContainer();
}
