package io.kestra.plugin.azure.storage.blob.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface AbstractBlobStorageObjectInterface {
    @Schema(
        title = "The blob container."
    )
    @NotNull
    Property<String> getContainer();

    @Schema(
        title = "The full blob path on the container."
    )
    @NotNull
    Property<String> getName();
}
