package io.kestra.plugin.azure.storage.blob.abstracts;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface AbstractBlobStorageContainerInterface {
    @Schema(
        title = "The blob container."
    )
    @NotNull
    Property<String> getContainer();
}
