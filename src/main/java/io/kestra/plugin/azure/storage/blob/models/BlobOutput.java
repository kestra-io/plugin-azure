package io.kestra.plugin.azure.storage.blob.models;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
public class BlobOutput implements io.kestra.core.models.tasks.Output {
    @Schema(
        title = "The blob container."
    )
    private final String container;

    @Schema(
        title = "The blob name."
    )
    private final String name;

    @Schema(
        title = "The blob URI."
    )
    private final String uri;

    @Schema(
        title = "The blob size."
    )
    private final long size;
}
