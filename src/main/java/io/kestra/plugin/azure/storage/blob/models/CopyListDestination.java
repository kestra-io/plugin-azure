package io.kestra.plugin.azure.storage.blob.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import jakarta.validation.constraints.NotNull;

@Builder
@Getter
public class CopyListDestination {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(title = "The destination container")
    private String container;

    @PluginProperty(dynamic = true)
    @Schema(title = "The destination prefix (folder path)")
    private String prefix;
}