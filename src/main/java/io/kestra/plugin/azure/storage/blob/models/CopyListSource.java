package io.kestra.plugin.azure.storage.blob.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import jakarta.validation.constraints.NotNull;

@Builder
@Getter
public class CopyListSource {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(title = "The source container")
    private String container;

    @PluginProperty(dynamic = true)
    @Schema(title = "The source prefix to list blobs")
    private String prefix;

    @PluginProperty(dynamic = true)
    @Schema(title = "A regular expression to filter blobs")
    private String regexp;
}