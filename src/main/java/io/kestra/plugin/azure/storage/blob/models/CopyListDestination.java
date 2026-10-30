package io.kestra.plugin.azure.storage.blob.models;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@Getter
@NoArgsConstructor
public class CopyListDestination {
    @Schema(title = "The destination container")
    @NotNull
    @PluginProperty(group = "destination")
    private Property<String> container;

    @Schema(
        title = "The destination prefix",
        description = "Replaces the source `prefix` in each copied blob name. When no source prefix is set, it is prepended to the blob name."
    )
    @PluginProperty(group = "destination")
    private Property<String> prefix;
}
