package io.kestra.plugin.azure.storage.blob.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.plugin.azure.storage.blob.Copy;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ActionInterface {
    @Schema(
        title = "The action to perform on the retrieved files. If using `NONE`, make sure to handle the files inside your flow to avoid infinite triggering"
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<Action> getAction();

    @Schema(
        title = "The destination container and blob name",
        description = """
            When moving blobs, any relative directory structure under the configured `prefix` is preserved under `moveTo.name`.
            If the `prefix` does not end on a folder boundary (e.g. `data/2026`), the common parent folder (`data/`) is used to preserve subdirectories.
            If no `prefix` is configured, only the blob name is appended.
            If multiple blobs resolve to the same destination name, execution fails before any copy or delete occurs to prevent data loss.
            """
    )
    @PluginProperty(dynamic = true, group = "destination")
    Copy.CopyObject getMoveTo();

    enum Action {
        MOVE,
        DELETE,
        NONE
    }
}
