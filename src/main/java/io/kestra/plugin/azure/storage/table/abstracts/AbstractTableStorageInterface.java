package io.kestra.plugin.azure.storage.table.abstracts;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;

public interface AbstractTableStorageInterface {
    @Schema(
        title = "Target Azure table name",
        description = "Existing Azure Table Storage table to operate on; case-insensitive."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<String> getTable();
}
