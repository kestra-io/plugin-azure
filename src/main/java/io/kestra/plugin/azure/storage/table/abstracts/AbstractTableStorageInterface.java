package io.kestra.plugin.azure.storage.table.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface AbstractTableStorageInterface {
    @Schema(
        title = "The Azure Table Storage table name."
    )
    @NotNull
    Property<String> getTable();
}
