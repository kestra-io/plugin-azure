package io.kestra.plugin.azure.storage.table.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotNull;

public interface AbstractTableStorageInterface {
    @Schema(
        title = "The Azure Storage Table name."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getTable();
}
