package io.kestra.plugin.azure.storage.adls.abstracts;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface AbstractDataLakeStorageInterface {
    @Schema(
        title = "The name of the file systems. If the path name contains special characters, pass in the url encoded version of the path name."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getFileSystem();
}
