package io.kestra.plugin.azure.batch;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface AbstractBatchInterface {
    @Schema(
        title = "The Batch account name."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getAccount();

    @Schema(
        title = "The Batch access key."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getAccessKey();
}
