package io.kestra.plugin.azure.batch;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface AbstractBatchInterface {
    @Schema(
        title = "The Batch account name."
    )
    @NotNull
    Property<String> getAccount();

    @Schema(
        title = "The Batch access key."
    )
    @NotNull
    Property<String> getAccessKey();
}
