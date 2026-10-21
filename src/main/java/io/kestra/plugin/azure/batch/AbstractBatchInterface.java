package io.kestra.plugin.azure.batch;

import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;

public interface AbstractBatchInterface {
    @Schema(title = "Batch account name")
    @NotNull
    @PluginProperty(group = "main")
    Property<String> getAccount();

    @Schema(title = "Batch account access key")
    @NotNull
    @PluginProperty(secret = true, group = "main")
    Property<String> getAccessKey();
}
