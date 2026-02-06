package io.kestra.plugin.azure.batch.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import jakarta.validation.constraints.NotNull;

@Builder
@Value
public class OutputFileBlobContainerDestination {
    @Schema(
        title = "Blob path or virtual directory",
        description = "File name when matching a single file; virtual directory prefix when using wildcards; default uploads to container root"
    )
    Property<String> path;

    @Schema(
        title = "Container URL",
        description = "SAS or managed-identity URL with write permission to the container"
    )
    @NotNull
    Property<String> containerUrl;

    @Schema(
        title = "User-assigned identity",
        description = "Identity with write access used when containerUrl relies on managed identity"
    )
    ComputeNodeIdentityReference identityReference;

    public com.microsoft.azure.batch.protocol.models.OutputFileBlobContainerDestination to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.OutputFileBlobContainerDestination()
            .withPath(runContext.render(path).as(String.class).orElse(null))
            .withContainerUrl(runContext.render(this.containerUrl).as(String.class).orElse(null))
            .withIdentityReference(this.identityReference == null ? null : this.identityReference.to(runContext));
    }
}
