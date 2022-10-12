package io.kestra.plugin.azure.batch.models;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import javax.validation.constraints.NotNull;

@Builder
@Value
public class OutputFileBlobContainerDestination {
    @Schema(
        title = "The destination blob or virtual directory within the Azure Storage container.",
        description = "If filePattern refers to a specific file (i.e. contains no wildcards), then path is the name of " +
            "the blob to which to upload that file. If filePattern contains one or more wildcards (and therefore may " +
            "match multiple files), then path is the name of the blob virtual directory (which is prepended to each " +
            "blob name) to which to upload the file(s). If omitted, file(s) are uploaded to the root of the container " +
            "with a blob name matching their file name."
    )
    @PluginProperty(dynamic = true)
    String path;

    @Schema(
        title = "The URL of the container within Azure Blob Storage to which to upload the file(s).",
        description = "If not using a managed identity, the URL must include a Shared Access Signature (SAS) " +
            "granting write permissions to the container."
    )
    @NotNull
    String containerUrl;

    @Schema(
        title = "The reference to the user assigned identity to use to access Azure Blob Storage specified by containerUrl.",
        description = "The identity must have write access to the Azure Blob Storage container."
    )
    ComputeNodeIdentityReference identityReference;

    public com.microsoft.azure.batch.protocol.models.OutputFileBlobContainerDestination to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.OutputFileBlobContainerDestination()
            .withPath(runContext.render(path))
            .withContainerUrl(runContext.render(this.containerUrl))
            .withIdentityReference(this.identityReference == null ? null : this.identityReference.to(runContext));
    }
}
