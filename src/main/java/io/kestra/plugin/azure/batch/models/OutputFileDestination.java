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
public class OutputFileDestination {
    @Schema(
        title = "A location in Azure blob storage to which files are uploaded."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    OutputFileBlobContainerDestination container;

    public com.microsoft.azure.batch.protocol.models.OutputFileDestination to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.OutputFileDestination()
            .withContainer(this.container == null ? null : this.container.to(runContext));
    }
}
