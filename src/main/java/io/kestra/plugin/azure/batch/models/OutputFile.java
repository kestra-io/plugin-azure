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
public class OutputFile {
    @Schema(
        title = "File pattern to upload",
        description = "Supports absolute or task-relative paths with wildcards (*, **, ?, [set]); env vars expanded before matching; dotfiles require explicit match"
    )
    Property<String> filePattern;

    @Schema(
        title = "Upload destination"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    OutputFileDestination destination;

    @Schema(
        title = "Upload options",
        description = "Controls when the upload runs and other transfer settings"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @Builder.Default
    OutputFileUploadOptions uploadOptions = OutputFileUploadOptions.builder().build();

    public com.microsoft.azure.batch.protocol.models.OutputFile to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.OutputFile()
            .withFilePattern(runContext.render(this.filePattern).as(String.class).orElse(null))
            .withDestination(this.destination == null ? null : this.destination.to(runContext))
            .withUploadOptions(this.uploadOptions == null ? null : this.uploadOptions.to(runContext));
    }
}
