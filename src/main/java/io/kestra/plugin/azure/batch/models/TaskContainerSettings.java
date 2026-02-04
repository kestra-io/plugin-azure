package io.kestra.plugin.azure.batch.models;

import com.microsoft.azure.batch.protocol.models.ContainerWorkingDirectory;
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
public class TaskContainerSettings {
    @Schema(
        title = "Container run options",
        description = "Extra arguments passed to docker create alongside Batch-managed options"
    )
    Property<String> containerRunOptions;

    @Schema(
        title = "Container image",
        description = "Full image reference (e.g. repo/name:tag); defaults to :latest if no tag provided"
    )
    @NotNull
    Property<String> imageName;

    @Schema(
        title = "Container registry",
        description = "Registry credentials; omit when provided at pool creation"
    )
    ContainerRegistry registry;

    @Schema(
        title = "Working directory scope",
        description = "Where the container starts; defaults to TASK_WORKING_DIRECTORY. Options: TASK_WORKING_DIRECTORY or CONTAINER_IMAGE_DEFAULT."
    )
    Property<ContainerWorkingDirectory> workingDirectory;

    public com.microsoft.azure.batch.protocol.models.TaskContainerSettings to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.TaskContainerSettings()
            .withContainerRunOptions(runContext.render(this.containerRunOptions).as(String.class).orElse(null))
            .withImageName(runContext.render(this.imageName).as(String.class).orElseThrow())
            .withRegistry(this.registry != null ? this.registry.to(runContext) : null)
            .withWorkingDirectory(runContext.render(this.workingDirectory).as(ContainerWorkingDirectory.class).orElse(null));
    }
}
