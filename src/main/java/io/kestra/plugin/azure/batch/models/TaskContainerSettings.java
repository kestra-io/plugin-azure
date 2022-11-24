package io.kestra.plugin.azure.batch.models;

import com.microsoft.azure.batch.protocol.models.ContainerWorkingDirectory;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

import javax.validation.constraints.NotNull;

@Builder
@Value
public class TaskContainerSettings {
    @Schema(
        title = "Additional options to the container create command.",
        description = "These additional options are supplied as arguments to the \"docker create\" command, in " +
            "addition to those controlled by the Batch Service."
    )
    @PluginProperty(dynamic = true)
    String containerRunOptions;

    @Schema(
        title = "The Image to use to create the container in which the Task will run.",
        description = "This is the full Image reference, as would be specified to \"docker pull\". If no tag is " +
            "provided as part of the Image name, the tag \":latest\" is used as a default."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String imageName;

    @Schema(
        title = "The private registry which contains the container Image.",
        description = "This setting can be omitted if was already provided at Pool creation."
    )
    @PluginProperty(dynamic = false)
    ContainerRegistry registry;

    @Schema(
        title = "The location of the container Task working directory.",
        description = "The default is 'taskWorkingDirectory'. Possible values include: 'taskWorkingDirectory', 'containerImageDefault'."
    )
    @PluginProperty(dynamic = false)
    ContainerWorkingDirectory workingDirectory;

    public com.microsoft.azure.batch.protocol.models.TaskContainerSettings to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new com.microsoft.azure.batch.protocol.models.TaskContainerSettings()
            .withContainerRunOptions(runContext.render(this.containerRunOptions))
            .withImageName(runContext.render(this.imageName))
            .withRegistry(this.registry != null ? this.registry.to(runContext) : null)
            .withWorkingDirectory(this.workingDirectory);
    }
}
