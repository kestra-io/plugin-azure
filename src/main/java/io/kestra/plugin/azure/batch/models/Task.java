package io.kestra.plugin.azure.batch.models;

import com.microsoft.azure.batch.protocol.models.EnvironmentSetting;
import com.microsoft.azure.batch.protocol.models.TaskAddParameter;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.util.*;
import java.util.stream.Collectors;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Builder
@Value
@Jacksonized
public class Task {
    @Schema(
        title = "Task ID",
        description = "Unique within the job (<=64 chars, alphanumeric, hyphen, underscore); case-insensitive; generated if omitted"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Size(max=64)
    String id;

    @Schema(
        title = "Display name",
        description = "Optional friendly name (<=1024 Unicode chars); not required to be unique"
    )
    @PluginProperty(dynamic = true)
    @Size(max=1024)
    String displayName;

    @Builder.Default
    @Schema(
        title = "Command interpreter",
        description = "Shell used to run the task; defaults to /bin/sh"
    )
    @PluginProperty
    @NotNull
    Property<String> interpreter = Property.ofValue("/bin/sh");

    @Builder.Default
    @Schema(
        title = "Interpreter arguments",
        description = "Arguments prepended to the command; defaults to ['-c']"
    )
    @PluginProperty
    String[] interpreterArgs = {"-c"};

    @Schema(
        title = "Commands",
        description = "Command lines executed inside the interpreter; rendered with expressions then joined and passed to interpreter/args"
    )
    @NotNull
    Property<List<String>> commands;

    @Schema(
        title = "Container settings",
        description = "Container image and run options; required if the pool is container-enabled, omit otherwise"
    )
    @PluginProperty(dynamic = true)
    TaskContainerSettings containerSettings;

    @Schema(
        title = "Output files",
        description = "Names of files to upload from the working directory to internal storage; use {{ outputFiles.key }} to write paths"
    )
    Property<List<String>> outputFiles;

    @Schema(
        title = "Output directories",
        description = "Directories to sync to internal storage; use {{ outputDirs.key }} to write files under that directory"
    )
    Property<List<String>> outputDirs;

    @Schema(
        title = "Resource files",
        description = "Files staged to the node before execution; subject to Azure Batch request size limits"
    )
    @PluginProperty(dynamic = true)
    List<ResourceFile> resourceFiles;

    @Schema(
        title = "Upload files",
        description = "Files Azure Batch uploads from the node after execution; for multi-instance tasks only the primary node uploads"
    )
    @PluginProperty(dynamic = true)
    List<OutputFile> uploadFiles;

    @Schema(
        title = "Environment variables",
        description = "Key/value environment map rendered before execution"
    )
    Property<Map<String, String>> environments;

    @Schema(
        title = "Execution constraints"
    )
    @PluginProperty(dynamic = true)
    TaskConstraints constraints;

    @Schema(
        title = "Required scheduling slots",
        description = "Number of slots needed on a node; default 1 and must remain 1 for multi-instance tasks"
    )
    Property<Integer> requiredSlots;

    public TaskAddParameter to(RunContext runContext) throws IllegalVariableEvaluationException {
        return new TaskAddParameter()
            .withId(this.id == null ? IdUtils.create() : runContext.render(this.id))
            .withDisplayName(runContext.render(this.displayName))
            .withCommandLine(runContext.render(this.commandLine(runContext)))
            .withContainerSettings(this.containerSettings == null ? null : this.containerSettings.to(runContext))
            .withEnvironmentSettings(this.environments == null ? null : runContext.render(this.environments).asMap(String.class, String.class)
                .entrySet()
                .stream()
                .map(throwFunction(e -> new EnvironmentSetting()
                    .withName(runContext.render(e.getKey()))
                    .withValue(runContext.render(e.getValue()))
                ))
                .collect(Collectors.toList())
            )
            .withResourceFiles(this.resourceFiles == null ? null : this.resourceFiles
                .stream()
                .map(throwFunction(s -> s.to(runContext)))
                .collect(Collectors.toList())
            )
            .withOutputFiles(this.uploadFiles == null ? null : this.uploadFiles
                .stream()
                .map(throwFunction(s -> s.to(runContext)))
                .collect(Collectors.toList()))
            .withConstraints(this.constraints == null ? null : this.constraints.to(runContext))
            .withRequiredSlots(runContext.render(this.requiredSlots).as(Integer.class).orElse(null))
        ;
    }

    private String commandLine(RunContext runContext) throws IllegalVariableEvaluationException {
        // renderer command
        List<String> renderer = new ArrayList<>();


        for (String command : runContext.render(this.commands).asList(String.class)) {
            renderer.add(runContext
                .render(command)
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                // already escape by az batch
                // .replace("$", "\\$")
                // .replace("`", "\\`")
            );
        }

        String commandAsString = "\"" + String.join("\n", renderer) + "\"";

        // interpreter
        List<String> commandsWithInterpreter = new ArrayList<>(Collections.singletonList(runContext.render(interpreter).as(String.class).orElseThrow()));
        commandsWithInterpreter.addAll(Arrays.asList(interpreterArgs));
        commandsWithInterpreter.add(commandAsString);

        // generate command
        return String.join(" ", commandsWithInterpreter);
    }
}
