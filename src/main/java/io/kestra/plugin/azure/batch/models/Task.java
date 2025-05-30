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
        title = "A string that uniquely identifies the Task within the Job.",
        description = "The ID can contain any combination of alphanumeric characters including hyphens and underscores, a" +
            "nd cannot contain more than 64 characters. The ID is case-preserving and case-insensitive " +
            "(that is, you may not have two IDs within a Job that differ only by case).\n" +
            "If not provided, a random UUID will be generated."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    @Size(max=64)
    String id;

    @Schema(
        title = "A display name for the Task.",
        description = "The display name need not be unique and can contain any Unicode characters up to a maximum length of 1024."
    )
    @PluginProperty(dynamic = true)
    @Size(max=1024)
    String displayName;

    @Builder.Default
    @Schema(
        title = "Interpreter to be used."
    )
    @PluginProperty
    @NotNull
    Property<String> interpreter = Property.ofValue("/bin/sh");

    @Builder.Default
    @Schema(
        title = "Interpreter args to be used."
    )
    @PluginProperty
    String[] interpreterArgs = {"-c"};

    @Schema(
        title = "The command line of the Task.",
        description = "For multi-instance Tasks, the command line is executed as the primary Task, after the primary " +
            "Task and all subtasks have finished executing the coordination command line. The command line does not " +
            "run under a shell, and therefore cannot take advantage of shell features such as environment variable " +
            "expansion. If you want to take advantage of such features, you should invoke the shell in the command line, " +
            "for example, using `cmd /c MyCommand` in Windows or `/bin/sh -c MyCommand` in Linux. If the command " +
            "line refers to file paths, it should use a relative path (relative to the Task working directory), or " +
            "use the Batch provided [environment variable](https://docs.microsoft.com/en-us/azure/batch/batch-compute-node-environment-variables).\n\n" +
            "Command will be passed as `/bin/sh -c \"command\"` by default."
    )
    @NotNull
    Property<List<String>> commands;

    @Schema(
        title = "The settings for the container under which the Task runs.",
        description = "If the Pool that will run this Task has `containerConfiguration` set, this must be set as well. " +
            "If the Pool that will run this Task doesn't have `containerConfiguration` set, this must not be set. " +
            "When this is specified, all directories recursively below the AZ_BATCH_NODE_ROOT_DIR (the root of Azure " +
            "Batch directories on the node) are mapped into the container, all Task environment variables are mapped " +
            "into the container, and the Task command line is executed in the container. Files produced in the " +
            "container outside of AZ_BATCH_NODE_ROOT_DIR might not be reflected to the host disk, meaning that Batch " +
            "file APIs will not be able to access those files."
    )
    @PluginProperty(dynamic = true)
    TaskContainerSettings containerSettings;

    @Schema(
        title = "Output file list that will be uploaded to the internal storage.",
        description = "List of keys that will generate temporary files.\n" +
            "In the command, you can use a special variable named `outputFiles.key`.\n" +
            "If you add a file with `[\"first\"]`, you can use the special variable `echo 1 >> {{ outputFiles.first }}`" +
            "on this task, and reference this file on others tasks using `{{ outputs.taskId.outputFiles.first }}`."
    )
    Property<List<String>> outputFiles;

    @Schema(
        title = "Output directories list that will be uploaded to the internal storage.",
        description = "List of keys that will generate temporary directories.\n" +
            "In the command, you can use a special variable named `outputDirs.key`.\n" +
            "If you add a file with `[\"myDir\"]`, you can use the special variable `echo 1 >> {{ outputDirs.myDir }}/file1.txt` " +
            "and `echo 2 >> {{ outputDirs.myDir }}/file2.txt`, and both files will be uploaded to the internal storage. " +
            "Then, you can use them on other tasks using `{{ outputs.taskId.files['myDir/file1.txt'] }}`"
    )
    Property<List<String>> outputDirs;

    @Schema(
        title = "A list of files that the Batch service will download to the Compute Node before running the command line.",
        description = "For multi-instance Tasks, the resource files will only be downloaded to the Compute Node on " +
            "which the primary Task is executed. There is a maximum size for the list of resource files. When the max " +
            "size is exceeded, the request will fail and the response error code will be RequestEntityTooLarge. If this " +
            "occurs, the collection of ResourceFiles must be reduced in size. This can be achieved using .zip files, " +
            "Application Packages, or Docker Containers."
    )
    @PluginProperty(dynamic = true)
    List<ResourceFile> resourceFiles;

    @Schema(
        title = "A list of files that the Batch service will upload from the Compute Node after running the command line.",
        description = "For multi-instance Tasks, the files will only be uploaded from the Compute Node on which the primary Task is executed."
    )
    @PluginProperty(dynamic = true)
    List<OutputFile> uploadFiles;

    @Schema(
        title = "A list of environment variable settings for the Task."
    )
    Property<Map<String, String>> environments;

    @Schema(
        title = "The execution constraints that apply to this Task."
    )
    @PluginProperty(dynamic = true)
    TaskConstraints constraints;

    @Schema(
        title = "The number of scheduling slots that the Task requires to run.",
        description = "The default is 1. A Task can only be scheduled to run on a compute node if the node has enough " +
            "free scheduling slots available. For multi-instance Tasks, this must be 1."
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
