package io.kestra.plugin.azure.runner;

import com.azure.storage.blob.BlobContainerClient;
import com.microsoft.azure.batch.protocol.models.ContainerWorkingDirectory;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.ListUtils;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.batch.AbstractBatchInterface;
import io.kestra.plugin.azure.batch.job.Create;
import io.kestra.plugin.azure.batch.models.*;
import io.kestra.plugin.azure.storage.blob.SharedAccess;
import io.kestra.plugin.azure.storage.blob.models.BlobStorageForBatch;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Task runner that executes a task inside a job in Azure Batch.",
    description = """
        This task runner is container-based so the `containerImage` property must be set.
        
        To access the task's working directory, use the `{{workingDir}}` Pebble expression or the `WORKING_DIR` environment variable. Input files and namespace files will be available in this directory.

        To generate output files you can either use the `outputFiles` task's property and create a file with the same name in the task's working directory, or create any file in the output directory which can be accessed by the `{{outputDir}}` Pebble expression or the `OUTPUT_DIR` environment variables.
        
        To use `inputFiles`, `outputFiles` or `namespaceFiles` properties, make sure to set the `blobStorage` property. The blob storage serves as an intermediary storage layer for the task runner. Input and namespace files will be uploaded to the cloud storage bucket before the task run. Similarly, the task runner will store outputFiles in this blob storage during the task run. In the end, the task runner will make those files available for download and preview from the UI by sending them to internal storage.
        To make it easier to track where all files are stored, the task runner will generate a folder for each task run. You can access that folder using the `{{bucketPath}}` Pebble expression or the `BUCKET_PATH` environment variable.
        
        Note that when the Kestra Worker running this task is terminated, the batch job will still run until completion."""
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a Shell command.",
            code = """
                id: new-shell
                namespace: myteam
                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    taskRunner:
                      type: io.kestra.plugin.azure.runner.AzureBatchTaskRunner
                      account: "{{secrets.account}}"
                      accessKey: "{{secrets.accessKey}}"
                      endpoint: "{{secrets.endpoint}}"
                      poolId: "{{vars.poolId}}"
                    commands:
                    - echo "Hello World\"""",
            full = true
        ),
        @Example(
            title = "Pass input files to the task, execute a Shell command, then retrieve output files.",
            code = """
                id: new-shell-with-file
                namespace: myteam
                
                inputs:
                  - id: file
                    type: FILE
                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    inputFiles:
                      data.txt: "{{inputs.file}}"
                    outputFiles:
                      - out.txt
                    containerImage: centos
                    taskRunner:
                      type: io.kestra.plugin.azure.runner.AzureBatchTaskRunner
                      account: "{{secrets.account}}"
                      accessKey: "{{secrets.accessKey}}"
                      endpoint: "{{secrets.endpoint}}"
                      poolId: "{{vars.poolId}}"
                      blobStorage:
                        connectionString: "{{secrets.connectionString}}"
                        containerName: "{{vars.containerName}}"
                    commands:
                    - cp {{workingDir}}/data.txt {{workingDir}}/out.txt""",
            full = true
        )
    },
    beta = true
)
public class AzureBatchTaskRunner extends TaskRunner implements AbstractBatchInterface, AbstractConnectionInterface, RemoteRunnerInterface {

    private String account;
    private String accessKey;
    private String endpoint;

    private BlobStorageForBatch blobStorage;

    @Schema(
        title = "Id of the pool on which to run the job."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String poolId;

    @Schema(
        title = "The maximum duration to wait for the job completion unless the task `timeout` property is set which will take precedence over this property.",
        description = "Azure Batch will automatically timeout the job upon reaching such duration and the task will be failed."
    )
    @Builder.Default
    private final Duration waitUntilCompletion = Duration.ofHours(1);

    @Schema(
        title = "The private registry which contains the container image."
    )
    private ContainerRegistry registry;

    @Override
    public RunnerResult run(RunContext runContext, TaskCommands taskCommands, List<String> filesToUpload, List<String> filesToDownload) throws Exception {
        boolean hasBlobStorage = blobStorage != null && blobStorage.valid();

        boolean hasFilesToUpload = !ListUtils.isEmpty(filesToUpload);
        if (hasFilesToUpload && !hasBlobStorage) {
            throw new IllegalArgumentException("You must provide a way to connect to a Blob Storage container to use `inputFiles` or `namespaceFiles`");
        }
        boolean hasFilesToDownload = !ListUtils.isEmpty(filesToDownload);
        boolean outputDirectoryEnabled = taskCommands.outputDirectoryEnabled();
        if ((hasFilesToDownload || outputDirectoryEnabled) && !hasBlobStorage) {
            throw new IllegalArgumentException("You must provide a way to connect to a Blob Storage container to use `outputFiles` or `{{ outputDir }}`");
        }

        Map<String, Object> additionalVars = this.additionalVars(runContext, taskCommands);
        Path outputDirectory = (Path) additionalVars.get(ScriptService.VAR_OUTPUT_DIR);
        String blobStorageWdir = additionalVars.get(ScriptService.VAR_BUCKET_PATH).toString();

        String jobId = ScriptService.jobName(runContext);
        List<ResourceFile> resourceFiles = new ArrayList<>();
        if (hasFilesToUpload || outputDirectoryEnabled) {
            List<String> filesToUploadWithOutputDir = new ArrayList<>(filesToUpload);
            if (outputDirectoryEnabled) {
                String relativeOutputDirectoryMarkerPath = outputDirectory + "/.kestradirectory";
                File outputDirectoryMarker = runContext.resolve(Path.of(relativeOutputDirectoryMarkerPath)).toFile();
                outputDirectoryMarker.getParentFile().mkdirs();
                outputDirectoryMarker.createNewFile();
                filesToUploadWithOutputDir.add(relativeOutputDirectoryMarkerPath);
            }

            BlobContainerClient blobContainerClient = blobStorage.blobContainerClient(runContext);

            filesToUploadWithOutputDir.stream().map(throwFunction(file -> {
                // Use path to eventually deduplicate leading '/'
                String blobName = blobStorageWdir + Path.of("/" + file);
                blobContainerClient.getBlobClient(blobName)
                    .uploadFromFile(runContext.resolve(Path.of(file)).toString(), true);

                SharedAccess task = SharedAccess.builder()
                    .id(SharedAccess.class.getSimpleName())
                    .type(io.kestra.plugin.azure.storage.blob.List.class.getName())
                    .endpoint(this.endpoint)
                    .connectionString(blobStorage.getConnectionString())
                    .container(blobStorage.getContainerName())
                    .name(blobName)
                    .expirationDate("{{ now() | dateAdd(1, 'DAYS')  }}")
                    .permissions(Set.of(SharedAccess.Permission.READ))
                    .build();

                SharedAccess.Output sas = task.run(runContext);

                return ResourceFile.builder()
                    .filePath(file.startsWith("/") ? file.substring(1) : file)
                    // Use path to eventually deduplicate leading '/'
                    .httpUrl(sas.getUri().toString())
                    .build();
            })).forEach(resourceFiles::add);
        }

        AbstractLogConsumer logConsumer = taskCommands.getLogConsumer();

        List<String> commands = taskCommands.getCommands();
        Duration waitDuration = Optional.ofNullable(taskCommands.getTimeout()).orElse(this.waitUntilCompletion);
        Task.TaskBuilder taskBuilder = Task.builder()
            .id("task-" + jobId)
            .constraints(
                TaskConstraints.builder()
                    .maxWallClockTime(waitDuration)
                    .maxTaskRetryCount(0)
                    .build()
            )
            .interpreter(commands.get(0))
            .interpreterArgs(commands.size() > 1 ? new String[]{commands.get(1)} : new String[0])
            .commands(commands.size() > 2 ? commands.subList(2, commands.size()) : Collections.emptyList())
            .resourceFiles(resourceFiles)
            .outputFiles(filesToDownload)
            .containerSettings(
                TaskContainerSettings.builder()
                    .workingDirectory(ContainerWorkingDirectory.TASK_WORKING_DIRECTORY)
                    .registry(registry)
                    .imageName(taskCommands.getContainerImage())
                    .build()
            )
            .environments(this.env(runContext, taskCommands));

        if (outputDirectoryEnabled) {
            taskBuilder.outputDirs(List.of(additionalVars.get(ScriptService.VAR_OUTPUT_DIR).toString()));
        }

        Create createJob = Create.builder()
            .id("create")
            .type(Create.class.getName())
            .account(this.account)
            .accessKey(this.accessKey)
            .endpoint(this.endpoint)
            .poolId(this.poolId)
            .maxDuration(waitDuration)
            .job(
                Job.builder()
                    .id(jobId)
                    .labels(ScriptService.labels(runContext, "kestra-", true, true))
                    .build()
            )
            .tasks(List.of(taskBuilder.build()))
            .logConsumer(new AbstractLogConsumer() {
                @Override
                public void accept(String log, Boolean isStdErr) {
                    log.replaceAll("(?!^)(::\\{)", System.lineSeparator() + "::{")
                        .lines()
                        .forEach(line -> logConsumer.accept(line, isStdErr));
                }
            })
            .pushOutputFilesToInternalStorage(false)
            .build();

        try {
            createJob.run(runContext);
        } catch (Exception e) {
            throw new TaskException(e.getMessage(), 1, logConsumer.getStdOutCount(), logConsumer.getStdErrCount());
        }

        return new RunnerResult(0, logConsumer);
    }

    @Override
    public Map<String, Object> runnerAdditionalVars(RunContext runContext, TaskCommands taskCommands) {
        Map<String, Object> vars = new HashMap<>();
        if (blobStorage != null && blobStorage.valid()) {
            Path nestedDir = taskCommands.getWorkingDirectory().relativize(taskCommands.getOutputDirectory());
            vars.put(ScriptService.VAR_WORKING_DIR, taskCommands.getWorkingDirectory().toString());
            vars.put(ScriptService.VAR_BUCKET_PATH, nestedDir.toString());

            if (taskCommands.outputDirectoryEnabled()) {
                vars.put(ScriptService.VAR_OUTPUT_DIR, nestedDir);
            }
        }

        return vars;
    }
}
