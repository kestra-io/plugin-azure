package io.kestra.plugin.azure.runner;

import com.azure.storage.blob.BlobContainerClient;
import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.protocol.models.ContainerWorkingDirectory;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.ListUtils;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.batch.AbstractBatchInterface;
import io.kestra.plugin.azure.batch.BatchService;
import io.kestra.plugin.azure.batch.job.Create;
import io.kestra.plugin.azure.batch.models.*;
import io.kestra.plugin.azure.storage.blob.SharedAccess;
import io.kestra.plugin.azure.storage.blob.models.BlobStorageForBatch;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

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
        There is two supported way to provide authentication for the blob storage:
        - `connectionString` and `containerName` properties
        - `containerName`, `endpoint`, `sharedKeyAccountName` and `sharedKeyAccountAccessKey` properties
        
        Note that when the Kestra Worker running this task is terminated, the batch job will still runs until completion, then after restarting, the Worker will resume processing on the existing job unless `resume` is set to false."""
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a Shell command.",
            code = """
                id: new-shell
                namespace: company.team
                
                tasks:
                  - id: shell
                    type: io.kestra.plugin.scripts.shell.Commands
                    taskRunner:
                      type: io.kestra.plugin.azure.runner.Batch
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
                namespace: company.team
                
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
                      type: io.kestra.plugin.azure.runner.Batch
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
    }
)
public class Batch extends TaskRunner implements AbstractBatchInterface, AbstractConnectionInterface, RemoteRunnerInterface {
    private String account;
    private String accessKey;
    @NotNull
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

    @Schema(
        title = "Determines how often Kestra should poll the container for completion. By default, the task runner checks every 5 seconds whether the job is completed. You can set this to a lower value (e.g. `PT0.1S` = every 100 milliseconds) for quick jobs and to a lower threshold (e.g. `PT1M` = every minute) for long-running jobs. Setting this property to a lower value will reduce the number of API calls Kestra makes to the remote service — keep that in mind in case you see API rate limit errors."
    )
    @Builder.Default
    @PluginProperty
    private final Duration completionCheckInterval = Duration.ofSeconds(5);

    @Schema(
        title = "Whether the job should be deleted upon completion."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private final Boolean delete = true;

    @Schema(
        title = "Whether to reconnect to the current job if it already exists."
    )
    @NotNull
    @Builder.Default
    @PluginProperty
    private final Boolean resume = true;

    @Override
    public RunnerResult run(RunContext runContext, TaskCommands taskCommands, List<String> filesToDownload) throws Exception {
        boolean hasBlobStorage = blobStorage != null && blobStorage.valid();

        Logger logger = runContext.logger();
        List<Path> relativeWorkingDirectoryFilesPaths = taskCommands.relativeWorkingDirectoryFilesPaths();
        boolean hasFilesToUpload = !ListUtils.isEmpty(relativeWorkingDirectoryFilesPaths);
        if (hasFilesToUpload && !hasBlobStorage) {
            logger.warn("Working directory is not empty but no Blob Storage container connection are specified. You must provide a Blob Storage in order to use `inputFiles` or `namespaceFiles`. Skipping importing files to runner.");
        }
        boolean hasFilesToDownload = !ListUtils.isEmpty(filesToDownload);
        boolean outputDirectoryEnabled = taskCommands.outputDirectoryEnabled();
        if ((hasFilesToDownload || outputDirectoryEnabled) && !hasBlobStorage) {
            throw new IllegalArgumentException("You must provide a way to connect to a Blob Storage container to use `outputFiles` or `{{ outputDir }}`");
        }

        Map<String, Object> additionalVars = this.additionalVars(runContext, taskCommands);

        String jobId = ScriptService.jobName(runContext);
        if (jobId.length() > 59) {
            jobId = jobId.substring(0, 59);
        }

        boolean resumeExisting = false;
        if (Boolean.TRUE.equals(this.resume)) {
            BatchClient client = BatchService.client(this.endpoint, this.account, this.accessKey, runContext);
            String baseJobName = jobId.substring(0, jobId.lastIndexOf('-') + 1);
            var cloudJob = BatchService.getExistingJob(runContext, client, baseJobName);
            resumeExisting = cloudJob.isPresent();
        }

        List<ResourceFile> resourceFiles = new ArrayList<>();
        if (!resumeExisting) {
            // only upload files if we resume an existing job
            if (hasFilesToUpload || outputDirectoryEnabled) {
                List<Path> filesToUploadWithOutputDir = new ArrayList<>(relativeWorkingDirectoryFilesPaths);
                if (outputDirectoryEnabled) {
                    Path outputDirectory = (Path) additionalVars.get(ScriptService.VAR_OUTPUT_DIR);
                    Path relativeOutputDirectoryMarkerPath = outputDirectory.resolve(".kestradirectory");
                    File outputDirectoryMarker = runContext.workingDir().resolve(relativeOutputDirectoryMarkerPath).toFile();
                    outputDirectoryMarker.getParentFile().mkdirs();
                    outputDirectoryMarker.createNewFile();
                    filesToUploadWithOutputDir.add(relativeOutputDirectoryMarkerPath);
                }

                BlobContainerClient blobContainerClient = blobStorage.blobContainerClient(runContext);

                String blobStorageWdir = additionalVars.get(ScriptService.VAR_BUCKET_PATH).toString();
                filesToUploadWithOutputDir.stream().map(throwFunction(file -> {
                    // Use path to eventually deduplicate leading '/'
                    String blobName = blobStorageWdir + Path.of("/" + file);
                    blobContainerClient.getBlobClient(blobName)
                        .uploadFromFile(runContext.workingDir().resolve(file).toString(), true);

                    SharedAccess task = SharedAccess.builder()
                        .id(SharedAccess.class.getSimpleName())
                        .type(io.kestra.plugin.azure.storage.blob.List.class.getName())
                        .endpoint(this.blobStorage.getEndpoint())
                        .sharedKeyAccountName(this.blobStorage.getSharedKeyAccountName())
                        .sharedKeyAccountAccessKey(this.blobStorage.getSharedKeyAccountAccessKey())
                        .connectionString(this.blobStorage.getConnectionString())
                        .container(this.blobStorage.getContainerName())
                        .name(blobName)
                        .expirationDate("{{ now() | dateAdd(1, 'DAYS')  }}")
                        .permissions(Set.of(SharedAccess.Permission.READ))
                        .build();

                    SharedAccess.Output sas = task.run(runContext);

                    return ResourceFile.builder()
                        .filePath(file.toString())
                        // Use path to eventually deduplicate leading '/'
                        .httpUrl(sas.getUri().toString())
                        .build();
                })).forEach(resourceFiles::add);
            }
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
            .completionCheckInterval(completionCheckInterval)
            .delete(this.delete)
            .resume(this.resume)
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
            onKill(createJob::kill);
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
