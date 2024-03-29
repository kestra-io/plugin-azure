package io.kestra.plugin.azure.runner;

import com.azure.storage.blob.BlobContainerClient;
import com.microsoft.azure.batch.protocol.models.ContainerWorkingDirectory;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.script.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.ListUtils;
import io.kestra.plugin.azure.AbstractConnectionInterface;
import io.kestra.plugin.azure.batch.AbstractBatchInterface;
import io.kestra.plugin.azure.batch.job.Create;
import io.kestra.plugin.azure.batch.models.*;
import io.kestra.plugin.azure.storage.blob.SharedAccess;
import io.kestra.plugin.azure.storage.blob.models.BlobStorageForBatch;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Introspected
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Azure Batch script runner", description = """
    Run a script in a container on Azure Batch.
    Upon worker restart, this job will be requeued and executed again. Moreover, the existing job will be kept running and handled by Azure Batch till this issue (https://github.com/kestra-io/plugin-azure/issues/80) is handled.
    To use `inputFiles`, `outputFiles` and `namespaceFiles` properties, you must provide a `blobStorage` to connect to.
    Doing so will upload the files to the bucket before running the script and download them after the script execution.
    This runner will wait for the task to succeed or fail up to a max `waitUntilCompletion` duration.""")
@Plugin(examples = {}, beta = true)
public class AzureBatchScriptRunner extends ScriptRunner implements AbstractBatchInterface, AbstractConnectionInterface {

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
        title = "The maximum duration to wait for the job completion. Azure Batch will automatically timeout the job upon reaching such duration and the task will be failed."
    )
    @Builder.Default
    private final Duration waitUntilCompletion = Duration.ofHours(1);

    @Schema(
        title = "The private registry which contains the container image."
    )
    private ContainerRegistry registry;

    @Override
    public RunnerResult run(RunContext runContext, ScriptCommands commandsWrapper, List<String> filesToUploadWithoutInternalStorage, List<String> filesToDownload) throws Exception {
        boolean hasBlobStorage = blobStorage != null && blobStorage.valid();

        String blobStorageWorkingDirName = IdUtils.create();
        Map<String, Object> additionalVars = commandsWrapper.getAdditionalVars();
        if (hasBlobStorage) {
            additionalVars.putAll(Map.<String, Object>of(
                "workingDir", blobStorageWorkingDirName,
                "outputDir", blobStorageWorkingDirName
            ));
        }

        List<String> filesToUpload = new ArrayList<>(ListUtils.emptyOnNull(filesToUploadWithoutInternalStorage));
        List<String> command = ScriptService.uploadInputFiles(
            runContext,
            runContext.render(commandsWrapper.getCommands(), additionalVars),
            (ignored, localFilePath) -> filesToUpload.add(localFilePath),
            true
        );

        boolean hasFilesToUpload = !ListUtils.isEmpty(filesToUpload);
        if (hasFilesToUpload && !hasBlobStorage) {
            throw new IllegalArgumentException("You must provide a way to connect to a Blob Storage container to use `inputFiles` or `namespaceFiles`");
        }
        boolean hasFilesToDownload = !ListUtils.isEmpty(filesToDownload);
        if (hasFilesToDownload && !hasBlobStorage) {
            throw new IllegalArgumentException("You must provide a way to connect to a Blob Storage container to use `outputFiles`");
        }

        BlobContainerClient blobContainerClient = blobStorage.blobContainerClient(runContext);

        String jobId = IdUtils.create();

        List<ResourceFile> resourceFiles = filesToUpload.stream().map(throwFunction(file -> {
            // Use path to eventually deduplicate leading '/'
            String blobName = blobStorageWorkingDirName + Path.of("/" + file);
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
        })).toList();

        AbstractLogConsumer logConsumer = commandsWrapper.getLogConsumer();
        Create createJob = Create.builder()
            .id("create")
            .type(Create.class.getName())
            .account(this.account)
            .accessKey(this.accessKey)
            .endpoint(this.endpoint)
            .poolId(this.poolId)
            .maxDuration(this.waitUntilCompletion)
            .job(
                Job.builder()
                    .id(jobId)
                    .build()
            )
            .tasks(List.of(
                Task.builder()
                    .id("task-" + jobId)
                    .constraints(
                        TaskConstraints.builder()
                            .maxWallClockTime(this.waitUntilCompletion)
                            .maxTaskRetryCount(0)
                            .build()
                    )
                    .interpreter(command.get(0))
                    .interpreterArgs(command.size() > 1 ? new String[]{command.get(1)} : new String[0])
                    .commands(command.size() > 2 ? command.subList(2, command.size()) : Collections.emptyList())
                    .resourceFiles(resourceFiles)
                    .outputFiles(filesToDownload)
                    .containerSettings(
                        TaskContainerSettings.builder()
                            .workingDirectory(ContainerWorkingDirectory.TASK_WORKING_DIRECTORY)
                            .registry(registry)
                            .imageName(commandsWrapper.getContainerImage())
                            .build()
                    )
                    .build()
            ))
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
            throw new ScriptException(1, logConsumer.getStdOutCount(), logConsumer.getStdErrCount());
        }

        return new RunnerResult(0, logConsumer);
    }
}
