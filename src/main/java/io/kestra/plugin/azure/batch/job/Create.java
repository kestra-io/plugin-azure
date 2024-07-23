package io.kestra.plugin.azure.batch.job;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.microsoft.azure.PagedList;
import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.protocol.models.*;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.azure.batch.AbstractBatch;
import io.kestra.plugin.azure.batch.BatchService;
import io.kestra.plugin.azure.batch.models.Job;
import io.kestra.plugin.azure.batch.models.Task;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = {
                "endpoint: https://***.francecentral.batch.azure.com",
                "account: <batch-account>",
                "accessKey: <access-key>",
                "poolId: <pool-id>",
                "job:",
                "  id: <job-name>",
                "tasks:",
                "- id: env",
                "  commands:",
                "  - 'echo t1=$ENV_STRING'",
                "  environments:",
                "    ENV_STRING: \"{{ inputs.first }}\"",
                "- id: echo",
                "  commands:",
                "  - 'echo t2={{ inputs.second }} 1>&2'",
                "- id: for",
                "  commands:",
                "  -  'for i in $(seq 10); do echo t3=$i; done'",
                "- id: vars",
                "  commands:",
                "  - echo '::{\"outputs\":{\"extract\":\"'$(cat files/in/in.txt)'\"}::'",
                "  resourceFiles:",
                "  - httpUrl: https://unittestkt.blob.core.windows.net/tasks/***?sv=***&se=***&sr=***&sp=***&sig=***",
                "    filePath: files/in/in.txt",
                "- id: output",
                "  commands:",
                "  - 'mkdir -p outs/child/sub'",
                "  - 'echo 1 > outs/1.txt'",
                "  - 'echo 2 > outs/child/2.txt'",
                "  - 'echo 3 > outs/child/sub/3.txt'",
                "  outputFiles:",
                "  - outs/1.txt",
                "  outputDirs:",
                "  - outs/child",
            }
        ),
        @Example(
            title = "Use a container to start the task, the pool must use a `microsoft-azure-batch` publisher.",
            code = {
                "endpoint: https://***.francecentral.batch.azure.com",
                "account: <batch-account>",
                "accessKey: <access-key>",
                "poolId: <pool-id>",
                "job:",
                "  id: <job-name>",
                "tasks:",
                "- id: echo",
                "  commands:",
                "  - 'python --version'",
                "  containerSettings:",
                "    imageName: python",
            }
        )
    }
)
@Schema(
    title = "Create a Azure Batch job with tasks."
)
public class Create extends AbstractBatch implements RunnableTask<Create.Output> {
    public static final String DIRECTORY_MARKER = ".kestradirectory";
    @Schema(
        title = "The ID of the pool."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String poolId;

    @Schema(
        title = "The job to create."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Job job;

    @Schema(
        title = "The list of tasks to be run."
    )
    @PluginProperty
    @NotNull
    private List<Task> tasks;

    @Schema(
        title = "The maximum total wait duration.",
        description = "If null, there is no timeout and the task is delegated to Azure Batch."
    )
    @PluginProperty
    private Duration maxDuration;

    @Schema(
        title = "The frequency with which the task checks whether the job is completed."
    )
    @Builder.Default
    @PluginProperty
    private final Duration completionCheckInterval = Duration.ofSeconds(1);

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

    @JsonIgnore
    private AbstractLogConsumer logConsumer;

    @JsonIgnore
    @Builder.Default
    private Boolean pushOutputFilesToInternalStorage = true;

    @JsonIgnore
    @Builder.Default
    @Getter(AccessLevel.NONE)
    private AtomicReference<Runnable> killable = new AtomicReference<>();

    @JsonIgnore
    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isKilled = new AtomicBoolean(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        if (logConsumer == null) {
            logConsumer = new DefaultLogConsumer(runContext);
        }

        Logger logger = runContext.logger();
        BatchClient client = BatchService.client(this.endpoint, this.account, this.accessKey, runContext);

        Map<String, Integer> tasksMapping = new HashMap<>();
        String jobId = null;
        if (Boolean.TRUE.equals(resume)) {
            String baseJobName = ScriptService.jobName(runContext);
            baseJobName = baseJobName.substring(0, baseJobName.lastIndexOf('-') + 1);
            var cloudJob = BatchService.getExistingJob(runContext, client, baseJobName);
            if (cloudJob.isPresent()) {
                jobId = cloudJob.get().id();
                logger.info("Job is resumed from an already running job {} ", jobId);

                // re-create the task mapping
                int index = 0;
                var existingTasks = client.taskOperations().listTasks(jobId).stream().toList();
                for (CloudTask current : existingTasks) {
                    tasksMapping.put(current.id(), index);
                    index++;
                }
            }
        }

        try {
            if (jobId == null) {
                // create job
                PoolInformation poolInfo = new PoolInformation()
                    .withPoolId(runContext.render(poolId));

                final JobAddParameter job = this.job.to(runContext, poolInfo);
                jobId = job.id();
                client.jobOperations().createJob(job);

                logger.info("Job '{}' created on pool '{}'", jobId, poolInfo.poolId());

                // create tasks
                List<TaskAddParameter> tasks = new ArrayList<>();
                int index = 0;
                for (Task current : this.tasks) {
                    TaskAddParameter taskAddParameter = current.to(runContext);

                    tasksMapping.put(taskAddParameter.id(), index);
                    tasks.add(taskAddParameter);

                    index++;
                }

                if (logger.isDebugEnabled()) {
                    tasks
                        .forEach(task -> logger.debug("Starting task '{}' with command: {}", task.id(), task.commandLine()));
                }

                client.taskOperations().createTasks(jobId, tasks);
            }

            String finalJobId = jobId;
            this.killable.set(() -> safelyKillJobTask(runContext, client, finalJobId));

            TaskService.waitForTasksToComplete(runContext, client, jobId, maxDuration, completionCheckInterval);

            // get tasks result
            List<CloudTask> results = client.taskOperations().listTasks(jobId);

            // failed ?
            List<CloudTask> failed = results.stream()
                .filter(cloudTask -> cloudTask.executionInfo().failureInfo() != null)
                .toList();

            // populate results
            Map<String, Object> outputs = new ConcurrentHashMap<>();
            Map<String, URI> outputFiles = new ConcurrentHashMap<>();

            for (CloudTask task : results) {
                if (task.executionInfo().failureInfo() != null) {
                    logger.warn("Task '{}' failed with exit code '{}': {}", task.id(), task.executionInfo().exitCode(), task.executionInfo().failureInfo().message());
                } else {
                    logger.info("Task ended '{}' with exit code '{}'", task.id(), task.executionInfo().exitCode());
                }

                // metrics
                if (task.stats() != null) {
                    runContext.metric(Counter.of("io.read.ops.count", task.stats().readIOps()));
                    runContext.metric(Counter.of("io.read.gib.count", task.stats().readIOGiB()));
                    runContext.metric(Counter.of("io.write.ops.count", task.stats().writeIOps()));
                    runContext.metric(Counter.of("io.write.gib.count", task.stats().writeIOGiB()));
                    runContext.metric(Timer.of("cpu.kernel.duration", Duration.ofMillis(task.stats().kernelCPUTime().getMillis())));
                    runContext.metric(Timer.of("cpu.user.duration", Duration.ofMillis(task.stats().userCPUTime().getMillis())));
                    runContext.metric(Timer.of("wall.clock.duration", Duration.ofMillis(task.stats().wallClockTime().getMillis())));
                }

                // log
                TaskService.readRemoteLog(runContext, client, jobId, task, "stdout.txt", msg -> {
                    logConsumer.accept(msg, false);
                    outputs.putAll(logConsumer.getOutputs());
                });

                TaskService.readRemoteLog(runContext, client, jobId, task, "stderr.txt", msg -> {
                    logConsumer.accept(msg, true);
                    outputs.putAll(logConsumer.getOutputs());
                });

                if (task.executionInfo().failureInfo() != null) {
                    continue;
                }

                // outputsFiles
                Task currentTask = this.tasks.get(tasksMapping.get(task.id()));
                String remoteWorkingDir = "wd/";
                if (currentTask.getOutputFiles() != null) {
                    for (String s : currentTask.getOutputFiles()) {
                        File file = TaskService.readRemoteFile(runContext, client, jobId, task, remoteWorkingDir + s, s, true);

                        // As this task is used in the task runner, this processing is already done by the CommandsWrapper so we need some guard
                        if (pushOutputFilesToInternalStorage) {
                            outputFiles.put(s, runContext.storage().putFile(file));
                        }
                    }
                }

                if (currentTask.getOutputDirs() != null) {
                    PagedList<NodeFile> nodeFiles = client.fileOperations()
                        .listFilesFromTask(jobId, task.id(), true, new DetailLevel.Builder().build());

                    for (String s : currentTask.getOutputDirs()) {
                        for (NodeFile nodeFile : nodeFiles) {
                            if (nodeFile.name().startsWith(remoteWorkingDir + s) && !nodeFile.isDirectory() && !nodeFile.name().endsWith("/" + DIRECTORY_MARKER)) {
                                String relativeFileName = nodeFile.name().substring(remoteWorkingDir.length());
                                File file = TaskService.readRemoteFile(runContext, client, jobId, task, nodeFile.name(), relativeFileName, true);
                                if (pushOutputFilesToInternalStorage) {
                                    outputFiles.put(relativeFileName, runContext.storage().putFile(file));
                                }
                            }
                        }
                    }
                }
            }

            if (!failed.isEmpty()) {
                throw new Exception(failed.size() + "/" + this.tasks.size() + " task(s) failed, terminating!");
            }

            return Output
                .builder()
                .vars(outputs)
                .outputFiles(outputFiles)
                .build();
        } catch (BatchErrorException e) {
            if (e.body() != null) {
                logger.error("Code '{}', Message '{}'", e.body().code(), e.body().message().value());
                if (e.body().values() != null) {
                    for (BatchErrorDetail detail : e.body().values()) {
                        logger.warn("Detail '{}'='{}'", detail.key(), detail.value());
                    }
                }
            }

            throw new Exception(e.toString(), e);
        } finally {
            kill();
        }
    }

    private void safelyKillJobTask(final RunContext runContext,
                                   final BatchClient client,
                                   final String jobId) {
        if (Boolean.TRUE.equals(delete)) {
            try {
                client.jobOperations().deleteJob(jobId);
                runContext.logger().debug("Job deleted: {}", jobId);
            } catch (IOException e) {
                runContext.logger().warn("Failed to delete job: {}.", jobId, e);
            }
        }
    }

    /** {@inheritDoc} **/
    @Override
    public void kill() {
        if (isKilled.compareAndSet(false, true)) {
            Runnable runnable = killable.get();
            if (runnable != null) {
                runnable.run();
            }
        }
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The values from the output of the commands."
        )
        private Map<String, Object> vars;

        @Schema(
            title = "The output files' URIs in Kestra's internal storage."
        )
        @PluginProperty(additionalProperties = URI.class)
        private Map<String, URI> outputFiles;
    }
}
