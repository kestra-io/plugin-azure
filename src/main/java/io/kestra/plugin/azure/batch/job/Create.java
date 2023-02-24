package io.kestra.plugin.azure.batch.job;

import com.microsoft.azure.PagedList;
import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.protocol.models.*;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.scripts.BashService;
import io.kestra.plugin.azure.batch.AbstractBatch;
import io.kestra.plugin.azure.batch.models.Job;
import io.kestra.plugin.azure.batch.models.Task;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

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
            title = "Use a container to start the task, the pool must used a `microsoft-azure-batch` publisher",
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
    @PluginProperty(dynamic = false)
    @NotNull
    private List<Task> tasks;

    @Schema(
        title = "The max total wait duration",
        description = "If null, there is no timeout and end is delegate to Azure Batch"
    )
    @PluginProperty(dynamic = false)
    private Duration maxDuration;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        BatchClient client = this.client(runContext);
        String jobId = null;

        try {
            // create job
            PoolInformation poolInfo = new PoolInformation()
                .withPoolId(runContext.render(poolId));

            JobAddParameter job = this.job.to(runContext, poolInfo);
            jobId = job.id();
            client.jobOperations().createJob(job);

            logger.info("Job '{}' created on pool '{}'", jobId, poolInfo.poolId());

            // create tasks
            List<TaskAddParameter> tasks = new ArrayList<>();
            Map<String, Integer> tasksMapping = new HashMap<>();
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

            TaskService.waitForTasksToComplete(runContext, client, jobId, maxDuration);

            // get tasks result
            List<CloudTask> results = client.taskOperations().listTasks(jobId);

            // failed ?
            List<CloudTask> failed = results.stream()
                .filter(cloudTask -> cloudTask.executionInfo().failureInfo() != null)
                .collect(Collectors.toList());

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
                    outputs.putAll(BashService.parseOut(msg, logger, runContext));
                    logger.info(msg);
                });

                TaskService.readRemoteLog(runContext, client, jobId, task, "stderr.txt", msg -> {
                    outputs.putAll(BashService.parseOut(msg, logger, runContext));
                    logger.warn(msg);
                });

                if (task.executionInfo().failureInfo() != null) {
                    continue;
                }

                // outputsFiles
                Task currentTask = this.tasks.get(tasksMapping.get(task.id()));
                if (currentTask.getOutputFiles() != null) {
                    for (String s : currentTask.getOutputFiles()) {
                        File file = TaskService.readRemoteFile(runContext, client, jobId, task, "wd/" + s, true);
                        outputFiles.put(s, runContext.putTempFile(file));
                    }
                }

                if (currentTask.getOutputDirs() != null) {
                    PagedList<NodeFile> nodeFiles = client.fileOperations()
                        .listFilesFromTask(jobId, task.id(), true, new DetailLevel.Builder().build());

                    for (String s : currentTask.getOutputDirs()) {
                        for (NodeFile nodeFile : nodeFiles) {
                            if (nodeFile.name().startsWith("wd/"+ s) && !nodeFile.isDirectory()) {
                                File file = TaskService.readRemoteFile(runContext, client, jobId, task, nodeFile.name(), true);
                                outputFiles.put(nodeFile.name().substring(3), runContext.putTempFile(file));
                            }
                        }
                    }
                }
            }

            if (failed.size() > 0) {
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
            if (jobId != null) {
                client.jobOperations().deleteJob(jobId);
            }
        }
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The value extract from output of the commands"
        )
        private Map<String, Object> vars;

        @Schema(
            title = "The output files uri in Kestra internal storage"
        )
        @PluginProperty(additionalProperties = URI.class)
        private Map<String, URI> outputFiles;
    }
}
