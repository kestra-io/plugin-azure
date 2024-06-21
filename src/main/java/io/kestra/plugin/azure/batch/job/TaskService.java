package io.kestra.plugin.azure.batch.job;

import com.microsoft.azure.batch.BatchClient;
import com.microsoft.azure.batch.DetailLevel;
import com.microsoft.azure.batch.protocol.models.BatchErrorException;
import com.microsoft.azure.batch.protocol.models.CloudTask;
import com.microsoft.azure.batch.protocol.models.TaskState;
import io.kestra.core.runners.RunContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.annotation.Nullable;

public class TaskService {
    public static List<CloudTask> waitForTasksToComplete(
        RunContext runContext,
        BatchClient client,
        String jobId,
        @Nullable Duration timeout,
        Duration completionCheckInterval
    ) throws BatchErrorException, IOException, InterruptedException, TimeoutException {
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0L;
        ArrayList<String> ended = new ArrayList<>();

        while (timeout == null || elapsedTime < timeout.toMillis()) {
            List<CloudTask> taskCollection = client
                .taskOperations()
                .listTasks(
                    jobId,
                    new DetailLevel.Builder().withSelectClause("id, state").build()
                );

            boolean allComplete = true;
            for (CloudTask task : taskCollection) {
                if (task.executionInfo() != null && task.executionInfo().endTime() != null && !ended.contains(task.id())) {
                    ended.add(task.id());

                    if (task.executionInfo().failureInfo() != null) {
                        runContext.logger().warn("Task '{}' failed with exit code '{}': {}", task.id(), task.executionInfo().exitCode(), task.executionInfo().failureInfo().message());
                    } else {
                        runContext.logger().info("Task ended '{}' with exit code '{}'", task.id(), task.executionInfo().exitCode());
                    }
                }

                if (task.state() != TaskState.COMPLETED) {
                    allComplete = false;
                    break;
                }
            }

            if (allComplete) {
                runContext.logger().info("{} tasks completed", taskCollection.size());
                return taskCollection;
            }

            TimeUnit.SECONDS.sleep(completionCheckInterval.toSeconds());
            elapsedTime = (new Date()).getTime() - startTime;
        }

        throw new TimeoutException("Task did not complete within the specified timeout");
    }

    public static File readRemoteFile(
        RunContext runContext,
        BatchClient client,
        String jobId,
        CloudTask task,
        String remoteFileName,
        Boolean copy
    ) throws IOException {
        return readRemoteFile(runContext, client, jobId, task, remoteFileName, null, copy);
    }

    public static File readRemoteFile(
        RunContext runContext,
        BatchClient client,
        String jobId,
        CloudTask task,
        String remoteFileName,
        String localFileName,
        Boolean copy
    ) throws IOException {
        File file = localFileName == null
            ? runContext.workingDir().createTempFile().toFile()
            : runContext.workingDir().resolve(Path.of(localFileName)).toFile();

        FileUtils.createParentDirectories(file);

        FileOutputStream fileOutputStream = new FileOutputStream(file);
        client.fileOperations().getFileFromTask(jobId, task.id(), remoteFileName, fileOutputStream);

        if (copy) {
            fileOutputStream.flush();
        }

        return file;
    }

    public static void readRemoteLog(
        RunContext runContext,
        BatchClient client,
        String jobId,
        CloudTask task,
        String fileName,
        Consumer<String> consumer
    ) throws IOException {
        File file = TaskService.readRemoteFile(runContext, client, jobId, task, fileName, false);

        IOUtils.lineIterator(new FileInputStream(file), StandardCharsets.UTF_8)
            .forEachRemaining(consumer);
    }
}
