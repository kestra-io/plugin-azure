package io.kestra.plugin.azure.runner;

import io.kestra.core.models.tasks.runners.AbstractTaskRunnerTest;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.plugin.azure.storage.blob.models.BlobStorageForBatch;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import org.junit.jupiter.api.Disabled;

import java.time.Duration;

@KestraTest
@Disabled("Too costly to run on CI")
public class BatchTest extends AbstractTaskRunnerTest {
    @Value("${kestra.variables.globals.azure.batch.accessKey}")
    private String accessKey;

    @Value("${kestra.variables.globals.azure.batch.account}")
    private String account;

    @Value("${kestra.variables.globals.azure.batch.endpoint}")
    private String endpoint;

    @Value("${kestra.variables.globals.azure.batch.poolId}")
    private String poolId;

    @Value("${kestra.variables.globals.azure.blobs.connection-string}")
    protected String connectionString;

    @Value("${kestra.variables.globals.azure.batch.blobs.containerName}")
    private String blobContainerName;

    @Override
    protected TaskRunner taskRunner() {
        return Batch.builder()
            .accessKey(accessKey)
            .account(account)
            .endpoint(endpoint)
            .blobStorage(
                BlobStorageForBatch.builder()
                    .containerName(blobContainerName)
                    .connectionString(connectionString)
                    .build()
            )
            .poolId(poolId)
            .waitUntilCompletion(Duration.ofMinutes(30))
            .build();
    }
}
