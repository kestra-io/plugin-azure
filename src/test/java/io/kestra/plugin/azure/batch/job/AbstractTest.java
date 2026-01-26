package io.kestra.plugin.azure.batch.job;

import io.kestra.plugin.azure.BaseTest;
import io.micronaut.context.annotation.Value;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = "AZURE_CONNECTION_STRING", matches = ".+")
abstract class AbstractTest extends BaseTest {
    @Value("${kestra.variables.globals.azure.batch.endpoint}")
    protected String endpoint;

    @Value("${kestra.variables.globals.azure.batch.account}")
    protected String account;

    @Value("${kestra.variables.globals.azure.batch.access-key}")
    protected String accessKey;

    @Value("${kestra.variables.globals.azure.batch.pool-id}")
    protected String poolId;

    @Value("${kestra.variables.globals.azure.batch.job-id}")
    protected String jobId;
}
