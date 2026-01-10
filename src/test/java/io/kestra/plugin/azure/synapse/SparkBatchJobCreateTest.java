package io.kestra.plugin.azure.synapse;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.azure.BaseTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Test for SparkBatchJobCreate task.
 * 
 * Note: This test is disabled by default as it requires:
 * - Azure Synapse workspace with Spark pool
 * - Valid Azure credentials (service principal)
 * - ADLS Gen2 storage with Spark application files
 * 
 * To run this test:
 * 1. Create Azure Synapse workspace and Spark pool
 * 2. Upload a test Spark application to ADLS Gen2
 * 3. Configure credentials in application.yml:
 *    - kestra.variables.globals.azure.synapse.endpoint
 *    - kestra.variables.globals.azure.synapse.sparkPoolName
 *    - kestra.variables.globals.azure.synapse.tenantId
 *    - kestra.variables.globals.azure.synapse.clientId
 *    - kestra.variables.globals.azure.synapse.clientSecret
 *    - kestra.variables.globals.azure.synapse.testFile
 * 4. Remove @Disabled annotation
 */
class SparkBatchJobCreateTest extends BaseTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @Disabled("Requires Azure Synapse workspace and Spark pool - enable for integration testing")
    void run() throws Exception {
        String jobName = "test-spark-job-" + IdUtils.create();
        
        SparkBatchJobCreate task = SparkBatchJobCreate.builder()
            .id(SparkBatchJobCreateTest.class.getSimpleName())
            .type(SparkBatchJobCreate.class.getName())
            .rEndpoint(Property.ofExpression("{{ globals.azure.synapse.endpoint }}"))
            .rSparkPoolName(Property.ofExpression("{{ globals.azure.synapse.sparkPoolName }}"))
            .tenantId(Property.ofExpression("{{ globals.azure.synapse.tenantId }}"))
            .clientId(Property.ofExpression("{{ globals.azure.synapse.clientId }}"))
            .clientSecret(Property.ofExpression("{{ globals.azure.synapse.clientSecret }}"))
            .rName(Property.ofValue(jobName))
            .rFile(Property.ofExpression("{{ globals.azure.synapse.testFile }}"))
            .rClassName(Property.ofValue("org.apache.spark.examples.SparkPi"))
            .rArguments(Property.ofValue(List.of("100")))
            .rDriverMemory(Property.ofValue("4g"))
            .rDriverCores(Property.ofValue(2))
            .rExecutorMemory(Property.ofValue("4g"))
            .rExecutorCores(Property.ofValue(2))
            .rExecutorCount(Property.ofValue(2))
            .build();

        RunContext runContext = runContextFactory.of();
        SparkBatchJobCreate.Output output = task.run(runContext);

        assertThat(output.getJobId(), is(notNullValue()));
        assertThat(output.getJobName(), is(jobName));
        assertThat(output.getState(), is(notNullValue()));
    }

    @Test
    @Disabled("Requires Azure Synapse workspace and Spark pool - enable for integration testing")
    void runWithMinimalConfig() throws Exception {
        String jobName = "test-minimal-job-" + IdUtils.create();
        
        SparkBatchJobCreate task = SparkBatchJobCreate.builder()
            .id(SparkBatchJobCreateTest.class.getSimpleName())
            .type(SparkBatchJobCreate.class.getName())
            .rEndpoint(Property.ofExpression("{{ globals.azure.synapse.endpoint }}"))
            .rSparkPoolName(Property.ofExpression("{{ globals.azure.synapse.sparkPoolName }}"))
            .tenantId(Property.ofExpression("{{ globals.azure.synapse.tenantId }}"))
            .clientId(Property.ofExpression("{{ globals.azure.synapse.clientId }}"))
            .clientSecret(Property.ofExpression("{{ globals.azure.synapse.clientSecret }}"))
            .rName(Property.ofValue(jobName))
            .rFile(Property.ofExpression("{{ globals.azure.synapse.testFile }}"))
            .build();

        RunContext runContext = runContextFactory.of();
        SparkBatchJobCreate.Output output = task.run(runContext);

        assertThat(output.getJobId(), is(notNullValue()));
        assertThat(output.getJobName(), is(jobName));
        assertThat(output.getState(), is(notNullValue()));
    }
}
