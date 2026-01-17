package io.kestra.plugin.azure.synapse;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("Requires Azure Synapse workspace")
class SparkBatchJobCreateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
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
