package io.kestra.plugin.azure.ml;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("Requires an Azure Machine Learning workspace with a completed job")
class GetJobTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        GetJob task = GetJob.builder()
            .id(GetJobTest.class.getSimpleName())
            .type(GetJob.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .jobName(Property.ofExpression("{{ globals.azure.ml.jobName }}"))
            .build();

        RunContext runContext = runContextFactory.of();
        GetJob.Output output = task.run(runContext);

        assertThat(output.getJobName(), is(notNullValue()));
        assertThat(output.getStatus(), is(notNullValue()));
        assertThat(output.getMetrics(), is(notNullValue()));
    }

    @Test
    void runWithUnknownJobFailsWithActionableMessage() {
        GetJob task = GetJob.builder()
            .id(GetJobTest.class.getSimpleName())
            .type(GetJob.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .jobName(Property.ofValue("does-not-exist"))
            .build();

        RunContext runContext = runContextFactory.of();

        IllegalArgumentException exception = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("does-not-exist"));
    }
}
