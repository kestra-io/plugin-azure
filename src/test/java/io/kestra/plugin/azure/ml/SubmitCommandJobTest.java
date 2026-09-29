package io.kestra.plugin.azure.ml;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("Requires an Azure Machine Learning workspace with a running compute target")
class SubmitCommandJobTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        SubmitCommandJob task = SubmitCommandJob.builder()
            .id(SubmitCommandJobTest.class.getSimpleName())
            .type(SubmitCommandJob.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .computeName(COMPUTE_NAME)
            .environmentId(ENVIRONMENT_ID)
            .name(Property.ofValue("kestra-test-" + IdUtils.create()))
            .command(Property.ofValue("echo hello from Kestra"))
            .build();

        RunContext runContext = runContextFactory.of();
        SubmitCommandJob.Output output = task.run(runContext);

        assertThat(output.getJobName(), is(notNullValue()));
        assertThat(output.getStatus(), is(JobState.COMPLETED));
        assertThat(output.getStudioUrl(), containsString(output.getJobName()));
    }

    @Test
    void runWithoutWaitReturnsImmediately() throws Exception {
        SubmitCommandJob task = SubmitCommandJob.builder()
            .id(SubmitCommandJobTest.class.getSimpleName())
            .type(SubmitCommandJob.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .computeName(COMPUTE_NAME)
            .environmentId(ENVIRONMENT_ID)
            .name(Property.ofValue("kestra-test-" + IdUtils.create()))
            .command(Property.ofValue("echo hello from Kestra"))
            .wait(Property.ofValue(false))
            .build();

        RunContext runContext = runContextFactory.of();
        SubmitCommandJob.Output output = task.run(runContext);

        assertThat(output.getJobName(), is(notNullValue()));
        assertThat(output.getMetrics(), is(nullValue()));
    }
}
