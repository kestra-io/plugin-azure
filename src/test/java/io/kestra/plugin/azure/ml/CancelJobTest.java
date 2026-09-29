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
@Disabled("Requires an Azure Machine Learning workspace with a running job")
class CancelJobTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        CancelJob task = CancelJob.builder()
            .id(CancelJobTest.class.getSimpleName())
            .type(CancelJob.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .jobName(Property.ofExpression("{{ globals.azure.ml.runningJobName }}"))
            .build();

        RunContext runContext = runContextFactory.of();
        CancelJob.Output output = task.run(runContext);

        assertThat(output.getJobName(), is(notNullValue()));
        assertThat(output.getStatus().isTerminal(), is(true));
    }
}
