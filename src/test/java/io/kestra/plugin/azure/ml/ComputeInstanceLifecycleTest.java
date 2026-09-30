package io.kestra.plugin.azure.ml;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("Requires an Azure Machine Learning workspace with a compute instance")
class ComputeInstanceLifecycleTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void startThenStop() throws Exception {
        StartComputeInstance start = StartComputeInstance.builder()
            .id("start")
            .type(StartComputeInstance.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .computeName(COMPUTE_NAME)
            .build();

        RunContext runContext = runContextFactory.of();
        AbstractComputeInstanceLifecycle.Output startOutput = start.run(runContext);
        assertThat(startOutput.getState(), is("RUNNING"));

        StopComputeInstance stop = StopComputeInstance.builder()
            .id("stop")
            .type(StopComputeInstance.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .computeName(COMPUTE_NAME)
            .build();

        AbstractComputeInstanceLifecycle.Output stopOutput = stop.run(runContext);
        assertThat(stopOutput.getState(), is("STOPPED"));
    }
}
