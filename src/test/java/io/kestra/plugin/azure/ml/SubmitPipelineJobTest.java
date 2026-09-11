package io.kestra.plugin.azure.ml;

import java.util.Map;

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
class SubmitPipelineJobTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        SubmitPipelineJob task = SubmitPipelineJob.builder()
            .id(SubmitPipelineJobTest.class.getSimpleName())
            .type(SubmitPipelineJob.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .name(Property.ofValue("kestra-test-pipeline-" + IdUtils.create()))
            .jobs(
                Property.ofValue(
                    Map.of(
                        "step_one", Map.of(
                            "type", "command",
                            "computeId", "cpu-cluster",
                            "command", "echo hello from Kestra",
                            "environmentId", "azureml:AzureML-sklearn-1.5:1"
                        )
                    )
                )
            )
            .build();

        RunContext runContext = runContextFactory.of();
        SubmitPipelineJob.Output output = task.run(runContext);

        assertThat(output.getJobName(), is(notNullValue()));
        assertThat(output.getStatus(), is(JobState.COMPLETED));
    }
}
