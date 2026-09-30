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
@Disabled("Requires an Azure Machine Learning workspace with a registered model")
class DownloadModelTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        DownloadModel task = DownloadModel.builder()
            .id(DownloadModelTest.class.getSimpleName())
            .type(DownloadModel.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .modelName(io.kestra.core.models.property.Property.ofExpression("{{ globals.azure.ml.modelName }}"))
            .build();

        RunContext runContext = runContextFactory.of();
        DownloadModel.Output output = task.run(runContext);

        assertThat(output.getUri(), is(notNullValue()));
        assertThat(output.getArchive(), is(notNullValue()));
    }
}
