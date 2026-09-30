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
@Disabled("Requires an Azure Machine Learning workspace with a registered model")
class GetModelTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runResolvesLatestVersion() throws Exception {
        GetModel task = GetModel.builder()
            .id(GetModelTest.class.getSimpleName())
            .type(GetModel.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .modelName(Property.ofExpression("{{ globals.azure.ml.modelName }}"))
            .build();

        RunContext runContext = runContextFactory.of();
        GetModel.Output output = task.run(runContext);

        assertThat(output.getVersion(), is(notNullValue()));
        assertThat(output.getModelUri(), is(notNullValue()));
    }
}
