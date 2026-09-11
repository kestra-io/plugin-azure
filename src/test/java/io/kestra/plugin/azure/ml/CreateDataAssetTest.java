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
@Disabled("Requires an Azure Machine Learning workspace")
class CreateDataAssetTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void runRegistersUriFolderDataAsset() throws Exception {
        CreateDataAsset task = CreateDataAsset.builder()
            .id(CreateDataAssetTest.class.getSimpleName())
            .type(CreateDataAsset.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .dataName(Property.ofValue("kestra-test-dataset"))
            .dataAssetType(Property.ofValue(DataAssetType.URI_FOLDER))
            .uri(Property.ofExpression("{{ globals.azure.ml.dataDatastoreUri }}"))
            .build();

        RunContext runContext = runContextFactory.of();
        CreateDataAsset.Output output = task.run(runContext);

        assertThat(output.getDataName(), is("kestra-test-dataset"));
        assertThat(output.getVersion(), is(notNullValue()));
    }
}
