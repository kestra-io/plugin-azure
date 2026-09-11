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
@Disabled("Requires an Azure Machine Learning workspace with a compute cluster")
class ScaleClusterTest extends AbstractMachineLearningTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        ScaleCluster task = ScaleCluster.builder()
            .id(ScaleClusterTest.class.getSimpleName())
            .type(ScaleCluster.class.getName())
            .tenantId(TENANT_ID)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .subscriptionId(SUBSCRIPTION_ID)
            .resourceGroupName(RESOURCE_GROUP_NAME)
            .workspaceName(WORKSPACE_NAME)
            .computeName(COMPUTE_NAME)
            .minNodeCount(Property.ofValue(0))
            .maxNodeCount(Property.ofValue(2))
            .build();

        RunContext runContext = runContextFactory.of();
        ScaleCluster.Output output = task.run(runContext);

        assertThat(output.getMinNodeCount(), is(0));
        assertThat(output.getMaxNodeCount(), is(2));
    }
}
