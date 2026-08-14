package io.kestra.plugin.azure.aifoundry;

import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class CreateDeploymentTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @Disabled("Needs Azure AI Foundry credentials via Entra ID")
    void run() throws Exception {
        CreateDeployment task = CreateDeployment.builder()
            .id("create-deployment")
            .type(CreateDeployment.class.getName())
            .endpoint(Property.ofValue("https://your-endpoint.openai.azure.com/"))
            .deploymentName(Property.ofValue("test-deployment"))
            .modelId(Property.ofValue("gpt-4o"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        CreateDeployment.Output runOutput = task.run(runContext);

        assertThat(runOutput.getDeploymentName(), is("test-deployment"));
    }
}
