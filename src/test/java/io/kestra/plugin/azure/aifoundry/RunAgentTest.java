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
import static org.hamcrest.Matchers.containsString;

@KestraTest
class RunAgentTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @Disabled("Needs Azure AI Foundry credentials via Entra ID")
    void run() throws Exception {
        RunAgent task = RunAgent.builder()
            .id("run-agent")
            .type(RunAgent.class.getName())
            .endpoint(Property.ofValue("https://your-endpoint.openai.azure.com/"))
            .agentId(Property.ofValue("test-agent"))
            .prompt(Property.ofValue("Hello, agent!"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        RunAgent.Output runOutput = task.run(runContext);

        assertThat(runOutput.getResult(), containsString("Mocked execution for: Hello, agent!"));
    }
}
