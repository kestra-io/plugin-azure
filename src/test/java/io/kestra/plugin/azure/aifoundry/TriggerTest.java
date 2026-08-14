package io.kestra.plugin.azure.aifoundry;

import java.util.Optional;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @Disabled("Needs Azure AI Foundry credentials via Entra ID")
    void evaluate() throws Exception {
        Trigger trigger = Trigger.builder()
            .id("trigger")
            .type(Trigger.class.getName())
            .endpoint(Property.ofValue("https://your-endpoint.openai.azure.com/"))
            .threadId(Property.ofValue("test-thread"))
            .runId(Property.ofValue("test-run"))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());

        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("company.team")
            .flowId("azure-ai-trigger-test")
            .triggerId(trigger.getId())
            .date(java.time.ZonedDateTime.now())
            .build();

        Optional<io.kestra.core.models.executions.Execution> evaluate = trigger.evaluate(
            io.kestra.core.models.conditions.ConditionContext.builder().runContext(runContext).build(),
            triggerContext
        );

        // Given our mocked logic currently returns empty
        assertThat(evaluate.isEmpty(), is(true));
    }
}
