package io.kestra.plugin.azure.aifoundry;

import java.time.Duration;
import java.util.Optional;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Trigger on completed agent run",
            code = {
                "id: azure_ai_on_agent_complete",
                "namespace: company.team",
                "triggers:",
                "  - id: on_agent_run",
                "    type: io.kestra.plugin.azure.aifoundry.Trigger",
                "    endpoint: \"{{ secret('AZURE_AI_FOUNDRY_ENDPOINT') }}\"",
                "    apiKey: \"{{ secret('AZURE_AI_FOUNDRY_API_KEY') }}\"",
                "    threadId: \"thread_123\"",
                "    runId: \"run_456\"",
                "    interval: PT5M",
                "tasks:",
                "  - id: notify",
                "    type: io.kestra.plugin.core.log.Log",
                "    message: \"Agent run completed: {{ trigger.runId }}\""
            }
        )
    }
)
@Schema(
    title = "Trigger based on a completed Azure AI Foundry agent run."
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output> {

    @Schema(title = "Azure AI Foundry endpoint.")
    @NotNull
    private Property<String> endpoint;

    @Schema(title = "Azure AI Foundry API key.")
    private Property<String> apiKey;

    @Schema(title = "The thread ID.")
    @NotNull
    private Property<String> threadId;

    @Schema(title = "The run ID.")
    @NotNull
    private Property<String> runId;

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        runContext.logger().warn("Agent triggers are not fully supported in the current beta of Azure AI SDK for Java.");
        String thread = runContext.render(this.threadId).as(String.class).orElseThrow();
        String run = runContext.render(this.runId).as(String.class).orElseThrow();

        // Mock evaluation logic
        if (false) {
            Execution execution = TriggerService.generateExecution(this, conditionContext, context, Output.builder().runId(run).build());
            return Optional.of(execution);
        }

        return Optional.empty();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The completed run ID.")
        private String runId;
    }
}
